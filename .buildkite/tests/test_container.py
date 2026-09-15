import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("ci_seccomp", ROOT / ".buildkite/container/seccomp.py")
SECCOMP = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SECCOMP)


class SeccompTests(unittest.TestCase):
    def test_image_creates_tmpdir_before_package_configuration(self):
        dockerfile = (ROOT / ".buildkite/container/Dockerfile").read_text()
        self.assertIn("TMPDIR=/root/tmp", dockerfile)
        self.assertLess(dockerfile.index("mkdir -p /root/tmp"), dockerfile.index("apt-get update"))

    def test_profile_preserves_upstream_and_only_adds_required_calls(self):
        original = json.loads(SECCOMP.UPSTREAM.read_bytes())
        profile = SECCOMP.make_profile()
        self.assertEqual(profile["defaultAction"], "SCMP_ACT_ERRNO")
        for key, value in original.items():
            if key != "syscalls":
                self.assertEqual(profile[key], value)
        self.assertEqual(profile["syscalls"][:-2], original["syscalls"])
        self.assertEqual(profile["syscalls"][-2:], [
            {
                "names": ["personality"],
                "action": "SCMP_ACT_ALLOW",
                "args": [{"index": 0, "value": 0x40000, "op": "SCMP_CMP_EQ"}],
            },
            {
                "names": ["io_uring_setup", "io_uring_enter", "io_uring_register"],
                "action": "SCMP_ACT_ALLOW",
            },
        ])

    def test_changed_upstream_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profile.json"
            path.write_bytes(SECCOMP.UPSTREAM.read_bytes() + b"\n")
            with mock.patch.object(SECCOMP, "UPSTREAM", path):
                with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                    SECCOMP.make_profile()


class HostedContainerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        self.bin = self.base / "bin"
        self.bin.mkdir()
        self.log = self.base / "calls.jsonl"
        self.marker = self.base / "ran"
        self.env = {
            **os.environ,
            "PATH": f"{self.bin}:{os.environ['PATH']}",
            "TMPDIR": str(self.base / "tmp"),
            "CALL_LOG": str(self.log),
            "FAKE_DAEMON": "linux/x86_64",
            "BUILD_STATUS": "0",
            "RUN_STATUS": "0",
            "PREFLIGHT_STATUS": "0",
        }
        self.executable("raftz-ci-preflight", '''#!/usr/bin/env bash
exit "$PREFLIGHT_STATUS"
''')
        self.executable("docker", '''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import subprocess
import sys
args = sys.argv[1:]
with open(os.environ["CALL_LOG"], "a") as log:
    log.write(json.dumps(args) + "\\n")
if args[0] == "info":
    print(os.environ["FAKE_DAEMON"])
elif args[0] == "build":
    status = int(os.environ["BUILD_STATUS"])
    if status == 0:
        Path(args[args.index("--iidfile") + 1]).write_text("sha256:" + "0" * 64)
    sys.exit(status)
elif args[0] == "run":
    Path(args[args.index("--cidfile") + 1]).write_text("test-container")
    profile = args[args.index("--security-opt") + 1].removeprefix("seccomp=")
    assert json.loads(Path(profile).read_text())["defaultAction"] == "SCMP_ACT_ERRNO"
    status = int(os.environ["RUN_STATUS"])
    if status:
        sys.exit(status)
    image_index = next(i for i, arg in enumerate(args) if arg.startswith("sha256:"))
    sys.exit(subprocess.run(args[image_index + 1:]).returncode)
elif args[0] != "rm":
    raise SystemExit("Unexpected Docker command")
''')

    def executable(self, name, content):
        path = self.bin / name
        path.write_text(content)
        path.chmod(0o755)

    def run_container(self, command=None):
        return subprocess.run(
            ["bash", str(ROOT / ".buildkite/scripts/in-container.sh"),
             *(command if command is not None else ["bash", "-c", 'printf "%s" "$1" > "$2"',
                                                   "--", "two words", str(self.marker)])],
            env=self.env, text=True, capture_output=True,
        )

    def calls(self):
        return [json.loads(line) for line in self.log.read_text().splitlines()]

    def assert_clean(self):
        self.assertEqual(list((self.base / "tmp/pi").iterdir()), [])
        self.assertEqual(self.calls()[-1], ["rm", "--force", "test-container"])

    def test_runs_in_scoped_container_and_preserves_arguments(self):
        result = self.run_container()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.marker.read_text(), "two words")
        build = next(call for call in self.calls() if call[0] == "build")
        self.assertIn("--load", build)
        self.assertIn("plain", build)
        run = next(call for call in self.calls() if call[0] == "run")
        self.assertIn("--init", run)
        self.assertIn("--rm", run)
        self.assertIn("linux/amd64", run)
        self.assertIn(f"{ROOT}:{ROOT}", run)
        self.assertIn("/tmp/raftz-ci-cache:/tmp/raftz-ci-cache", run)
        self.assertIn("memlock=67108864:67108864", run)
        forwarded = [run[i + 1] for i, arg in enumerate(run) if arg == "--env"]
        self.assertEqual(forwarded, ["CI=true", "PYTHONDONTWRITEBYTECODE=1"])
        for forbidden in ["--privileged", "--cap-add", "seccomp=unconfined", "/var/run/docker.sock"]:
            self.assertFalse(any(forbidden in arg for arg in run))
        self.assert_clean()

    def test_preflight_failure_prevents_test_command(self):
        self.env["PREFLIGHT_STATUS"] = "19"
        result = self.run_container()
        self.assertEqual(result.returncode, 19, result.stderr)
        self.assertFalse(self.marker.exists())
        self.assert_clean()

    def test_test_failure_is_preserved(self):
        result = self.run_container(["bash", "-c", "exit 23"])
        self.assertEqual(result.returncode, 23, result.stderr)
        self.assert_clean()

    def test_build_failure_prevents_container(self):
        self.env["BUILD_STATUS"] = "17"
        self.assertEqual(self.run_container().returncode, 17)
        self.assertFalse(self.marker.exists())
        self.assertEqual([call[0] for call in self.calls()], ["info", "build"])
        self.assertEqual(list((self.base / "tmp/pi").iterdir()), [])

    def test_docker_failure_is_preserved(self):
        self.env["RUN_STATUS"] = "125"
        self.assertEqual(self.run_container().returncode, 125)
        self.assertFalse(self.marker.exists())
        self.assert_clean()

    def test_rejects_wrong_daemon_architecture(self):
        self.env["FAKE_DAEMON"] = "linux/aarch64"
        result = self.run_container()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Linux AMD64 Docker daemon is required", result.stderr)
        self.assertEqual(len(self.calls()), 1)

    def test_rejects_missing_command(self):
        self.assertNotEqual(self.run_container([]).returncode, 0)
        self.assertFalse(self.log.exists())


if __name__ == "__main__":
    unittest.main()
