import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = {
    "crash-*", "leak-*", "oom-*", "timeout-*",
    ".zig-cache/f/**/*", ".zig-cache/tmp/libfuzzer.log",
}


def load_pipeline(name, environment=None):
    command = ["buildkite-agent", "pipeline", "upload", "--dry-run",
               "--agent-access-token", "dry-run-only"]
    if environment is None:
        command.append("--no-interpolation")
    command.append(str(ROOT / ".buildkite" / name))
    result = subprocess.run(
        command, cwd=ROOT, env={**os.environ, **(environment or {})},
        text=True, capture_output=True, check=True,
    )
    return json.loads(result.stdout)


def jobs(pipeline):
    for step in pipeline["steps"]:
        for value in step.get("matrix", [""]):
            yield {
                **step,
                "command": step["command"].replace("{{matrix}}", value),
                "agents": step.get("agents", pipeline["agents"]),
            }


class PipelineTests(unittest.TestCase):
    def test_regular_workloads(self):
        pipeline = load_pipeline("pipeline.yml")
        expanded = list(jobs(pipeline))
        self.assertEqual(len(expanded), 16)
        core = [job for job in expanded if job["key"].startswith("core-")]
        self.assertEqual(len(core), 4)
        for arch, queue in [("x86_64", "amd64"), ("aarch64", "arm64")]:
            for mode in ["Debug", "ReleaseSafe"]:
                command = (
                    f"bash .buildkite/scripts/run.sh {arch} zig build test "
                    f"-Doptimize={mode} --summary all"
                )
                match = [job for job in core if job["command"] == command]
                self.assertEqual(len(match), 1)
                self.assertEqual(match[0]["agents"]["queue"], f"raftz-linux-{queue}")
        commands = "\n".join(job["command"] for job in expanded)
        self.assertIn("mise run ci-lint-all", commands)
        self.assertIn("mise run test-wal-crash", commands)
        for task in ["test-tsan", "test-ubsan", "test-gperftools",
                     "test-raft-sqlite", "test-libelection"]:
            self.assertIn(f"mise run {task}", commands)
        self.assertIn("bash .buildkite/scripts/coverage.sh", commands)
        for target in ["codec", "wal", "confchange"]:
            self.assertIn(f"fuzz-{target} 100K", commands)
        self.assertIn("fuzz-sim 10K", commands)

    def test_composite_tasks(self):
        lint = (ROOT / ".mise/tasks/ci-lint-all").read_text()
        for task in ["fmt-check", "ci-lint", "ci-lint-buildkite"]:
            self.assertIn(f"mise run {task}\n", lint)
        wal = (ROOT / ".mise/tasks/test-wal-crash").read_text()
        self.assertIn("mise run wal-durability\n", wal)
        self.assertIn("mise run fuzz-wal-crash\n", wal)

    def test_nightly_workloads(self):
        expanded = list(jobs(load_pipeline("nightly.yml")))
        self.assertEqual(len(expanded), 5)
        commands = {job["command"] for job in expanded}
        for target, runs in [("codec", "1M"), ("wal", "1M"),
                             ("confchange", "1M"), ("sim", "100K"),
                             ("wal-crash", "10K")]:
            self.assertIn(
                "bash .buildkite/scripts/run.sh x86_64 "
                "bash .buildkite/scripts/with-fuzz-artifacts.sh "
                f"scripts/run-fuzz.sh fuzz-{target} {runs}",
                commands,
            )

    def test_fuzz_artifact_wrapper(self):
        script_path = ROOT / ".buildkite/scripts/with-fuzz-artifacts.sh"
        self.assertTrue(os.access(script_path, os.X_OK))
        script = script_path.read_text()
        self.assertIn("buildkite-agent artifact upload", script)
        for pattern in ARTIFACTS:
            self.assertIn(pattern, script)

    def test_cache_volumes(self):
        for name in ["pipeline.yml", "nightly.yml"]:
            cache = load_pipeline(name)["cache"]
            self.assertIn(".zig-cache", cache["paths"])
            self.assertIn("/tmp/raftz-ci-cache", cache["paths"])
            self.assertIn("${BUILDKITE_COMMIT}", cache["name"])
            self.assertNotIn("${BUILDKITE_BRANCH}", cache["name"])
        pipeline = load_pipeline("pipeline.yml")
        arm = [s for s in pipeline["steps"] if s["key"] == "core-arm64"]
        self.assertEqual(len(arm), 1)
        self.assertEqual(arm[0]["cache"]["paths"], pipeline["cache"]["paths"])
        self.assertIn("arm64", arm[0]["cache"]["name"])

    def test_cache_names_are_valid_after_interpolation(self):
        commit = "a" * 40
        for branch in ["formal/etcd-tla-baseline", "ci/cache_zig", "feature.v2"]:
            for name in ["pipeline.yml", "nightly.yml"]:
                with self.subTest(branch=branch, pipeline=name):
                    pipeline = load_pipeline(name, {
                        "BUILDKITE_BRANCH": branch, "BUILDKITE_COMMIT": commit,
                    })
                    caches = [pipeline["cache"], *[
                        step["cache"] for step in pipeline["steps"] if "cache" in step
                    ]]
                    for cache in caches:
                        self.assertRegex(cache["name"], r"^[A-Za-z0-9-]+$")
                        self.assertIn(commit, cache["name"])

    def test_cache_names_separate_commits_architectures_and_nightly(self):
        names_by_commit = []
        for commit in ["a" * 40, "b" * 40]:
            environment = {"BUILDKITE_BRANCH": "formal/etcd-tla-baseline",
                           "BUILDKITE_COMMIT": commit}
            regular = load_pipeline("pipeline.yml", environment)
            nightly = load_pipeline("nightly.yml", environment)
            names = [regular["cache"]["name"], nightly["cache"]["name"]]
            names.extend(step["cache"]["name"] for step in regular["steps"] if "cache" in step)
            self.assertEqual(len(names), 3)
            self.assertEqual(len(set(names)), 3)
            names_by_commit.append(set(names))
        self.assertTrue(names_by_commit[0].isdisjoint(names_by_commit[1]))

    def test_failure_and_artifact_contract(self):
        for name in ["pipeline.yml", "nightly.yml"]:
            pipeline = load_pipeline(name)
            keys = [step["key"] for step in pipeline["steps"]]
            self.assertEqual(len(keys), len(set(keys)))
            for job in jobs(pipeline):
                self.assertNotIn("soft_fail", job)
                self.assertNotIn("skip", job)
                self.assertNotIn("if", job)
                self.assertNotIn("cancel_on_build_failing", job)
                self.assertNotIn("bash -c", job["command"])
                self.assertGreater(job["timeout_in_minutes"], 0)
                if job["key"].startswith("fuzz") or job["key"] == "wal-durability":
                    self.assertNotIn("artifact_paths", job)
                    self.assertIn("with-fuzz-artifacts.sh", job["command"])
                if job["key"] == "coverage":
                    self.assertIn("zig-out/coverage/**/*", job["artifact_paths"])
                if job["key"] != "core-arm64":
                    self.assertEqual(job["agents"]["queue"], "raftz-linux-amd64")
                    self.assertIn("run.sh x86_64 ", job["command"])


class BootstrapTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        self.bin = self.base / "bin"
        self.bin.mkdir()
        self.log = self.base / "calls"
        self.env = {
            **os.environ,
            "PATH": f"{self.bin}:{os.environ['PATH']}",
            "TMPDIR": str(self.base / "tmp"),
            "CALL_LOG": str(self.log),
            "FAKE_BIN": str(self.bin),
            "FAKE_ARCH": "x86_64",
            "FAKE_OS": "Linux",
            "SHA_STATUS": "0",
            "INSTALL_STATUS": "0",
        }
        self.stub("uname", 'if [[ $1 == -s ]]; then echo "$FAKE_OS"; else echo "$FAKE_ARCH"; fi')
        self.stub("curl", 'printf "curl %s\\n" "$*" >> "$CALL_LOG"')
        self.stub("sha256sum", 'cat >/dev/null; exit "$SHA_STATUS"')
        self.stub("tar", '''
while [[ $# -gt 0 ]]; do
    if [[ $1 == --directory ]]; then dest=$2; break; fi
    shift
done
printf 'tar\\n' >> "$CALL_LOG"
mkdir -p "$dest/mise/bin"
cp "$FAKE_BIN/fake-mise" "$dest/mise/bin/mise"
''')
        self.stub("fake-mise", '''
printf 'mise %s\\n' "$*" >> "$CALL_LOG"
if [[ $1 == install ]]; then exit "$INSTALL_STATUS"; fi
[[ $1 == exec && $2 == -- ]]
shift 2
"$@"
''')

    def stub(self, name, body):
        path = self.bin / name
        path.write_text("#!/usr/bin/env bash\nset -euo pipefail\n" + body + "\n")
        path.chmod(0o755)

    def run_command(self, arch="x86_64", command=None):
        return subprocess.run(
            ["bash", str(ROOT / ".buildkite/scripts/run.sh"), arch,
             *(command or ["bash", "-c", "exit 0"])],
            env=self.env, text=True, capture_output=True,
        )

    def test_rejects_wrong_architecture_before_download(self):
        result = self.run_command("aarch64")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Expected Linux aarch64", result.stderr)
        self.assertFalse(self.log.exists())

    def test_rejects_non_linux(self):
        self.env["FAKE_OS"] = "Darwin"
        self.assertNotEqual(self.run_command().returncode, 0)
        self.assertFalse(self.log.exists())

    def test_checksum_failure_prevents_execution(self):
        self.env["SHA_STATUS"] = "1"
        self.assertNotEqual(self.run_command().returncode, 0)
        self.assertNotIn("tar\n", self.log.read_text())
        self.assertNotIn("mise install", self.log.read_text())

    def test_install_failure_stops_command(self):
        self.env["INSTALL_STATUS"] = "19"
        self.assertEqual(self.run_command().returncode, 19)
        self.assertNotIn("mise exec", self.log.read_text())

    def test_preserves_exit_status_and_cleans_temporary_files(self):
        result = self.run_command(command=["bash", "-c", "exit 23"])
        self.assertEqual(result.returncode, 23, result.stderr)
        self.assertEqual(list((self.base / "tmp/pi").iterdir()), [])

    def test_downloads_matching_architecture_and_preserves_arguments(self):
        for arch, release_arch in [("x86_64", "x64"), ("aarch64", "arm64")]:
            with self.subTest(arch=arch):
                self.env["FAKE_ARCH"] = arch
                result = self.run_command(
                    arch, ["bash", "-c", 'printf "%s" "$1"', "--", "two words"],
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout, "two words")
                self.assertIn(f"linux-{release_arch}.tar.gz", self.log.read_text())


if __name__ == "__main__":
    unittest.main()
