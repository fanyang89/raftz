import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
TOOLS = ["cc", "c++", "make", "cmake", "ninja", "pkg-config"]


class NativeDependencyTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        self.bin = self.base / "bin"
        self.bin.mkdir()
        self.log = self.base / "calls"
        self.bash = shutil.which("bash")
        for tool in ["bash", "env", "dirname"]:
            (self.bin / tool).symlink_to(shutil.which(tool))
        self.env = {
            **os.environ,
            "PATH": str(self.bin),
            "CALL_LOG": str(self.log),
            "FAKE_BIN": str(self.bin),
            "FAKE_UID": "0",
            "REAL_CHMOD": shutil.which("chmod"),
            "UPDATE_STATUS": "0",
            "INSTALL_STATUS": "0",
            "INSTALL_INCOMPLETE": "0",
        }
        self.stub("id", 'printf "%s\\n" "$FAKE_UID"')
        self.stub("uname", 'if [[ $1 == -s ]]; then echo Linux; else echo x86_64; fi')
        self.stub("sudo", '''
printf 'sudo %s\\n' "$*" >> "$CALL_LOG"
[[ $1 == -n ]]
shift
"$@"
''')
        self.stub("apt-get", '''
printf 'apt-get %s [%s]\\n' "$*" "${DEBIAN_FRONTEND:-}" >> "$CALL_LOG"
if [[ $1 == update ]]; then exit "$UPDATE_STATUS"; fi
[[ $1 == install ]]
if [[ $INSTALL_STATUS != 0 ]]; then exit "$INSTALL_STATUS"; fi
if [[ $INSTALL_INCOMPLETE == 1 ]]; then exit 0; fi
for tool in cc c++ make cmake ninja pkg-config; do
    printf '#!/usr/bin/env bash\\nexit 0\\n' > "$FAKE_BIN/$tool"
    "$REAL_CHMOD" +x "$FAKE_BIN/$tool"
done
''')

    def stub(self, name, body):
        path = self.bin / name
        path.write_text("#!/usr/bin/env bash\nset -euo pipefail\n" + body + "\n")
        path.chmod(0o755)

    def run_installer(self):
        return subprocess.run(
            [self.bash, str(ROOT / ".buildkite/scripts/install-deps.sh")],
            env=self.env, text=True, capture_output=True,
        )

    def test_existing_tools_do_not_install(self):
        for tool in TOOLS:
            self.stub(tool, "exit 0")
        result = self.run_installer()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(self.log.exists())

    def test_missing_cmake_triggers_installation(self):
        for tool in TOOLS:
            self.stub(tool, "exit 0")
        (self.bin / "cmake").unlink()
        result = self.run_installer()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Missing native build tools: cmake\n", result.stderr)
        self.assertTrue(os.access(self.bin / "cmake", os.X_OK))
        self.assertIn("apt-get install", self.log.read_text())

    def test_bootstrap_dependency_failure_prevents_download(self):
        self.env["UPDATE_STATUS"] = "17"
        result = subprocess.run(
            [self.bash, str(ROOT / ".buildkite/scripts/run.sh"), "x86_64",
             self.bash, "-c", "exit 0"],
            env=self.env, text=True, capture_output=True,
        )
        self.assertEqual(result.returncode, 17, result.stderr)
        self.assertEqual(self.log.read_text(), "apt-get update []\n")
        self.assertNotIn("curl", result.stderr)

    def test_root_installs_and_verifies_all_tools(self):
        result = self.run_installer()
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.log.read_text().splitlines()
        self.assertEqual(calls, [
            "apt-get update []",
            "apt-get install --yes --no-install-recommends build-essential cmake ninja-build pkg-config [noninteractive]",
        ])
        for tool in TOOLS:
            self.assertTrue(os.access(self.bin / tool, os.X_OK))

    def test_nonroot_uses_noninteractive_sudo(self):
        self.env["FAKE_UID"] = "1000"
        result = self.run_installer()
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.log.read_text()
        self.assertIn("sudo -n apt-get update", calls)
        self.assertIn("sudo -n env DEBIAN_FRONTEND=noninteractive apt-get install", calls)

    def test_update_failure_stops_installation(self):
        self.env["UPDATE_STATUS"] = "17"
        result = self.run_installer()
        self.assertEqual(result.returncode, 17)
        self.assertNotIn("apt-get install", self.log.read_text())

    def test_install_failure_is_preserved(self):
        self.env["INSTALL_STATUS"] = "23"
        self.assertEqual(self.run_installer().returncode, 23)

    def test_incomplete_installation_fails(self):
        self.env["INSTALL_INCOMPLETE"] = "1"
        result = self.run_installer()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("still missing after installation", result.stderr)

    def test_missing_package_manager_fails(self):
        (self.bin / "apt-get").unlink()
        result = self.run_installer()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("automatic setup requires apt-get", result.stderr)
        self.assertFalse(self.log.exists())

    def test_nonroot_without_sudo_fails(self):
        self.env["FAKE_UID"] = "1000"
        (self.bin / "sudo").unlink()
        result = self.run_installer()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("root or passwordless sudo", result.stderr)
        self.assertFalse(self.log.exists())


if __name__ == "__main__":
    unittest.main()
