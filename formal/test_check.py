import contextlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock


spec = importlib.util.spec_from_file_location("tla_check", Path(__file__).with_name("check.py"))
check = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check)


class CheckTests(unittest.TestCase):
    def test_upstream_is_unmodified(self):
        with contextlib.redirect_stdout(io.StringIO()):
            check.verify_upstream()

    def test_upstream_corruption_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "file.tla").write_text("modified")
            (root / "manifest.json").write_text(json.dumps({"files": {"file.tla": "0" * 64}}))
            with mock.patch.object(check, "UPSTREAM", root):
                with self.assertRaisesRegex(RuntimeError, "missing or changed"):
                    check.verify_upstream()

    def test_missing_tool_is_rejected_without_download(self):
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(check, "CACHE", Path(directory)):
                with self.assertRaisesRegex(RuntimeError, "prepare-tla"):
                    check.classpath()

    def test_corrupt_download_is_not_installed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = {"tool.jar": {"url": "https://example.invalid/tool.jar", "sha256": "0" * 64}}
            with mock.patch.object(check, "CACHE", root), mock.patch.object(check, "tools", return_value=artifact):
                with mock.patch.object(check.urllib.request, "urlopen", return_value=io.BytesIO(b"corrupt")):
                    with contextlib.redirect_stdout(io.StringIO()):
                        with self.assertRaisesRegex(RuntimeError, "Checksum mismatch"):
                            check.prepare()
            self.assertFalse((root / "tool.jar").exists())

    def run_mock_tlc(self, status=0, output="", timeout=False, profile="single"):
        def run(command, **kwargs):
            kwargs["stdout"].write(output)
            if timeout:
                raise subprocess.TimeoutExpired(command, 1)
            return subprocess.CompletedProcess(command, status)

        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(check, "CACHE", Path(directory)), mock.patch.object(check, "classpath", return_value="tools.jar"):
                with mock.patch.object(check.subprocess, "run", side_effect=run):
                    with contextlib.redirect_stdout(io.StringIO()):
                        check.check(profile, 1)

    def test_completed_model_passes(self):
        self.run_mock_tlc(output="Model checking completed. No error has been found.")

    def test_nonzero_exit_fails(self):
        with self.assertRaisesRegex(RuntimeError, "TLC failed"):
            self.run_mock_tlc(status=12)

    def test_incomplete_search_is_not_a_pass(self):
        with self.assertRaisesRegex(RuntimeError, "did not report completed"):
            self.run_mock_tlc(output="Progress: 100 states")

    def test_completed_simulation_passes(self):
        self.run_mock_tlc(profile="smoke", output="100 traces generated\nSimulation using seed 20260912")

    def test_incomplete_simulation_is_not_a_pass(self):
        with self.assertRaisesRegex(RuntimeError, "did not report completed simulation"):
            self.run_mock_tlc(profile="smoke", output="10 traces generated")

    def test_timeout_is_not_a_pass(self):
        with self.assertRaisesRegex(RuntimeError, "NOT a pass"):
            self.run_mock_tlc(timeout=True)


if __name__ == "__main__":
    unittest.main()
