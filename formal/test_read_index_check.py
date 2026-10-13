import contextlib
import io
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

import read_index_check as runner


SUCCESS = """Model checking completed. No error has been found.
100 states generated, 50 distinct states found, 0 states left on queue.
"""
COUNTEREXAMPLE = """Error: Invariant ReadIndexSafety is violated.
Error: The behavior up to this point is:
State 1: <Initial predicate>
State 2: <Release>
"""


class ReadIndexCheckTests(unittest.TestCase):
    def test_complete_exploration_passes(self):
        runner.validate_result(0, SUCCESS, None)

    def test_success_requires_exit_zero(self):
        with self.assertRaises(RuntimeError):
            runner.validate_result(1, SUCCESS, None)

    def test_success_requires_final_empty_queue(self):
        with self.assertRaises(RuntimeError):
            runner.validate_result(0, SUCCESS + "101 states generated, 51 distinct states found, 1 states left on queue.\n", None)

    def test_success_requires_completion_marker(self):
        with self.assertRaises(RuntimeError):
            runner.validate_result(0, SUCCESS.splitlines()[1], None)

    def test_initial_state_only_is_not_coverage(self):
        with self.assertRaises(RuntimeError):
            runner.validate_result(0, SUCCESS.replace("50 distinct", "1 distinct"), None)

    def test_expected_counterexample_passes(self):
        runner.validate_result(12, COUNTEREXAMPLE, "ReadIndexSafety")

    def test_wrong_invariant_does_not_count(self):
        with self.assertRaises(RuntimeError):
            runner.validate_result(12, COUNTEREXAMPLE, "AppliedBarrier")

    def test_tool_error_does_not_count(self):
        with self.assertRaises(RuntimeError):
            runner.validate_result(255, COUNTEREXAMPLE, "ReadIndexSafety")

    def test_missing_counterexample_does_not_count(self):
        with self.assertRaises(RuntimeError):
            runner.validate_result(12, COUNTEREXAMPLE.splitlines()[0], "ReadIndexSafety")

    def test_negative_that_passes_is_rejected(self):
        with self.assertRaises(RuntimeError):
            runner.validate_result(0, SUCCESS, "ReadIndexSafety")

    def test_unexpected_invariant_on_positive_is_rejected(self):
        with self.assertRaises(RuntimeError):
            runner.validate_result(0, SUCCESS + COUNTEREXAMPLE, None)

    def test_timeout_never_counts_as_counterexample(self):
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(runner.check, "CACHE", Path(directory)), \
                    mock.patch.object(runner.check, "classpath", return_value="tools.jar"), \
                    mock.patch.object(runner.subprocess, "run", side_effect=subprocess.TimeoutExpired("java", 1)), \
                    contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(RuntimeError, "NOT a pass"):
                    runner.run("no-current-term", 1)


if __name__ == "__main__":
    unittest.main()
