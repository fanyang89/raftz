import contextlib
import io
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

import history_check as history


WITNESS = '''<<"RAFTZ_HISTORY_COMPLETE", 1>>
Error: Invariant NoLinearization is violated.
Error: The behavior up to this point is:
State 1: <Initial predicate>
State 2: <Linearize(1)>
'''
EXHAUSTED = '''Model checking completed. No error has been found.
1 states generated, 1 distinct states found, 0 states left on queue.
'''


def read():
    return dict(id="r", kind="read", value="", result="", invoked=0, returned=1)


def event(kind, seq, value="", completed=None, writes=None, node=1):
    states = [dict(completed=[], writes=[]) for _ in range(3)]
    states[node-1] = dict(completed=completed or [], writes=writes or [])
    return dict(kind=kind, seq=seq, node=node, value=value, reads=states,
                proposal_context=value if kind == "propose" else "")


class HistoryTests(unittest.TestCase):
    def test_exact_witness_passes(self):
        self.assertTrue(history.verdict(12, WITNESS, 1))

    def test_exhaustive_dead_end_is_non_linearizable(self):
        self.assertFalse(history.verdict(0, EXHAUSTED, 1))

    def test_wrong_marker_rejected(self):
        with self.assertRaises(RuntimeError):
            history.verdict(12, WITNESS, 2)

    def test_wrong_invariant_rejected(self):
        with self.assertRaises(RuntimeError):
            history.verdict(12, WITNESS.replace("NoLinearization", "HistoryTypeInv"), 1)

    def test_tool_error_rejected(self):
        with self.assertRaises(RuntimeError):
            history.verdict(150, WITNESS, 1)

    def test_missing_behavior_rejected(self):
        with self.assertRaises(RuntimeError):
            history.verdict(12, WITNESS.split("State 1")[0], 1)

    def test_incomplete_search_not_a_negative(self):
        with self.assertRaises(RuntimeError):
            history.verdict(0, EXHAUSTED.replace("0 states left", "10 states left"), 1)

    def test_only_final_statistics_count(self):
        with self.assertRaises(RuntimeError):
            history.verdict(0, EXHAUSTED + EXHAUSTED.replace("0 states left", "1 states left"), 1)

    def test_no_completion_marker_not_a_negative(self):
        with self.assertRaises(RuntimeError):
            history.verdict(0, EXHAUSTED.splitlines()[1], 1)

    def test_successful_search_with_witness_marker_is_not_negative(self):
        with self.assertRaises(RuntimeError):
            history.verdict(0, EXHAUSTED + '<<"RAFTZ_HISTORY_COMPLETE", 1>>', 1)

    def test_duplicate_operation_id_rejected(self):
        with self.assertRaises(ValueError):
            history.validate_history([read(), read()])

    def test_response_before_invocation_rejected(self):
        with self.assertRaises(ValueError):
            history.validate_history([dict(read(), invoked=2)])

    def test_pending_result_rejected(self):
        with self.assertRaises(ValueError):
            history.validate_history([read(), dict(read(), id="pending", returned=-1, result="A")])

    def test_no_completed_operations_rejected(self):
        with self.assertRaises(ValueError):
            history.validate_history([dict(read(), returned=-1)])

    def test_empty_history_rejected(self):
        with self.assertRaises(ValueError):
            history.validate_history([])

    def test_boolean_time_rejected(self):
        with self.assertRaises(ValueError):
            history.validate_history([dict(read(), invoked=False)])
        with self.assertRaises(ValueError):
            history.validate_history([dict(read(), returned=True)])

    def test_unknown_fields_rejected(self):
        with self.assertRaises(ValueError):
            history.validate_history([dict(read(), ignored=True)])

    def test_extract_keeps_pending_operations_and_actual_results(self):
        events = [event("propose", 0, "A"), event("read", 1, "r"),
                  event("apply", 2, writes=[dict(context="A", value="A", completed=1)]),
                  event("complete_reads", 3, completed=[dict(context="r", index=1, value="A")],
                        writes=[dict(context="A", value="A", completed=1)]), event("propose", 4, "pending",
                        completed=[dict(context="r", index=1, value="A")], writes=[dict(context="A", value="A", completed=1)])]
        operations = history.extract(events)
        self.assertEqual([op["returned"] for op in operations], [2, 3, -1])
        self.assertEqual(operations[1]["result"], "A".encode().hex())
        history.validate_history(operations)

    def test_apply_without_write_callback_does_not_acknowledge_write(self):
        operations = history.extract([event("propose", 0, "A"), event("apply", 1)])
        self.assertEqual(operations[0]["returned"], -1)

    def test_equal_values_are_not_merged_and_callbacks_use_identity(self):
        first, second = event("propose", 0, "same"), event("propose", 1, "same")
        first["proposal_context"], second["proposal_context"] = "a", "b"
        applied = event("apply", 2, writes=[dict(context="a", value="same", completed=1),
                                           dict(context="b", value="same", completed=0)])
        operations = history.extract([first, second, applied])
        self.assertEqual(len(operations), 2)
        self.assertEqual([op["returned"] for op in operations], [2, -1])
        self.assertEqual(operations[0]["value"], operations[1]["value"])
        self.assertNotEqual(operations[0]["id"], operations[1]["id"])
        history.validate_history(operations)

    def test_unknown_write_identity_cannot_match_by_value(self):
        proposal = event("propose", 0, "same")
        proposal["proposal_context"] = "a"
        with self.assertRaisesRegex(ValueError, "Unknown completion"):
            history.extract([proposal, event("apply", 1, writes=[dict(context="b", value="same", completed=1)])])

    def test_wrong_completion_owner_rejected(self):
        with self.assertRaises(ValueError):
            history.extract([event("read", 0, "r"), event("complete_reads", 1, node=2,
                            completed=[dict(context="r", index=0, value="")])])

    def test_duplicate_callback_rejected(self):
        callback = dict(context="r", index=0, value="")
        with self.assertRaises(ValueError):
            history.extract([event("read", 0, "r"), event("complete_reads", 1, completed=[callback, callback])])

    def test_disappearing_callback_rejected(self):
        with self.assertRaises(ValueError):
            history.extract([event("read", 0, "r"), event("complete_reads", 1,
                            completed=[dict(context="r", index=0, value="")]), event("end", 2)])

    def test_changed_result_rejected(self):
        with self.assertRaises(ValueError):
            history.extract([event("read", 0, "r"), event("complete_reads", 1,
                            completed=[dict(context="r", index=0, value="")]), event("end", 2,
                            completed=[dict(context="r", index=0, value="changed")])])

    def test_callback_outside_completion_operation_rejected(self):
        with self.assertRaises(ValueError):
            history.extract([event("read", 0, "r"), event("end", 1,
                            completed=[dict(context="r", index=0, value="")])])

    def test_timeout_is_not_a_verdict(self):
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(history.tools, "CACHE", Path(directory)), \
                    mock.patch.object(history.tools, "classpath", return_value="tools.jar"), \
                    mock.patch.object(history.subprocess, "run", side_effect=subprocess.TimeoutExpired("java", 1)), \
                    contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(RuntimeError, "NOT a verdict"):
                    history.check([read()], 1)


if __name__ == "__main__":
    unittest.main()
