import copy
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

import read_trace_check as trace
from test_trace_check import event


def observation(kind, seq, node=0, value=""):
    result = event(kind, seq, node)
    if value:
        result["value"] = value
    state = dict(waiting=[], pending=[], ready=[], tracked=[], applied=0, completed=[], value="", writes=[])
    result["reads"] = [copy.deepcopy(state) for _ in range(3)]
    return result


def heartbeat(ctx="read-1"):
    return dict(kind="heartbeat", **{"from": 1, "to": 2}, term=1, index=0, log_term=0,
                commit=0, commit_term=0, reject=False, reject_hint=0, entries=[], context=ctx)


class ReadTraceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "read.ndjson"
        self.events = [observation("init", 0), observation("campaign", 1, 1),
                       observation("propose", 2, 1), observation("read", 3, 1, "read-1"),
                       observation("complete_reads", 4, 1), observation("end", 5)]
        trace.write_events(self.path, self.events)

    def rejects(self, mutate):
        rows = [json.loads(line) for line in self.path.read_text().splitlines()]
        mutate(rows)
        self.path.write_text("".join(json.dumps(row) + "\n" for row in rows))
        with self.assertRaises((ValueError, TypeError, KeyError)):
            trace.load(self.path)

    def test_every_observation_is_consumed_by_both_checks(self):
        events = trace.load(self.path)
        self.assertEqual(events, self.events)
        self.assertEqual(len(trace.instructions(events)), len(events))
        self.assertEqual([trace.project(e)["seq"] for e in events], list(range(len(events))))
        self.assertEqual(trace.project(events[3])["kind"], "beat")
        self.assertEqual(trace.project(events[4])["kind"], "ready")

    def test_context_is_preserved_in_read_check_only(self):
        self.events[3]["pending"] = [heartbeat()]
        trace.write_events(self.path, self.events)
        events = trace.load(self.path)
        step = trace.instructions(events)[3]
        self.assertEqual(step["pending"][0]["context"], "read-1".encode().hex())
        self.assertNotIn("context", trace.project(events[3])["pending"][0])
        self.assertEqual(events[3]["pending"][0]["context"], "read-1")

    def test_semantic_read_corruption_reaches_model(self):
        self.events[3]["reads"][0]["completed"] = [dict(context="read-1", index=0, value="")]
        trace.write_events(self.path, self.events)
        events = trace.load(self.path)
        self.assertEqual(len(trace.instructions(events)[3]["reads"][0]["completed"]), 1)

    def test_legacy_header_cannot_enable_reads(self):
        self.rejects(lambda rows: rows[0].update(version=1, read_index=False))

    def test_unknown_feature_is_rejected(self):
        self.rejects(lambda rows: rows[0].update(check_quorum=True))

    def test_missing_terminal(self):
        self.rejects(lambda rows: rows.pop())

    def test_appended_suffix(self):
        self.rejects(lambda rows: rows.append({}))

    def test_missing_sequence(self):
        self.rejects(lambda rows: rows[3].update(seq=12))

    def test_unknown_read_state_field(self):
        self.rejects(lambda rows: rows[1]["reads"][0].update(ignored=True))

    def test_unknown_event(self):
        self.rejects(lambda rows: rows[3].update(kind="snapshot"))

    def test_restart_not_in_read_profile(self):
        self.rejects(lambda rows: rows[3].update(kind="restart"))

    def test_empty_context(self):
        self.rejects(lambda rows: rows[4].update(value=""))

    def test_boolean_is_not_applied_index(self):
        self.rejects(lambda rows: rows[3]["reads"][0].update(applied=True))

    def test_duplicate_ack_is_not_silently_collapsed(self):
        self.rejects(lambda rows: rows[4]["reads"][0]["pending"].append(
            dict(context="read-1", index=1, acks=[1, 1])))

    def test_boolean_is_not_ack_id(self):
        self.rejects(lambda rows: rows[4]["reads"][0]["pending"].append(
            dict(context="read-1", index=1, acks=[True])))

    def test_context_on_vote_not_silently_stripped(self):
        message = dict(heartbeat(), kind="request_vote")
        self.rejects(lambda rows: rows[3]["pending"].append(message))

    def test_truncated_line(self):
        self.path.write_bytes(self.path.read_bytes()[:-1])
        with self.assertRaises(ValueError):
            trace.load(self.path)

    def test_duplicate_json_key(self):
        self.path.write_text(self.path.read_text().replace('"version": 3', '"version": 3, "version": 3'))
        with self.assertRaises(ValueError):
            trace.load(self.path)

    def test_invalid_apply_target(self):
        self.rejects(lambda rows: rows[5].update(kind="apply", value="01"))

    def run_mock(self, code=0, output="", timeout=False, history_ok=True):
        def run(command, **kwargs):
            kwargs["stdout"].write(output)
            if timeout:
                raise subprocess.TimeoutExpired(command, 1)
            return subprocess.CompletedProcess(command, code)

        with mock.patch.object(trace.check, "CACHE", Path(self.temp.name)), \
                mock.patch.object(trace.check, "classpath", return_value="tools.jar"), \
                mock.patch.object(trace.core, "validate", return_value=Path("checked-consensus")) as consensus, \
                mock.patch.object(trace.subprocess, "run", side_effect=run), \
                mock.patch.object(trace.history_check, "check", return_value=(history_ok, Path("checked-history"))) as history:
            result = trace.validate(self.path, 1)
            consensus.assert_called_once()
            history.assert_called_once()
            return result

    def test_timeout_is_not_negative_evidence(self):
        with self.assertRaisesRegex(RuntimeError, "timeout, NOT a pass"):
            self.run_mock(timeout=True)

    def test_tool_error_is_not_negative_evidence(self):
        with self.assertRaisesRegex(RuntimeError, "Incomplete or failed"):
            self.run_mock(code=150, output="Error: Deadlock reached.")

    def test_wrong_invariant_is_not_negative_evidence(self):
        with self.assertRaisesRegex(RuntimeError, "Incomplete or failed"):
            self.run_mock(code=12, output="Error: Invariant WrongInv is violated.")

    def test_incomplete_search_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "Incomplete or failed"):
            self.run_mock(output='<<"RAFTZ_TRACE_COMPLETE", 6>>\n')

    def test_success_requires_consensus_and_full_terminal(self):
        self.run_mock(output='<<"RAFTZ_TRACE_COMPLETE", 6>>\n'
                      'Model checking completed. No error has been found.\n'
                      '8 states generated, 7 distinct states found, 0 states left on queue.\n')

    def test_non_linearizable_history_cannot_be_bypassed(self):
        with self.assertRaisesRegex(trace.core.ConformanceError, "not linearizable"):
            self.run_mock(history_ok=False, output='<<"RAFTZ_TRACE_COMPLETE", 6>>\n'
                          'Model checking completed. No error has been found.\n'
                          '8 states generated, 7 distinct states found, 0 states left on queue.\n')

    def test_unknown_application_is_rejected(self):
        self.rejects(lambda rows: rows[0].update(application="other"))

    def test_missing_return_value_is_rejected(self):
        self.rejects(lambda rows: rows[5]["reads"][0]["completed"].append(dict(context="read-1", index=0)))

    def test_consensus_failure_cannot_be_bypassed(self):
        with mock.patch.object(trace.check, "CACHE", Path(self.temp.name)), \
                mock.patch.object(trace.check, "classpath", return_value="tools.jar"), \
                mock.patch.object(trace.core, "validate", side_effect=trace.core.ConformanceError("consensus rejected")), \
                mock.patch.object(trace.subprocess, "run") as run:
            with self.assertRaisesRegex(trace.core.ConformanceError, "consensus rejected"):
                trace.validate(self.path, 1)
            run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
