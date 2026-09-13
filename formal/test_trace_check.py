import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import trace_check as trace


def event(kind, seq, node=0):
    state = dict(term=0, vote=0, commit=0, role="follower", log=[], durable_term=0,
                 durable_vote=0, durable_commit=0, durable_log=[])
    return dict(kind=kind, seq=seq, node=node, input=None,
                value="test" if kind == "propose" else "",
                states=[copy.deepcopy(state) for _ in range(3)], pending=[], network=[])


class TraceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "trace.ndjson"
        self.events = [event("init", 0), event("campaign", 1, 1), event("propose", 2, 1), event("end", 3)]
        trace.write_events(self.path, self.events)

    def rejects(self, mutate):
        rows = [json.loads(line) for line in self.path.read_text().splitlines()]
        mutate(rows)
        self.path.write_text("".join(json.dumps(row) + "\n" for row in rows))
        with self.assertRaises((ValueError, KeyError, TypeError)):
            trace.load(self.path)

    def test_complete_schema_consumes_every_event(self):
        self.assertEqual(trace.load(self.path), self.events)
        steps = trace.instructions(self.events)
        self.assertEqual([s["event"] for s in steps if s["op"] == "check"], [0, 1, 2, 3])
        self.assertEqual([s["op"] for s in steps if s["event"] == 1], ["timeout", "selfVote", "check"])
        self.assertTrue(all(s["bound"] == 36 for s in steps))

    def test_empty(self):
        self.path.write_bytes(b"")
        with self.assertRaises(ValueError):
            trace.load(self.path)

    def test_missing_terminal(self):
        self.rejects(lambda rows: rows.pop())

    def test_appended_suffix(self):
        self.rejects(lambda rows: rows.append({}))

    def test_unknown_event(self):
        self.rejects(lambda rows: rows[2].update(kind="snapshot"))

    def test_unknown_field(self):
        self.rejects(lambda rows: rows[2].update(ignored=True))

    def test_skipped_sequence(self):
        self.rejects(lambda rows: rows[2].update(seq=30))

    def test_duplicate_json_key(self):
        self.path.write_text(self.path.read_text().replace('"version": 1', '"version": 1, "version": 1'))
        with self.assertRaises(ValueError):
            trace.load(self.path)

    def test_truncated_final_line(self):
        self.path.write_bytes(self.path.read_bytes()[:-1])
        with self.assertRaises(ValueError):
            trace.load(self.path)

    def test_wrong_feature_profile(self):
        self.rejects(lambda rows: rows[0].update(pre_vote=True))

    def test_boolean_is_not_version(self):
        self.rejects(lambda rows: rows[0].update(version=True))

    def test_malformed_log_index(self):
        with self.assertRaises(ValueError):
            trace.log([dict(index=2, term=1, data="command", context="")])
        with self.assertRaises(ValueError):
            trace.log([dict(index=True, term=1, data="command", context="")])

    def test_preserves_payload_context_and_noop_identity(self):
        values = [trace.log([dict(index=1, term=1, data=data, context=context)])[0]
                  for data, context in [("alpha", ""), ("beta", ""), ("alpha", "id"), ("", "")]]
        self.assertEqual(len({trace.key(v) for v in values}), 4)
        self.assertEqual(values[-1]["value"]["val"]["kind"], "noop")
        self.assertEqual(values[0]["value"]["val"]["data"], "616c706861")

    def test_semantic_corruption_is_not_filtered_before_tlc(self):
        self.events[1]["states"][0]["term"] = 9
        self.events[1]["states"][0]["vote"] = 2
        trace.write_events(self.path, self.events)
        steps = trace.instructions(trace.load(self.path))
        check = next(s for s in steps if s["op"] == "check" and s["event"] == 1)
        self.assertEqual(check["expected"]["states"][0]["term"], 9)
        self.assertEqual(check["expected"]["states"][0]["vote"], 2)

    def test_all_upstream_invariants_enabled_and_deadlocks_checked(self):
        cfg = trace.config()
        for invariant in trace.INVARIANTS:
            self.assertIn("    " + invariant + "\n", cfg)
        self.assertEqual(len(trace.INVARIANTS), 8)
        self.assertIn("CHECK_DEADLOCK TRUE", cfg)
        self.assertNotIn("CONSTRAINT", cfg)

    def test_completion_requires_exact_terminal_and_exhaustion(self):
        text = '<<"RAFTZ_TRACE_COMPLETE", 7>>\nModel checking completed. No error has been found.\n0 states left on queue.'
        self.assertTrue(trace.completed(0, text, 7))
        self.assertFalse(trace.completed(0, text, 8))
        self.assertFalse(trace.completed(1, text, 7))
        self.assertFalse(trace.completed(0, text.replace("0 states", "1 states"), 7))
        self.assertFalse(trace.completed(0, text.replace("RAFTZ_TRACE_COMPLETE", "Progress %"), 7))

    def test_timeout_is_not_a_semantic_negative(self):
        import subprocess
        with mock.patch.object(trace.check, "classpath", return_value="unused"), \
                mock.patch.object(trace.check, "CACHE", Path(self.temp.name)), \
                mock.patch.object(trace.subprocess, "run", side_effect=subprocess.TimeoutExpired("java", 1)):
            with self.assertRaisesRegex(RuntimeError, "timeout, NOT a pass"):
                trace.validate(self.path, 1)
        self.assertTrue(list(Path(self.temp.name).glob("runs/*/input.ndjson")))


if __name__ == "__main__":
    unittest.main()
