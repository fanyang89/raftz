import copy
import contextlib
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

import history_check as history
from test_history_check import EXHAUSTED, WITNESS, read


def write(identity="w", value="A", invoked=0, returned=1):
    return dict(id=identity, kind="write", value=value, result="", invoked=invoked, returned=returned)


def raw_witness(operations, order):
    states = [[1, dict(done=[], value="")]]
    actions = []
    for identity in order:
        index = next(i for i, op in enumerate(operations, 1) if op["id"] == identity)
        op = operations[index - 1]
        after = [len(states) + 1, dict(done=sorted(set(states[-1][1]["done"]) | {index}),
                 value=op["value"] if op["kind"] == "write" else states[-1][1]["value"])]
        actions.append([states[-1], dict(name="Linearize", location={}, context=dict(i=index), parameters=["i"]), after])
        states.append(after)
    return dict(vars=["done", "value"], counterexample=dict(state=states, action=actions))


class ReplayTests(unittest.TestCase):
    def test_pending_write_can_explain_read(self):
        ops = [write(returned=-1), dict(read(), invoked=1, returned=2, result="A")]
        witness = history.replay(ops, ["w", "r"])
        self.assertEqual(witness["values"], ["", "A", "A"])
        self.assertEqual(witness["omitted"], [])

    def test_unused_pending_operations_can_be_omitted(self):
        ops = [read(), write(returned=-1), dict(read(), id="pending", returned=-1)]
        self.assertEqual(history.replay(ops, ["r"])["omitted"], ["w", "pending"])

    def test_pending_read_cannot_be_selected(self):
        with self.assertRaisesRegex(ValueError, "Pending read"):
            history.replay([read(), dict(read(), id="pending", returned=-1)], ["pending", "r"])

    def test_future_pending_write_cannot_explain_earlier_read(self):
        with self.assertRaisesRegex(ValueError, "real-time"):
            history.replay([write(invoked=2, returned=-1), dict(read(), result="A")], ["w", "r"])

    def test_realtime_precedence_is_checked_even_if_values_match(self):
        ops = [write(value=""), dict(read(), invoked=2, returned=3)]
        with self.assertRaisesRegex(ValueError, "real-time"):
            history.replay(ops, ["r", "w"])

    def test_tied_endpoints_are_not_strict_precedence(self):
        ops = [write(value=""), dict(read(), invoked=1, returned=2)]
        history.replay(ops, ["r", "w"])

    def test_repeated_values_do_not_allow_omitting_completed_identity(self):
        ops = [write(), write(identity="other")]
        with self.assertRaisesRegex(ValueError, "omits a completed"):
            history.replay(ops, ["w"])
        history.replay(ops, ["w", "other"])

    def test_unknown_duplicate_and_malformed_orders_rejected(self):
        ops = [read(), write(returned=-1)]
        for order in (["unknown"], ["r", "r"], [True], "r", [], ["r", "w", "r"]):
            with self.subTest(order=order), self.assertRaises(ValueError):
                history.replay(ops, order)

    def test_incorrect_read_value_rejected(self):
        with self.assertRaisesRegex(ValueError, "read does not match"):
            history.replay([dict(read(), result="unwritten")], ["r"])


class WitnessTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "witness.json"
        self.ops = [write(), dict(read(), invoked=2, returned=3, result="A")]
        self.raw = raw_witness(self.ops, ["w", "r"])

    def verify(self, raw):
        self.path.write_text(json.dumps(raw))
        return history.verify_witness(self.ops, self.path)

    def test_concrete_states_reconstruct_named_order(self):
        self.assertEqual(self.verify(self.raw), dict(version=1, order=["w", "r"], values=["", "A", "A"], omitted=[]))

    def test_structurally_bad_witnesses_rejected(self):
        mutations = {
            "unknown-variable": lambda r: r["vars"].append("ignored"),
            "unknown-field": lambda r: r.update(ignored=True),
            "missing-edge": lambda r: r["counterexample"]["action"].pop(),
            "wrong-number": lambda r: r["counterexample"]["state"][1].__setitem__(0, 4),
            "boolean-number": lambda r: r["counterexample"]["state"][0].__setitem__(0, True),
            "unknown-index": lambda r: r["counterexample"]["state"][1][1].update(done=[3]),
            "boolean-index": lambda r: r["counterexample"]["state"][1][1].update(done=[True]),
            "duplicate-index": lambda r: r["counterexample"]["state"][1][1].update(done=[1, 1]),
            "nonempty-initial": lambda r: r["counterexample"]["state"][0][1].update(done=[1]),
            "initial-value": lambda r: r["counterexample"]["state"][0][1].update(value="A"),
            "multiple-additions": lambda r: r["counterexample"]["state"][1][1].update(done=[1, 2]),
            "removed-operation": lambda r: r["counterexample"]["state"][2][1].update(done=[2]),
            "stutter": lambda r: r["counterexample"]["state"][2][1].update(done=[1]),
            "disconnected-edge": lambda r: r["counterexample"]["action"][0].__setitem__(0, [99, {}]),
            "wrong-action": lambda r: r["counterexample"]["action"][0][1].update(name="Other"),
            "wrong-action-index": lambda r: r["counterexample"]["action"][0][1]["context"].update(i=2),
            "boolean-action-index": lambda r: r["counterexample"]["action"][0][1]["context"].update(i=True),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name), self.assertRaises(ValueError):
                raw = copy.deepcopy(self.raw)
                mutate(raw)
                self.verify(raw)

    def test_consistent_edges_with_wrong_register_value_rejected(self):
        self.raw["counterexample"]["state"][-1][1]["value"] = "wrong"
        with self.assertRaisesRegex(ValueError, "independent replay"):
            self.verify(self.raw)

    def test_valid_prefix_is_not_a_complete_witness(self):
        self.raw["counterexample"]["state"].pop()
        self.raw["counterexample"]["action"].pop()
        with self.assertRaisesRegex(ValueError, "omits a completed"):
            self.verify(self.raw)

    def test_duplicate_json_keys_rejected(self):
        self.path.write_text(json.dumps(self.raw).replace('"value": ""', '"value": "", "value": ""', 1))
        with self.assertRaises(ValueError):
            history.verify_witness(self.ops, self.path)

    def test_missing_empty_oversized_or_truncated_artifacts_rejected(self):
        with self.assertRaises(OSError):
            history.verify_witness(self.ops, self.path)
        for content in ("", " " * 4_000_001, "{"):
            with self.subTest(size=len(content)), self.assertRaises(ValueError):
                self.path.write_text(content)
                history.verify_witness(self.ops, self.path)

    def run_check(self, raw, returncode=12, output=WITNESS):
        def run(command, cwd, stdout, **kwargs):
            offset = command.index("-dumpTrace")
            self.assertEqual(command[offset+1:offset+3], ["json", "witness.raw.json"])
            stdout.write(output)
            if raw is not None:
                (cwd / "witness.raw.json").write_text(json.dumps(raw))
            return subprocess.CompletedProcess(command, returncode)

        with mock.patch.object(history.tools, "CACHE", Path(self.temp.name)), \
                mock.patch.object(history.tools, "classpath", return_value="tools.jar"), \
                mock.patch.object(history.subprocess, "run", side_effect=run), \
                contextlib.redirect_stdout(io.StringIO()):
            return history.check([read()])

    def test_positive_check_exports_only_replayed_witness(self):
        success, path = self.run_check(raw_witness([read()], ["r"]))
        self.assertTrue(success)
        self.assertEqual(json.loads((path / "linearization.json").read_text())["order"], ["r"])

    def test_success_log_without_artifact_is_not_a_positive_verdict(self):
        with self.assertRaises(OSError):
            self.run_check(None)

    def test_success_log_with_invalid_artifact_is_not_a_positive_verdict(self):
        raw = raw_witness([read()], ["r"])
        raw["counterexample"]["state"][-1][1]["value"] = "wrong"
        with self.assertRaises(ValueError):
            self.run_check(raw)
        self.assertFalse(list(Path(self.temp.name).glob("runs/*/linearization.json")))

    def test_negative_verdict_rejects_contradictory_witness_artifact(self):
        with self.assertRaisesRegex(ValueError, "Unexpected witness artifact"):
            self.run_check(raw_witness([read()], ["r"]), 0, EXHAUSTED)

    def test_exhaustion_without_witness_remains_negative(self):
        success, path = self.run_check(None, 0, EXHAUSTED)
        self.assertFalse(success)
        self.assertFalse((path / "linearization.json").exists())
