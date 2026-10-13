#!/usr/bin/env python3
"""Find a linearization of a finite register history with TLC (existential reachability)."""

import argparse
import copy
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

import check as tools
import trace_check as core


def validate_history(operations):
    core.require(isinstance(operations, list) and 0 < len(operations) <= 16, "Unsupported history size")
    ids = set()
    for op in operations:
        core.fields(op, "id kind value result invoked returned")
        core.require(isinstance(op["id"], str) and 0 < len(op["id"]) <= 9000 and op["id"] not in ids, "Invalid/duplicate operation ID")
        ids.add(op["id"])
        core.require(op["kind"] in ("read", "write"), "Unknown operation")
        for field in ("value", "result"):
            core.require(isinstance(op[field], str) and len(op[field].encode("utf-8")) <= 8192, "Unsupported register value")
        core.natural(op["invoked"], 1000)
        core.require(type(op["returned"]) is int and -1 <= op["returned"] <= 1000, "Invalid return time")
        core.require(op["returned"] == -1 or op["returned"] >= op["invoked"], "Response precedes invocation")
        core.require(op["kind"] != "read" or op["value"] == "", "Read has a write argument")
        core.require(op["kind"] != "write" or op["result"] == "", "Write has an unexpected result")
        core.require(op["returned"] != -1 or op["result"] == "", "Pending operation has a result")
    core.require(any(op["returned"] >= 0 for op in operations), "No completed operations")


def extract(events):
    operations = {}
    owners = {}
    for event in events:
        if event["kind"] in ("read", "propose"):
            kind = "write" if event["kind"] == "propose" else "read"
            identity = event["proposal_context"] if kind == "write" else event["value"]
            key = (kind, identity)
            core.require(key not in operations, "Repeated history identity")
            operations[key] = dict(id=kind + ":" + core.payload(identity), kind=kind,
                                   value=core.payload(event["value"]) if kind == "write" else "",
                                   result="", invoked=event["seq"], returned=-1)
            owners[key] = event["node"]
        for node, state in enumerate(event["reads"], 1):
            seen = set()
            returns = [(("read", r["context"]), r["value"]) for r in state["completed"]]
            for write in state["writes"]:
                core.require(write["completed"] in (0, 1), "Duplicate write completion")
                if write["completed"]:
                    returns.append((("write", write["context"]), ""))
            for key, value in returns:
                core.require(key not in seen, "Duplicate read completion")
                seen.add(key)
                core.require(key in operations and owners[key] == node, "Unknown completion or wrong owner")
                op = operations[key]
                result = core.payload(value)
                if op["returned"] == -1:
                    core.require(event["kind"] == ("apply" if key[0] == "write" else "complete_reads"), "Completion outside callback operation")
                    op.update(returned=event["seq"], result=result)
                else:
                    core.require(op["result"] == result, "Completed result changed")
            core.require(all(key in seen for key, op in operations.items()
                             if owners[key] == node and op["returned"] >= 0), "Completed operation disappeared")
    return list(operations.values())


def verdict(returncode, text, completed):
    violations = re.findall(r"^Error: Invariant (\w+) is violated\.$", text, re.MULTILINE)
    if (returncode == 12 and violations == ["NoLinearization"]
            and f'<<"RAFTZ_HISTORY_COMPLETE", {completed}>>' in text
            and "Error: The behavior up to this point is:" in text
            and re.search(r"^State 2: ", text, re.MULTILINE)):
        return True
    summaries = re.findall(
        r"^([0-9]+) states generated, ([0-9]+) distinct states found, ([0-9]+) states left on queue\.$",
        text, re.MULTILINE,
    )
    if (returncode == 0 and not violations
            and "Model checking completed. No error has been found." in text
            and "RAFTZ_HISTORY_COMPLETE" not in text
            and summaries and int(summaries[-1][1]) >= 1 and summaries[-1][2] == "0"):
        return False
    raise RuntimeError("TLC did not produce an exact linearization witness or finish excluding all linearizations")


def replay(operations, order):
    validate_history(operations)
    core.require(isinstance(order, list) and len(order) <= len(operations)
                 and all(isinstance(identity, str) for identity in order), "Invalid linearization order")
    by_id = {op["id"]: op for op in operations}
    done, values = set(), [""]
    for identity in order:
        core.require(identity in by_id and identity not in done, "Unknown or repeated witness operation")
        op = by_id[identity]
        core.require(op["returned"] >= 0 or op["kind"] == "write", "Pending read in witness")
        core.require(all(other["id"] in done for other in operations
                         if 0 <= other["returned"] < op["invoked"]), "Witness violates real-time precedence")
        if op["kind"] == "read":
            core.require(op["result"] == values[-1], "Witness read does not match register value")
        values.append(op["value"] if op["kind"] == "write" else values[-1])
        done.add(identity)
    core.require(all(op["id"] in done for op in operations if op["returned"] >= 0),
                 "Witness omits a completed operation")
    return dict(version=1, order=list(order), values=values,
                omitted=[op["id"] for op in operations if op["id"] not in done])


def verify_witness(operations, path):
    validate_history(operations)
    data = path.read_bytes()
    core.require(0 < len(data) <= 4_000_000, "Empty or oversized witness")
    raw = json.loads(data, object_pairs_hook=core.unique_object)
    core.fields(raw, "counterexample vars")
    core.require(raw["vars"] == ["done", "value"], "Unexpected witness variables")
    trace = raw["counterexample"]
    core.fields(trace, "state action")
    states, actions = trace["state"], trace["action"]
    core.require(isinstance(states, list) and 2 <= len(states) <= len(operations) + 1,
                 "Invalid witness state count")
    core.require(isinstance(actions, list) and len(actions) == len(states) - 1,
                 "Missing witness transitions")
    previous, order, values = set(), [], []
    for position, state in enumerate(states, 1):
        core.require(isinstance(state, list) and len(state) == 2
                     and type(state[0]) is int and state[0] == position, "Invalid witness state numbering")
        core.fields(state[1], "done value")
        selected = state[1]["done"]
        core.require(isinstance(selected, list) and len(selected) <= len(operations)
                     and all(type(i) is int and 1 <= i <= len(operations) for i in selected),
                     "Invalid witness operation indices")
        done = set(selected)
        core.require(len(done) == len(selected), "Duplicate witness set member")
        values.append(state[1]["value"])
        if position == 1:
            core.require(not done and values[-1] == "", "Invalid witness initial state")
        else:
            core.require(previous < done and len(done) == len(previous) + 1,
                         "Witness transition must add exactly one operation")
            index = (done - previous).pop()
            order.append(operations[index - 1]["id"])
            edge = actions[position - 2]
            core.require(isinstance(edge, list) and len(edge) == 3
                         and core.key(edge[0]) == core.key(states[position - 2])
                         and core.key(edge[2]) == core.key(state), "Disconnected witness transition")
            core.fields(edge[1], "name location context parameters")
            core.fields(edge[1]["context"], "i")
            core.require(edge[1]["name"] == "Linearize" and edge[1]["parameters"] == ["i"]
                         and type(edge[1]["context"]["i"]) is int and edge[1]["context"]["i"] == index,
                         "Witness action disagrees with selected operation")
        previous = done
    witness = replay(operations, order)
    core.require(values == witness["values"], "TLC witness values disagree with independent replay")
    return witness


def check(operations, timeout=60):
    validate_history(operations)
    cp = tools.classpath()
    runs = tools.CACHE / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    run = Path(tempfile.mkdtemp(prefix="register-history-", dir=runs))
    (run / "history.ndjson").write_text("".join(json.dumps(op) + "\n" for op in operations))
    shutil.copy2(tools.ROOT / "models" / "RaftzRegisterHistory.tla", run / "RaftzRegisterHistory.tla")
    (run / "RaftzRegisterHistory.cfg").write_text(
        "SPECIFICATION Spec\nINVARIANTS HistoryTypeInv NoLinearization\nCHECK_DEADLOCK FALSE\n")
    command = ["java", "-Xmx2g", "-XX:+UseParallelGC", "-cp", cp, "tlc2.TLC", "-workers", "1",
               "-dumpTrace", "json", "witness.raw.json",
               "-metadir", str(run / "states"), "-config", "RaftzRegisterHistory.cfg", "RaftzRegisterHistory.tla"]
    (run / "command.json").write_text(json.dumps(command, indent=2) + "\n")
    print(f"Register history artifacts: {run}", flush=True)
    with (run / "tlc.log").open("w") as output:
        try:
            process = subprocess.run(command, cwd=run, stdout=output, stderr=subprocess.STDOUT, timeout=timeout)
        except subprocess.TimeoutExpired as error:
            raise RuntimeError(f"History TLC timeout, NOT a verdict: {run}") from error
    text = (run / "tlc.log").read_text()
    linearizable = verdict(process.returncode, text, sum(op["returned"] >= 0 for op in operations))
    witness_path = run / "witness.raw.json"
    if linearizable:
        witness = verify_witness(operations, witness_path)
        (run / "linearization.json").write_text(json.dumps(witness, indent=2) + "\n")
    else:
        core.require(not witness_path.exists(), "Unexpected witness artifact for an exhausted history")
    print(f"History {'has a linearization witness' if linearizable else 'has no linearization'}: {len(operations)} operations", flush=True)
    return linearizable, run


def witness_negatives(operations, run):
    raw = json.loads((run / "witness.raw.json").read_text())
    witness = verify_witness(operations, run / "witness.raw.json")
    directory = run / "witness-mutations"
    directory.mkdir()
    wrong_value, incomplete = copy.deepcopy(raw), copy.deepcopy(raw)
    wrong_value["counterexample"]["state"][-1][1]["value"] = "unwritten"
    wrong_value["counterexample"]["action"][-1][2][1]["value"] = "unwritten"
    incomplete["counterexample"]["state"].pop()
    incomplete["counterexample"]["action"].pop()
    cases = (
        ("wrong-register", wrong_value, "independent replay"),
        ("missing-completion", incomplete, "omits a completed"),
        ("reversed-realtime", list(reversed(witness["order"])), "real-time"),
        ("duplicate-operation", witness["order"][:-1] + witness["order"][:1], "Unknown or repeated"),
    )
    for name, data, reason in cases:
        path = directory / f"{name}.json"
        path.write_text(json.dumps(data, indent=2) + "\n")
        try:
            if isinstance(data, list):
                replay(operations, data)
            else:
                verify_witness(operations, path)
        except ValueError as error:
            if reason not in str(error):
                raise
            print(f"PASS witness semantic negative: {name}", flush=True)
        else:
            raise RuntimeError(f"Witness mutation unexpectedly accepted: {name}")


def regressions(timeout):
    def op(name, kind, invoked, returned, value="", result=""):
        return dict(id=name, kind=kind, invoked=invoked, returned=returned, value=value, result=result)

    cases = {
        "initial-read": (True, [op("r", "read", 0, 1)]),
        "overlapping-orders": (True, [op("a", "write", 0, 5, value="A"),
                                     op("b", "write", 1, 5, value="B"),
                                     op("ra", "read", 2, 3, result="A"),
                                     op("rb", "read", 6, 7, result="B")]),
        "pending-write": (True, [op("w", "write", 0, -1, value="A"), op("r", "read", 1, 2, result="A")]),
        "future-pending-write": (False, [op("r", "read", 0, 1, result="A"), op("w", "write", 2, -1, value="A")]),
        "unwritten-value": (False, [op("r", "read", 0, 1, result="ghost")]),
        "stale-read": (False, [op("w", "write", 0, 1, value="A"), op("r", "read", 2, 3)]),
        "repeated-values": (True, [op("a", "write", 0, 1, value="A"), op("b", "write", 2, 3, value="A"),
                                   op("r", "read", 4, 5, result="A")]),
        "pending-read": (True, [op("w", "write", 0, 1, value="A"), op("r", "read", 2, -1)]),
    }
    for name, (expected, operations) in cases.items():
        actual, run = check(operations, timeout)
        if actual != expected:
            raise RuntimeError(f"Unexpected register history verdict: {name}")
        print(f"PASS history regression: {name}", flush=True)
        if name == "overlapping-orders":
            witness_negatives(operations, run)


def negatives(operations, timeout):
    writes = [op for op in operations if op["kind"] == "write"]
    reads = [op for op in operations if op["kind"] == "read" and op["returned"] >= 0]
    early = next(op for op in reads if op["returned"] < min(w["invoked"] for w in writes))
    late = next(op for op in reads if any(0 <= w["returned"] < op["invoked"] for w in writes))
    for name, target, value in (("future-value", early, writes[0]["value"]), ("stale-value", late, "")):
        mutated = [dict(op, result=value) if op["id"] == target["id"] else dict(op) for op in operations]
        linearizable, _ = check(mutated, timeout)
        if linearizable:
            raise RuntimeError(f"History mutation unexpectedly linearizable: {name}")
        print(f"PASS history semantic negative: {name}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path, help="JSON array of register operations")
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()
    core.require(args.timeout > 0, "Timeout must be positive")
    data = args.path.read_bytes()
    core.require(0 < len(data) <= 1_000_000, "Empty or oversized history")
    operations = json.loads(data, object_pairs_hook=core.unique_object)
    linearizable, _ = check(operations, args.timeout)
    if not linearizable:
        raise core.ConformanceError("History is not linearizable")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, TypeError, OSError, RuntimeError) as error:
        print(f"error: {error}", file=sys.stderr)
        sys.exit(1)
