#!/usr/bin/env python3
"""Find a linearization of a finite register history with TLC (existential reachability)."""

import argparse
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
            key = (kind, event["value"])
            core.require(key not in operations, "Repeated history identity")
            operations[key] = dict(id=kind + ":" + core.payload(event["value"]), kind=kind,
                                   value=core.payload(event["value"]) if kind == "write" else "",
                                   result="", invoked=event["seq"], returned=-1)
            owners[key] = event["node"]
        for node, state in enumerate(event["reads"], 1):
            seen = set()
            returns = [(("read", r["context"]), r["value"]) for r in state["completed"]]
            for write in state["writes"]:
                core.require(write["completed"] in (0, 1), "Duplicate write completion")
                if write["completed"]:
                    returns.append((("write", write["value"]), ""))
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
    print(f"History {'has a linearization witness' if linearizable else 'has no linearization'}: {len(operations)} operations", flush=True)
    return linearizable, run


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
        "pending-read": (True, [op("w", "write", 0, 1, value="A"), op("r", "read", 2, -1)]),
    }
    for name, (expected, operations) in cases.items():
        actual, _ = check(operations, timeout)
        if actual != expected:
            raise RuntimeError(f"Unexpected register history verdict: {name}")
        print(f"PASS history regression: {name}", flush=True)


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
