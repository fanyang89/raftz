#!/usr/bin/env python3
"""Check complete RawNode/ProposalTracker read traces against consensus and read models."""

import argparse
import copy
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

import check
import history_check
import trace_check as core

HEADER = dict(core.HEADER, version=4, read_index=True, application="register-v1")
READ_EVENTS = {"read", "read_ready", "apply", "complete_reads"}


def context(value):
    result = core.payload(value)
    core.require(bool(result), "Empty read context")
    return result


def bounded_list(value):
    core.require(isinstance(value, list) and len(value) <= 16, "Unsupported read list")
    return value


def read_state(state):
    core.fields(state, "waiting pending ready tracked applied completed value writes")
    core.natural(state["applied"], 32)
    out = dict(waiting=[context(c) for c in bounded_list(state["waiting"])],
               pending=[], ready=[], tracked=[], applied=state["applied"], completed=[],
               value=core.payload(state["value"]), writes=[])
    for write in bounded_list(state["writes"]):
        core.fields(write, "context value completed")
        core.natural(write["completed"], 16)
        out["writes"].append(dict(context=context(write["context"]), value=context(write["value"]), completed=write["completed"]))
    for field in ("pending", "ready", "tracked", "completed"):
        for item in bounded_list(state[field]):
            core.fields(item, "context index acks" if field == "pending" else
                        "context index value" if field == "completed" else "context index")
            ctx = context(item["context"])
            index = item["index"]
            if field == "tracked" and index is None:
                index = -1
            else:
                core.natural(index, 32)
            result = dict(context=ctx, index=index)
            if field == "completed":
                result["value"] = core.payload(item["value"])
            if field == "pending":
                acks = bounded_list(item["acks"])
                core.require(all(type(n) is int and n in (1, 2, 3) for n in acks)
                             and len(set(acks)) == len(acks), "Invalid ACK set")
                result["acks"] = acks
            out[field].append(result)
    return out


def packet(message):
    core.require(isinstance(message, dict) and "context" in message, "Missing message context")
    plain = {k: v for k, v in message.items() if k != "context"}
    core.message(plain)
    ctx = core.payload(message["context"])
    core.require(not ctx or message["kind"] in ("heartbeat", "heartbeat_response"), "Unsupported contextual message")
    if ctx:
        return dict(kind=message["kind"], source=message["from"], dest=message["to"],
                    term=message["term"], context=ctx)
    return None


def project(event):
    result = copy.deepcopy(event)
    del result["reads"]
    if event["kind"] in READ_EVENTS:
        result["kind"] = "beat" if event["kind"] == "read" else "ready"
        result["value"] = ""
    for field in ("pending", "network"):
        for message in result[field]:
            del message["context"]
    if result["input"] is not None:
        del result["input"]["context"]
    return result


def load(path):
    data = Path(path).read_bytes()
    core.require(0 < len(data) <= 16_000_000 and data.endswith(b"\n"), "Empty, oversized or truncated trace")
    rows = [json.loads(line, object_pairs_hook=core.unique_object) for line in data.splitlines()]
    core.require(4 <= len(rows) <= 1002, "Unsupported event count")
    core.require(core.key(rows[0]) == core.key(HEADER), "Unsupported read trace header")
    core.require(core.key(rows[-1]) == core.key(dict(kind="terminal", events=len(rows)-2)), "Invalid terminal")
    events = rows[1:-1]
    core.require(events[0]["kind"] == "init" and events[-1]["kind"] == "end", "Missing init/end")
    for i, event in enumerate(events):
        core.fields(event, "kind seq node input value states pending network reads proposal_context")
        core.require(type(event["seq"]) is int and event["seq"] == i, "Invalid sequence")
        core.require((event["kind"] == "init") == (i == 0), "Unexpected init")
        core.require((event["kind"] == "end") == (i == len(events)-1), "Unexpected end")
        core.require(event["kind"] in core.EVENTS | READ_EVENTS and event["kind"] != "restart", "Unsupported event")
        core.require(isinstance(event["reads"], list) and len(event["reads"]) == 3, "Expected three read states")
        for state in event["reads"]:
            read_state(state)
        for field in ("pending", "network"):
            core.require(isinstance(event[field], list) and len(event[field]) <= 128, "Unsupported message bag")
            for message in event[field]:
                packet(message)
        if event["input"] is not None:
            packet(event["input"])
        if event["kind"] == "read":
            context(event["value"])
        elif event["kind"] == "apply":
            core.require(isinstance(event["value"], str) and event["value"] in {str(n) for n in range(1, 33)}, "Invalid apply target")
        elif event["kind"] in READ_EVENTS:
            core.require(event["value"] == "", "Unexpected read input")
        core.observation(project(event))
    core.require(any(e["kind"] == "read" for e in events)
                 and any(e["kind"] == "complete_reads" for e in events), "Vacuous read trace")
    return events


def environment(states):
    result = copy.deepcopy(states)
    for state in result:
        for field in ("log", "durable_log"):
            for entry in state[field]:
                entry["data"] = core.payload(entry["data"])
                entry["context"] = core.payload(entry["context"])
    return result


def instructions(events):
    result = []
    for i, event in enumerate(events):
        result.append(dict(
            kind=event["kind"], node=event["node"],
            has_packet=event["input"] is not None and packet(event["input"]) is not None,
            context=context(event["value"]) if event["kind"] == "read" else "",
            command=core.payload(event["value"]) if event["kind"] == "propose" else "",
            command_context=core.payload(event["proposal_context"]),
            target=int(event["value"]) if event["kind"] == "apply" else 0,
            packet=(packet(event["input"]) or "") if event["input"] is not None else "",
            before=environment(events[max(0, i-1)]["states"]), after=environment(event["states"]),
            reads=[read_state(s) for s in event["reads"]],
            pending=[p for m in event["pending"] if (p := packet(m)) is not None],
            network=[p for m in event["network"] if (p := packet(m)) is not None],
        ))
    return result


def write_events(path, events):
    events = copy.deepcopy(events)
    for i, event in enumerate(events):
        event["seq"] = i
    rows = [HEADER, *events, dict(kind="terminal", events=len(events))]
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))


def validate(path, timeout=60):
    events = load(path)
    cp = check.classpath()
    runs = check.CACHE / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    run = Path(tempfile.mkdtemp(prefix=f"read-trace-{Path(path).stem}-", dir=runs))
    shutil.copy2(path, run / "input.ndjson")
    projection = run / "consensus.ndjson"
    core.write_events(projection, [project(e) for e in events])
    consensus_run = core.validate(projection, timeout)
    (run / "consensus-run.txt").write_text(str(consensus_run) + "\n")
    steps = instructions(events)
    (run / "steps.ndjson").write_text("".join(json.dumps(s) + "\n" for s in steps))
    shutil.copy2(check.ROOT / "models" / "RaftzReadTrace.tla", run / "RaftzReadTrace.tla")
    (run / "RaftzReadTrace.cfg").write_text("SPECIFICATION TraceSpec\nINVARIANT ReadBarrierInv\nCHECK_DEADLOCK TRUE\n")
    command = ["java", "-Xmx2g", "-XX:+UseParallelGC", "-cp", cp, "tlc2.TLC", "-workers", "1",
               "-metadir", str(run / "states"), "-config", "RaftzReadTrace.cfg", "RaftzReadTrace.tla"]
    (run / "command.json").write_text(json.dumps(command, indent=2) + "\n")
    print(f"Read trace artifacts: {run}", flush=True)
    with (run / "tlc.log").open("w") as output:
        try:
            process = subprocess.run(command, cwd=run, stdout=output, stderr=subprocess.STDOUT, timeout=timeout)
        except subprocess.TimeoutExpired as error:
            raise RuntimeError(f"Read TLC timeout, NOT a pass: {run}") from error
    text = (run / "tlc.log").read_text()
    print("\n".join(text.splitlines()[-12:]), flush=True)
    if not core.completed(process.returncode, text, len(steps)):
        if ((process.returncode == 11 and "Error: Deadlock reached." in text)
                or (process.returncode == 12 and "Error: Invariant ReadBarrierInv is violated." in text)):
            raise core.ConformanceError(f"Read conformance rejected: {run}")
        raise RuntimeError(f"Incomplete or failed read TLC run ({process.returncode}): {run}")
    history = history_check.extract(events)
    (run / "history.json").write_text(json.dumps(history, indent=2) + "\n")
    linearizable, history_run = history_check.check(history, timeout)
    (run / "history-run.txt").write_text(str(history_run) + "\n")
    if not linearizable:
        raise core.ConformanceError(f"Register history is not linearizable: {history_run}")
    print(f"PASS {Path(path).name}: consensus, {len(events)} read/application observations, and register history", flush=True)
    return run


def negatives(path, timeout):
    events = load(path)
    directory = Path(tempfile.mkdtemp(prefix="read-mutations-", dir=check.CACHE))
    names = ("premature-ready", "forged-ack", "wrong-context", "wrong-index", "early-callback", "duplicate-callback", "apply-ahead", "heartbeat-commit", "wrong-register", "wrong-read-value", "early-write-callback")
    for name in names:
        changed = copy.deepcopy(events)
        if name == "premature-ready":
            e = next(e for e in changed if e["kind"] == "read")
            e["reads"][e["node"]-1]["ready"].append(dict(context=e["value"], index=0))
        elif name == "forged-ack":
            e = next(e for e in changed if e["kind"] == "read" and e["value"] == "batch-first")
            e["reads"][0]["pending"][0]["acks"].append(2)
        elif name == "wrong-context":
            e = next(e for e in changed if e["kind"] == "receive" and e["input"]["context"])
            e["input"]["context"] = "forged"
        elif name == "wrong-index":
            e = next(e for e in changed if e["kind"] == "read_ready")
            e["reads"][e["node"]-1]["tracked"][0]["index"] += 1
        elif name == "early-callback":
            e = next(e for e in changed if e["kind"] == "complete_reads")
            e["reads"][0]["completed"].append(dict(context="deferred", index=0, value=""))
        elif name == "duplicate-callback":
            e = changed[-1]
            e["reads"][0]["completed"].append(copy.deepcopy(e["reads"][0]["completed"][0]))
        elif name == "apply-ahead":
            e = next(e for e in changed if e["kind"] == "apply")
            e["value"] = str(e["states"][e["node"]-1]["commit"] + 1)
            e["reads"][e["node"]-1]["applied"] = int(e["value"])
        elif name == "heartbeat-commit":
            e = next(e for i, e in enumerate(changed) if e["kind"] == "receive"
                     and e["input"]["kind"] == "heartbeat" and e["input"]["commit"] > 0
                     and changed[i-1]["states"][e["node"]-1]["commit"] == 0)
            e["states"][e["node"]-1]["commit"] = 0
        elif name == "wrong-register":
            e = next(e for e in changed if e["kind"] == "apply" and e["reads"][e["node"]-1]["value"])
            e["reads"][e["node"]-1]["value"] = "corrupted"
        elif name == "wrong-read-value":
            e = next(e for e in changed if e["kind"] == "complete_reads" and e["reads"][e["node"]-1]["completed"])
            e["reads"][e["node"]-1]["completed"][0]["value"] = "corrupted"
        elif name == "early-write-callback":
            e = next(e for e in changed if e["kind"] == "propose")
            e["reads"][e["node"]-1]["writes"][-1]["completed"] = 1
        target = directory / f"{name}.ndjson"
        write_events(target, changed)
        try:
            validate(target, timeout)
        except core.ConformanceError:
            print(f"PASS read semantic negative: {name}", flush=True)
        else:
            raise RuntimeError(f"Read mutation unexpectedly accepted: {name}")
    history_check.negatives(history_check.extract(events), timeout)
    history_check.regressions(timeout)
    print(f"Read negative fixtures: {directory}; {len(names)} semantic mutations rejected", flush=True)



def identity_negatives(path, timeout):
    events = load(path)
    proposals = [e for e in events if e["kind"] == "propose"]
    pair = next(((a, b) for i, a in enumerate(proposals) for b in proposals[i+1:]
                 if a["value"] == b["value"] and a["proposal_context"] != b["proposal_context"]), None)
    core.require(pair is not None, "Identity regressions require equal values with distinct proposal contexts")
    first, second = pair
    first_id, second_id = first["proposal_context"], second["proposal_context"]
    directory = Path(tempfile.mkdtemp(prefix="identity-mutations-", dir=check.CACHE))
    names = ("proposal-id-substitution", "wrong-write-callback", "duplicate-write-callback", "log-context-substitution")
    for name in names:
        changed = copy.deepcopy(events)
        if name == "proposal-id-substitution":
            changed[second["seq"]]["proposal_context"] = first_id
        elif name == "wrong-write-callback":
            for e in changed:
                writes = {w["context"]: w for w in e["reads"][first["node"]-1]["writes"]}
                if e["kind"] == "apply" and first_id in writes and second_id in writes:
                    if writes[first_id]["completed"] == 1 and writes[second_id]["completed"] == 0:
                        writes[first_id]["completed"] = 0
                        writes[second_id]["completed"] = 1
                        break
            else:
                raise RuntimeError("Missing independent write completion boundary")
        elif name == "duplicate-write-callback":
            write = next(w for w in changed[-1]["reads"][first["node"]-1]["writes"] if w["context"] == first_id)
            write["completed"] = 2
        else:
            entry = next(e for e in changed[-1]["states"][first["node"]-1]["log"] if e["context"] == second_id)
            entry["context"] = first_id
        target = directory / f"{name}.ndjson"
        write_events(target, changed)
        try:
            validate(target, timeout)
        except core.ConformanceError:
            print(f"PASS identity semantic negative: {name}", flush=True)
        else:
            raise RuntimeError(f"Identity mutation unexpectedly accepted: {name}")
    print(f"Identity negative fixtures: {directory}; {len(names)} mutations rejected", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="+", type=Path)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--negatives", action="store_true")
    parser.add_argument("--identity-negatives", type=Path, help="Trace with equal-valued writes and independent completions")
    args = parser.parse_args()
    core.require(args.timeout > 0, "Timeout must be positive")
    check.verify_upstream()
    for path in args.paths:
        validate(path, args.timeout)
    if args.negatives:
        negatives(args.paths[0], args.timeout)
    if args.identity_negatives:
        if args.identity_negatives not in args.paths:
            validate(args.identity_negatives, args.timeout)
        identity_negatives(args.identity_negatives, args.timeout)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, TypeError, OSError, RuntimeError) as error:
        print(f"error: {error}", file=sys.stderr)
        sys.exit(1)
