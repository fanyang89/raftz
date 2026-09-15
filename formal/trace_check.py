#!/usr/bin/env python3
"""Validate bounded, complete observations of real RawNode executions with TLC."""

import argparse
from collections import Counter
import copy
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

import check

HEADER = dict(kind="header", version=1, voters=[1, 2, 3], pre_vote=False,
              check_quorum=False, transfer=False, read_index=False, snapshot=False,
              compaction=False, atomic_memory_storage=True, send_after_persist=True)
EVENTS = {"init", "campaign", "propose", "beat", "receive", "ready", "persist",
          "advance", "release", "restart", "drop", "duplicate", "end"}
MESSAGE_TYPES = {"request_vote": "RequestVoteRequest", "request_vote_response": "RequestVoteResponse",
                 "append": "AppendEntriesRequest", "append_response": "AppendEntriesResponse",
                 "heartbeat": "AppendEntriesRequest", "heartbeat_response": "AppendEntriesResponse"}
INVARIANTS = ["LogInv", "MoreThanOneLeaderInv", "ElectionSafetyInv", "LogMatchingInv",
              "QuorumLogInv", "MoreUpToDateCorrectInv", "LeaderCompletenessInv", "CommittedIsDurableInv"]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def fields(obj, names):
    require(isinstance(obj, dict) and set(obj) == set(names.split()), f"Unexpected fields: {obj!r}")


def natural(n, maximum=1_000_000):
    require(type(n) is int and 0 <= n <= maximum, f"Unsupported integer: {n!r}")


def payload(value):
    require(isinstance(value, str) and len(value.encode("utf-8")) <= 4096, "Unsupported payload")
    return value.encode("utf-8").hex()


def entry(e, index):
    fields(e, "index term data context")
    natural(e["index"])
    require(e["index"] == index, "Noncontiguous log indices")
    natural(e["term"], 16)
    return dict(term=e["term"], type="ValueEntry", value=dict(val=dict(
        kind="command" if e["data"] else "noop", data=payload(e["data"]), context=payload(e["context"]))))


def log(es, start=1):
    require(isinstance(es, list) and len(es) <= 32, "Unsupported log length")
    return [entry(e, i) for i, e in enumerate(es, start)]


def message(m):
    fields(m, "kind from to term index log_term commit commit_term reject reject_hint entries")
    require(m["kind"] in MESSAGE_TYPES, "Unsupported message")
    natural(m["from"])
    natural(m["to"])
    require(m["from"] in (1, 2, 3) and m["to"] in (1, 2, 3), "Unsupported peer")
    for key in ("term", "index", "log_term", "commit", "commit_term", "reject_hint"):
        natural(m[key])
    require(type(m["reject"]) is bool, "Invalid rejection flag")
    entries = log(m["entries"], m["index"] + 1)
    kind = m["kind"]
    out = dict(mtype=MESSAGE_TYPES[kind], msource=m["from"], mdest=m["to"], mterm=m["term"])
    if kind == "request_vote":
        out.update(mlastLogIndex=m["index"], mlastLogTerm=m["log_term"])
    elif kind == "request_vote_response":
        out.update(mvoteGranted=not m["reject"])
    elif kind in ("append", "heartbeat"):
        out.update(msubtype="app" if kind == "append" else "heartbeat", mprevLogIndex=m["index"],
                   mprevLogTerm=m["log_term"], mcommitIndex=m["commit"], mentries=entries)
    else:
        out.update(msubtype="app" if kind == "append_response" else "heartbeat",
                   msuccess=not m["reject"], mmatchIndex=m["index"] if kind == "append_response" and not m["reject"] else 0)
    require(kind == "append" or not entries, "Entries on unsupported message")
    return out


def observation(e):
    fields(e, "kind seq node input value states pending network")
    require(e["kind"] in EVENTS, "Unknown event")
    require(type(e["node"]) is int and e["node"] in ((0,) if e["kind"] in ("init", "end") else (1, 2, 3)), "Invalid event node")
    require(isinstance(e["states"], list) and len(e["states"]) == 3, "Expected three states")
    states = []
    for s in e["states"]:
        fields(s, "term vote commit role log durable_term durable_vote durable_commit durable_log")
        for key in ("term", "vote", "commit", "durable_term", "durable_vote", "durable_commit"):
            natural(s[key])
        require(s["role"] in ("follower", "candidate", "leader"), "Unsupported role")
        states.append(dict(s, role=s["role"].capitalize(), log=log(s["log"]), durable_log=log(s["durable_log"])))
    has_input = e["kind"] in ("receive", "drop", "duplicate")
    require((e["input"] is not None) == has_input, "Missing or unexpected input")
    if has_input:
        message(e["input"])
        require(e["input"]["to"] == e["node"], "Input destination mismatch")
    payload(e["value"])
    require(bool(e["value"]) == (e["kind"] == "propose"), "Unexpected proposal value")
    for key in ("pending", "network"):
        require(isinstance(e[key], list) and len(e[key]) <= 128, "Unsupported message bag")
    return dict(states=states, pending=[message(m) for m in e["pending"]], network=[message(m) for m in e["network"]])


def unique_object(pairs):
    result = {}
    for k, v in pairs:
        require(k not in result, f"Duplicate JSON key: {k}")
        result[k] = v
    return result


def load(path):
    data = Path(path).read_bytes()
    require(0 < len(data) <= 16_000_000 and data.endswith(b"\n"), "Empty, oversized or truncated trace")
    rows = [json.loads(line, object_pairs_hook=unique_object) for line in data.splitlines()]
    require(4 <= len(rows) <= 1002, "Unsupported event count")
    require(key(rows[0]) == key(HEADER), "Unsupported or missing trace header")
    fields(rows[-1], "kind events")
    require(key(rows[-1]) == key(dict(kind="terminal", events=len(rows)-2)), "Missing or inconsistent terminal marker")
    events = rows[1:-1]
    require(events[0]["kind"] == "init" and events[-1]["kind"] == "end", "Missing init/end observation")
    for i, e in enumerate(events):
        require(type(e.get("seq")) is int and e["seq"] == i, "Missing or reordered sequence number")
        require((e["kind"] == "init") == (i == 0), "Unexpected init")
        require((e["kind"] == "end") == (i == len(events)-1), "Unexpected end")
        observation(e)
    require(any(e["kind"] == "campaign" for e in events) and any(e["kind"] == "propose" for e in events), "Vacuous trace")
    return events


def key(m):
    return json.dumps(m, sort_keys=True)


def instructions(events):
    steps = []

    def add(op, event, **kw):
        steps.append(dict(op=op, node=event["node"], event=event["seq"], bound=36, **kw))

    previous = None
    for e in events:
        expected = observation(e)
        kind, node = e["kind"], e["node"]
        before = previous["states"][node-1] if previous and node else None
        after = e["states"][node-1] if node else None
        if kind == "campaign":
            add("timeout", e)
            add("selfVote", e)
        elif kind == "propose":
            add("client", e, value=dict(kind="command", data=payload(e["value"]), context=""))
        elif kind == "receive":
            add("receive", e, msg=message(e["input"]))
            if before["role"] != "leader" and after["role"] == "leader":
                add("leader", e)
                add("client", e, value=dict(kind="noop", data="", context=""))
            if after["role"] == "leader":
                add("commit", e)
        elif kind == "advance":
            if before["role"] == "leader":
                add("selfAck", e)
                add("commit", e)
        elif kind in ("persist", "release", "restart"):
            add(kind, e)
        elif kind in ("drop", "duplicate"):
            add(kind, e, msg=message(e["input"]))
        if kind in ("campaign", "propose", "receive", "advance", "beat"):
            old = Counter(key(message(m)) for m in previous["pending"])
            for m in expected["pending"]:
                k = key(m)
                if old[k]:
                    old[k] -= 1
                elif m["mtype"] in ("RequestVoteRequest", "AppendEntriesRequest"):
                    require(m["msource"] == node, "Send from wrong node")
                    if kind == "campaign":
                        require(m["mtype"] == "RequestVoteRequest", "Unsupported campaign send")
                    else:
                        require(m["mtype"] == "AppendEntriesRequest", "Unsupported replication send")
                    if kind == "beat":
                        require(m["msubtype"] == "heartbeat", "Unsupported heartbeat send")
                    add("send", e, msg=m)
        add("check", e, expected=expected)
        previous = e
    return steps


def config():
    constants = dict(Nil=0, ValueEntry="ValueEntry", ConfigEntry="ConfigEntry", Follower="Follower",
                     Candidate="Candidate", Leader="Leader", RequestVoteRequest="RequestVoteRequest",
                     RequestVoteResponse="RequestVoteResponse", AppendEntriesRequest="AppendEntriesRequest",
                     AppendEntriesResponse="AppendEntriesResponse")
    return "SPECIFICATION TraceSpec\nCONSTANTS\n    Server = {1,2,3}\n    InitServer = {1,2,3}\n" + "".join(
        f"    {k} = {json.dumps(v)}\n" for k, v in constants.items()) + "INVARIANTS\n    " + "\n    ".join(INVARIANTS) + "\nCHECK_DEADLOCK TRUE\n"


def validate(path, timeout=60):
    events = load(path)
    steps = instructions(events)
    cp = check.classpath()
    runs = check.CACHE / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    run = Path(tempfile.mkdtemp(prefix=f"trace-{Path(path).stem}-", dir=runs))
    shutil.copy2(path, run / "input.ndjson")
    shutil.copy2(check.UPSTREAM / "tla" / "etcdraft.tla", run / "etcdraft.tla")
    shutil.copy2(check.ROOT / "models" / "RaftzTrace.tla", run / "RaftzTrace.tla")
    (run / "steps.ndjson").write_text("".join(json.dumps(s) + "\n" for s in steps))
    (run / "RaftzTrace.cfg").write_text(config())
    command = ["java", "-Xmx2g", "-XX:+UseParallelGC", "-cp", cp, "tlc2.TLC", "-workers", "1",
               "-metadir", str(run / "states"), "-config", "RaftzTrace.cfg", "RaftzTrace.tla"]
    (run / "command.json").write_text(json.dumps(command, indent=2) + "\n")
    print(f"Trace artifacts: {run}", flush=True)
    with (run / "tlc.log").open("w") as output:
        try:
            result = subprocess.run(command, cwd=run, stdout=output, stderr=subprocess.STDOUT, timeout=timeout)
        except subprocess.TimeoutExpired as error:
            raise RuntimeError(f"TLC timeout, NOT a pass: {run}") from error
    text = (run / "tlc.log").read_text()
    passed = completed(result.returncode, text, len(steps))
    print("\n".join(text.splitlines()[-12:]), flush=True)
    if not passed:
        # Semantic negatives must fail through the model, not parser/tool failures.
        if result.returncode != 0 and ("Error: Deadlock reached." in text or "is violated" in text):
            raise ConformanceError(f"Conformance rejected: {run}")
        raise RuntimeError(f"Incomplete or failed TLC run ({result.returncode}): {run}")
    print(f"PASS {Path(path).name}: {len(events)} observations, {len(steps)} instructions", flush=True)
    return run


def completed(returncode, text, count):
    queues = re.findall(
        r"^[0-9]+ states generated, [0-9]+ distinct states found, ([0-9]+) states left on queue\.$",
        text, re.MULTILINE)
    return (returncode == 0 and f'<<"RAFTZ_TRACE_COMPLETE", {count}>>' in text
            and "Model checking completed. No error has been found." in text
            and bool(queues) and int(queues[-1]) == 0)


class ConformanceError(RuntimeError):
    pass


def write_events(path, events):
    events = copy.deepcopy(events)
    for i, e in enumerate(events):
        e["seq"] = i
    rows = [HEADER, *events, dict(kind="terminal", events=len(events))]
    path.write_text("".join(json.dumps(e) + "\n" for e in rows))


def negatives(path, timeout):
    events = load(path)
    directory = Path(tempfile.mkdtemp(prefix="mutations-", dir=check.CACHE))
    cases = {}
    for name in ("term", "vote", "committed-payload", "committed-identity", "durable-payload", "release-before-persist", "ack-before-persist", "missing-operation", "message-causality"):
        changed = copy.deepcopy(events)
        if name in ("term", "vote"):
            e = next(e for e in changed if e["kind"] == "campaign")
            e["states"][e["node"]-1][name] = 9 if name == "term" else 2
        elif name in ("committed-payload", "committed-identity", "durable-payload"):
            e = changed[-1]
            log_name = "durable_log" if name == "durable-payload" else "log"
            es = e["states"][0][log_name]
            es[1]["data"] = "corrupted" if name != "committed-identity" else es[2]["data"]
        elif name in ("release-before-persist", "ack-before-persist"):
            start = next(i for i, e in enumerate(changed) if e["kind"] == "persist"
                         and (name.startswith("release") or
                              (e["states"][e["node"]-1]["role"] == "leader"
                               and len(e["states"][e["node"]-1]["durable_log"]) >
                               len(changed[i-1]["states"][e["node"]-1]["durable_log"]))))
            wanted = "release" if name.startswith("release") else "advance"
            end = next(i for i in range(start+1, len(changed)) if changed[i]["kind"] == wanted)
            changed.insert(start, changed.pop(end))
        elif name == "message-causality":
            e = next(e for e in changed if e["kind"] == "receive")
            e["input"]["from"] = 3
        else:
            del changed[next(i for i, e in enumerate(changed) if e["kind"] == "propose")]
        cases[name] = changed
    for name, changed in cases.items():
        target = directory / f"{name}.ndjson"
        write_events(target, changed)
        try:
            validate(target, timeout)
        except ConformanceError:
            print(f"PASS semantic negative: {name}", flush=True)
        else:
            raise RuntimeError(f"Mutation unexpectedly accepted: {name}")
    unsupported = copy.deepcopy(events)
    next(e for e in unsupported if e["kind"] == "campaign")["kind"] = "transfer"
    unsupported_path = directory / "unsupported.ndjson"
    write_events(unsupported_path, unsupported)
    for name, data in {
        "empty": b"", "truncated-line": Path(path).read_bytes()[:-5],
        "missing-terminal": b"\n".join(Path(path).read_bytes().splitlines()[:-1]) + b"\n",
        "unsupported": unsupported_path.read_bytes(),
        "suffix": Path(path).read_bytes() + b'{}\n',
    }.items():
        target = directory / f"{name}.ndjson"
        target.write_bytes(data)
        try:
            load(target)
        except (ValueError, KeyError):
            print(f"PASS structural negative: {name}", flush=True)
        else:
            raise RuntimeError(f"Malformed trace unexpectedly accepted: {name}")
    print(f"Negative fixtures: {directory}; {len(cases)} semantic + 5 structural rejected", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="+", type=Path)
    parser.add_argument("--negatives", action="store_true")
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()
    require(args.timeout > 0, "Timeout must be positive")
    check.verify_upstream()
    for path in args.paths:
        validate(path, args.timeout)
    if args.negatives:
        negatives(args.paths[0], args.timeout)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, TypeError, OSError, RuntimeError) as error:
        print(f"error: {error}", file=sys.stderr)
        sys.exit(1)
