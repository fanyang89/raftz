# Raft TLA+ baseline

## Status and assurance boundary

This directory imports a pinned, unmodified etcd Raft specification and provides
reproducible baseline checks plus a [real RawNode trace conformance slice](trace.md).
Two deterministic three-voter Zig executions now pass complete TLC trace checks,
including elections, commits, leader change/conflict repair and atomic restart.
This does **not** prove correctness for all raftz executions. No TLAPS proof is
included. A `THEOREM` declaration in an upstream module is not a checked proof.

See [implementation alignment](../docs/formal-verification.md) for the source
selection, abstraction gaps, and staged acceptance criteria.

## Provenance

- Repository: <https://github.com/etcd-io/raft>
- Commit: `3cbf6a74be3fa392edd8b64253fcd11c3ce5649b`
- Source: <https://github.com/etcd-io/raft/tree/3cbf6a74be3fa392edd8b64253fcd11c3ce5649b/tla>
- Import: `upstream/etcd/`; `manifest.json` records each imported file's SHA-256.
- Changes to imported files: none. Local constraints live in `models/`.
- The upstream root `LICENSE` is Apache-2.0. The specification also retains
  CC-BY-4.0 attribution to Diego Ongaro, Brandon Amos, Huanchen Zhang, Daniel
  Ricketts, George Pîrlea, and Darius Foo. Preserve both notices and the embedded
  <https://creativecommons.org/licenses/by/4.0/> link when redistributing.
- The two local `.cfg` files are modified derivatives of upstream
  `MCetcdraft.cfg`; their modification notices identify the local restrictions.

The upstream trace modules are preserved unchanged as a reference. The local
`RaftzTrace` adapter reuses etcdraft actions with explicit self-vote, self-match
and persistence-boundary deltas; it does not use Traceetcdraft's length-only
checks. See [the action mapping and coverage limits](trace.md). The large example
trace and upstream scripts downloading floating dependencies are not imported.

## Run

Requirements: Python 3.10+, Java 17+, and network access for the preparation step.
No Python packages or global Java tools are installed.

```bash
mise run prepare-tla
mise run test-tla
mise run test-tla-trace  # Real Zig executions and conformance, distinct from test-tla

# Equivalent commands without mise:
python3 formal/check.py prepare
python3 formal/check.py smoke

# Optional three-node exhaustive exploration; may be expensive:
python3 formal/check.py model --timeout 3600

# Offline provenance and runner regression checks:
python3 formal/check.py verify
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s formal -p 'test_*.py'
```

`prepare` downloads about 10 MB from the official upstream release endpoints.
`tools.lock.json` pins URLs and SHA-256 digests published by the upstream GitHub
release API. Cached tools are rechecked before execution. There are no automatic
upgrades or floating `latest` downloads. Update the lock only after revalidating
the profiles. A digest mismatch fails rather than trusting a replacement asset.

Tools and per-run artifacts live under `.cache/tla/`. Each run preserves the
module/config copies, command line, TLC log, and state directory. The runner uses
one worker and a 2 GiB Java heap. A timeout (default 120 seconds per profile),
missing dependency, checksum mismatch, or TLC error is a failure, never a pass.

## Profiles

`test-tla` first exhaustively checks `SingleNode.cfg`, then samples
`ThreeNode.cfg` with TLC simulation: 100 traces, maximum depth 100, seed 20260912.
Simulation is **not exhaustive verification**. The single-node profile is a
harness sanity check, not evidence of multi-node consensus safety.

Both configurations use:

- static membership, either one or three initially bootstrapped voters;
- maximum term 3, one value entry per node's log, no reconfigurations;
- at most three pending plus in-flight messages, counting multiplicities;
- upstream `MCTimeout` and `MCSend` restrictions, including fewer than one
  existing candidate before starting another election and restricted concurrent
  AppendEntries traffic;
- crashes/restarts, message loss/duplication, and the upstream atomic `Ready`;
- no symmetry reduction and no liveness/deadlock claim.

The message budget is a TLC state constraint, not a production transport bound.
It prunes exploration beyond that budget. Maximum term and request limits alone
do **not** bound the upstream message bags: repeated vote/self-ack messages can
accumulate. An initial single-node run without a message budget timed out after
120 seconds; this was not a safety failure and is not counted as a successful
check. State constraints and action restrictions deliberately omit behaviors.

All eight upstream model-checking invariants remain enabled:
`LogInv`, `MoreThanOneLeaderInv`, `ElectionSafetyInv`, `LogMatchingInv`,
`QuorumLogInv`, `MoreUpToDateCorrectInv`, `LeaderCompletenessInv`, and
`CommittedIsDurableInv`. These names must not be interpreted more broadly than
their definitions. In particular, `MoreThanOneLeaderInv` compares current leaders,
not the full history of leaders within a term.

Initial validation with the pinned tools and OpenJDK 25:

| Check | Result |
| --- | --- |
| Upstream file integrity | All eight files match |
| One-node bounded exhaustive check | 34,573 generated / 4,650 distinct states, depth 24, no errors |
| Three-node simulation | 100 traces / 10,769 states, no errors |
| Three-node exhaustive check | Not completed as part of baseline validation |
| Zig trace conformance | Two real executions pass; 9 semantic and 5 structural negatives rejected; see [trace results](trace.md) |
| TLAPS proof | Not implemented |

These are baseline model results only. Neither bounded exploration nor sampled
trace validation would establish correctness for all cluster sizes, terms,
faults, configurations, or Zig executions.
