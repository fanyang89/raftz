# Formal verification and implementation alignment

## Decision

Use the [official etcd Raft TLA+ development](https://github.com/etcd-io/raft/tree/3cbf6a74be3fa392edd8b64253fcd11c3ce5649b/tla)
as raftz's initial formal specification baseline. Keep the imported source
unchanged and pin its commit and checksums. Introduce raftz-specific wrappers and
trace adapters separately, with explicit differences and regression evidence.
This is an engineering-fit decision, not a claim that a universally best or
fully proved Raft TLA+ model exists.

| Candidate | Strength | Fit for raftz |
| --- | --- | --- |
| [Ongaro's canonical specification](https://github.com/ongardie/raft.tla) | Original-author specification and dissertation safety argument; ancestor of the selected model | Important semantic reference, but requires more work for implementation-level Ready, membership, and trace checking |
| [etcd Raft `tla/`](https://github.com/etcd-io/raft/tree/3cbf6a74be3fa392edd8b64253fcd11c3ce5649b/tla) | Maintained in the implementation repository; model-checking configurations, eight safety invariants, Ready/durable state and NDJSON trace validation | Selected: closest to raftz's RawNode/Ready/Advance architecture and etcd-oriented behaviors |
| [Microsoft CCF](https://ccf.dev/main/architecture/raft_tla.html) | Detailed model and operational implementation-trace validation workflow | Useful workflow reference; its signed-entry commit semantics are CCF-specific and should not become raftz semantics |
| [Logless dynamic reconfiguration](https://github.com/will62794/logless-reconfig) | Machine-checked TLAPS proofs for MongoDB's Raft-based reconfiguration protocol | Useful proof-method reference, not a drop-in proof of raftz's protocol |
| [Verdi Raft](https://github.com/uwplse/verdi-raft) | Machine-checked Coq development | Valuable proof reference, but not a TLA+ baseline and not a proof of this Zig implementation |

The etcd README documents an existing trace-validation mechanism, not merely a
roadmap. It also explicitly acknowledges the assumption of atomic persistence
and the gap for a crash that saves only a log prefix. Importing its model does
not close that gap. Its TLA+ theorem declarations do not contain TLAPS proofs.

## Initial mapping inventory

This inventory identifies candidate correspondence points, not verified
refinements. Code locations refer to the baseline raftz commit `63436df`.

| Concept | Upstream model | raftz implementation | Alignment work still required |
| --- | --- | --- | --- |
| Role, term, vote | `state`, `currentTerm`, `votedFor` | `src/raft.zig`: `becomeFollower`, `campaign`, `becomeLeader`, `step` | Define trace points before/after transitions, higher-term handling, and durable vote boundary |
| Election messages | `Timeout`, `RequestVote`, `BecomeLeader`, `Receive` | `src/raft.zig`: `campaign`, `stepCandidate` | Map synchronous self-voting and leader no-op insertion; a Zig call may contain multiple model actions |
| Log replication | `ClientRequest`, `AppendEntries`, `Receive` | `src/raft.zig`: `stepLeader`, `handleAppendEntries`; `src/raft_log.zig` | Map entry identities, indices, batches, conflict handling, and empty/no-op entries without conflating client values |
| Heartbeats | `Heartbeat`, AppendEntries subtype `heartbeat` | `src/raft.zig`: `handleHeartbeat`, `handleHeartbeatResponse` | Check commit propagation and response/progress semantics independently from ordinary appends |
| Commit and durability | `AdvanceCommitIndex`, `Ready`, `durableState` | `src/raw_node.zig`: `getReady`, `advanceAppend`, `advance`; `src/ready_processor.zig`; `src/raftor.zig`: `processReadyStep` | Map early versus persistence-dependent messages and async Ready acknowledgment; do not collapse crash-visible stages without justification |
| Restart | `Restart` restores the recorded durable prefix/state | `src/wal.zig`, Raftor recovery | Initially validate only the stated atomic storage abstraction; partial writes, fsync and WAL recovery need a separate model/adapter |
| Membership | `AddNewServer`, `AddLearner`, `DeleteServer`, `ApplySimpleConfChange` | `src/conf_changer.zig`, `src/tracker_conf.zig`, `Raft.applyConfChange` | Start with simple changes. A `jointConfig` field alone is not full joint-consensus coverage: the imported transition relation only applies simple changes |
| Snapshots | `SendSnapshot` represents a log prefix as an append subtype | `Raft.handleSnapshot`, RaftLog compaction, WAL snapshots | Preserve an abstract full-log history; separately account for compacted indices, snapshot ConfState, installation, and durability |
| PreVote / check-quorum / transfer | No complete corresponding actions in the imported model | `src/raft.zig`, `src/raft_config.zig` | Disable in initial trace scenarios, then add explicit semantics and regression scenarios |
| Linearizable reads | No ReadIndex/read-only state or client read history | `src/read_only.zig`, Raft ReadIndex paths | Model current-term commit gating, quorum confirmation, apply index and returned values before claiming linearizability |
| Multi-Raft, RPC, storage implementation | Abstract servers and message bags only | `src/multi_raft.zig`, `src/rpc/`, `src/wal.zig` | Out of the baseline verification claim; do not infer isolation, transport or filesystem correctness |

## Alignment rules

1. Start with fixed-membership consensus and atomic storage. State all disabled
   features explicitly in fixtures rather than silently dropping their events.
2. Treat a model mismatch as a discrepancy to investigate. Decide whether the
   code is wrong, the abstraction is wrong, or the model lacks a legitimate
   behavior; never weaken an invariant just to make a trace pass.
3. Preserve upstream source. Every local protocol extension needs a documented
   semantic delta, bounded model checking, and positive/negative trace fixtures.
4. Distinguish a no-op from a client command and retain command identity when
   comparing committed prefixes. Entry terms alone cannot establish state-machine
   safety for application values.
5. Match the actual persistence and send order. A trace at only `Raft.step` return
   can miss an invalid intermediate transition or a send-before-durability bug.
6. Record globally ordered events from a deterministic, single-process cluster
   harness, not sampled wall-clock logs merged from independent processes.
7. Unknown or missing event kinds, skipped trace suffixes, malformed traces and
   incomplete TLC searches must fail closed. Accepting an empty trace is not a
   valid conformance test.

## Stages and acceptance criteria

### 1. Reproducible upstream baseline — implemented

- Pin and retain the model, upstream provenance and licensing.
- Provide checksum-verified tools, bounded configurations, and a failing-on-error
  runner. Keep model checks distinct from random simulation.
- Retain all eight upstream invariants and document every local reduction.
- See [`formal/README.md`](../formal/README.md) for commands and actual results.

### 2. Fixed-membership Zig trace validation — planned

Use `tests/simulation_test.zig` and, after assessing its abstraction,
`tests/vopr/raft_adapter.zig` as candidate scenario drivers. Drive real `RawNode`
instances through elections, proposals, conflicts, partitions, duplicate/lost
messages, leader changes and atomic-storage restarts.

Define an explicit abstraction map from Zig state and messages to TLA+ state.
Instrument actual internal boundaries where a public call performs multiple
steps, or justify composition/stuttering in the adapter. Preserve the existing
production logging contract: trace output belongs to an opt-in test hook/sink,
not direct stdout/stderr writes in production code.

Acceptance requires complete traces that reach a validator end marker, plus
negative fixtures that corrupt term/vote, committed entries, and durability
ordering and are rejected. Add a CI task only after this gate exists and is
reproducible. The present `test-tla` task is **not** that gate.

### 3. Feature and storage extensions — planned

Add and verify separately: PreVote/check-quorum/leader transfer; full joint
consensus including entering/leaving and learners; snapshot installation and
compaction; ReadIndex and client-observable linearizability; partial WAL
persistence and recovery. Maintain a coverage table per feature and fault model.
Do not advertise a capability as proved because its data field exists upstream.

### 4. Stronger proof claims — not implemented

TLC checks the states explored under a finite model/configuration; random
simulation checks only sampled paths; trace validation checks only recorded
implementation executions. None alone proves that every Zig execution refines
Raft for arbitrary terms, logs and cluster sizes.

An unbounded safety claim needs a machine-checked inductive invariant/refinement
argument (for example TLAPS), including all assumptions and modeled extensions.
An implementation-correctness claim additionally needs a justified connection
from the actual Zig semantics and runtime/storage assumptions to that model.
Liveness needs separate fairness and environmental assumptions.
