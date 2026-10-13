# Safe ReadIndex design check

## Inspection before modeling

The existing `RaftzMC` wrapper checks the pinned etcd consensus baseline, and
`RaftzTrace` checks two real fixed-membership RawNode executions. Neither contains
ReadIndex state or checks client reads. They remain separate from this model. This model
is a separate, **conditional design check**, not an extension of the existing
Zig trace adapter and not a proof that the Zig implementation refines this model.

Inspection of the current implementation identified these boundaries:

| Implementation | Model correspondence |
| --- | --- |
| `Raft.stepLeader(.read_index)` postpones requests until `commitToCurrentTerm()`; `releasePendingReadIndexMessages` replays them after commit | `Invoke`, `Register`, `currentTermCommitted` |
| `Raft.serveReadIndex` captures `raft_log.committed`; singleton bypasses the network | `Register` captures `knownCommit`; the singleton's self ACK is already a quorum |
| `ReadOnly.addRequest` queues unique contexts and inserts the leader's ACK | Sequential read IDs, FIFO registration, per-request ACK sets |
| `Raft.step` rejects lower-term responses and steps down on a higher term; `reset` discards unconfirmed read queues | Per-node `term`, `leaders`, and `Active`; obsolete requests cannot register, accept replies, or release |
| `handleHeartbeat` echoes the request context; `handleHeartbeatResponse` collects ACKs and checks `hasQuorum` | `HeartbeatReply`, delayed `DeliverReply`, `Release` |
| `ReadOnly.advance(ctx)` releases the queue prefix, not only the acknowledged request | `Release` also marks earlier requests for the same leader/term ready |
| `ReadyProcessor.completeReads` marks read indices ready; `ProposalTracker.completeReadyReads` waits for `applied_index >= ready_index` | Independent `Apply` and `Complete` actions |

Source files: `src/raft.zig`, `src/read_only.zig`, `src/ready_processor.zig`, and
`src/proposal_tracker.zig`. No production code is changed by this check. Source
inspection found the three modeled guards present; the negative configurations
are hypothetical guard removals, **not reports of existing Zig defects**.

## Model boundary and assumptions

`models/RaftzReadIndex.tla` models one or three fixed voters, at most two successful
election terms, two committed log positions, and two distinct read requests.
These are action bounds, with no TLC state constraints, symmetry, or VIEW
reduction. TLC exhaustively explores this finite abstraction, not the full Raft
state machine.

Consensus is assumed to supply a single immutable committed prefix. `committed`
is its length; each `Commit` abstracts committing a current-term no-op or command
and learning the resulting commit index on the leader. Elections and commits
require intersecting majorities, terms never decrease, and a leader may retain a
stale **known commit index**, even though leader completeness guarantees its log
contains the committed prefix. The model does not implement or prove log voting,
replication, persistence, leader completeness, or payload identity. Commit actions
have an atomic quorum boundary; tentative writes and partially completed elections
are omitted. A heartbeat does not advance `knownCommit`: ignoring that optional
knowledge propagation conservatively retains stale knowledge for this check.

Elections and commits update the promises of their quorums, leaving an isolated
old leader unaware. It can receive new requests but cannot collect a new majority
of lower-term heartbeat replies. `HeartbeatReply` and `DeliverReply` are separate:
a reply created before a later election can arrive afterward. There is no
wall-clock or lease assumption. Not delivering a reply represents arbitrary delay
or loss; duplicate replies are idempotent set insertions and are abstracted away.
This is a safety abstraction, not a proof of transport behavior.

Requests have globally fresh identities, and registration preserves per-leader
FIFO order. Reusing contexts, follower-forwarded reads, singleton-to-multi-node
changes, joint consensus, leases, PreVote, crashes, OOM, WAL, and concurrency in
the application are outside this slice. Higher-term invalidation leaves orphaned
records as diagnostic history rather than literally freeing Zig queues. Already
confirmed reads may complete after a role change: confirmation is a barrier, not
a promise that the node is still leader at callback time. The model does not
require the implementation to complete requests that it cancels.

`floor` records the global committed prefix at invocation. It is an observer-only
field: **no action uses it to choose a safe result or enable progress**. `returned`
records the applied prefix at callback completion. The checked properties are:

- `TypeOK`: state and request fields stay inside the finite domains.
- `CommitBounds`: applied <= locally known commit <= globally committed.
- `ReadIndexSafety`: every confirmed read index covers the invocation's prefix.
- `AppliedBarrier`: completion has applied at least the confirmed read index.
- `NoStaleRead`: completion has applied all entries committed before invocation.

This establishes bounded **prefix freshness and apply-barrier safety under the
consensus assumptions**, not client-observable linearizability for an arbitrary
state machine. There are no command values, application read operations, or full
client histories here. A later integration must connect the barrier to actual
state-machine results and their invocation/response intervals. No liveness or
fairness is asserted; deadlock checking is disabled because partitions, canceled
requests and exhausted action bounds may legitimately stop progress.

## Run and failure discipline

```bash
mise run prepare-tla
mise run test-tla-read-index

# Equivalent:
PYTHONDONTWRITEBYTECODE=1 python3 formal/read_index_check.py --timeout 120
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s formal -p 'test_*.py'
```

The opt-in task uses the existing checksum-verified jars and a 2 GiB heap, one
worker, and a 120-second timeout per profile. It does not add Java to default Zig
tests or CI. Each run retains model/config copies, the exact command, TLC log,
states and generated counterexamples under `.cache/tla/runs/read-index-*/`.

The two positive profiles require exit zero, TLC's completed-search marker, a
nontrivial state graph and a final empty queue. Three negative profiles each
remove exactly one guard. They require TLC's invariant-violation exit code, the
**exact expected invariant**, and a concrete multi-state counterexample. Syntax
errors, wrong invariants, timeouts, incomplete searches, and unexpectedly passing
mutants fail the task. Two additional anti-invariant checks require witnesses for
successful completion and releasing an earlier request without its own quorum;
this prevents interpreting an unreachable read path as useful safety coverage.

## Results

Checked with the pinned TLC v1.8.0 asset (SHA-256 `c0d2f72a…`) and OpenJDK 25:

| Profile | Result |
| --- | --- |
| One voter, all guards | 3,248 generated / 1,801 distinct states; depth 15; complete |
| Three voters, all guards | 2,059,996 generated / 834,433 distinct states; depth 24; complete |
| Remove current-term commit gate | `ReadIndexSafety` counterexample: new leader confirms stale commit knowledge |
| Remove quorum confirmation | `ReadIndexSafety` counterexample: isolated old leader confirms after a newer commit |
| Remove apply gate | `AppliedBarrier` counterexample: callback before application catches up |
| Completion witness | `NoCompletedRead` counterexample with a completed read and all safety invariants intact |
| Batch witness | `NoBatchedRelease` counterexample: a later quorum releases an earlier read lacking its own quorum |

The existing upstream baseline and real Zig trace gate were also rerun after the
tool pin refresh: the one-node baseline retains 4,650 distinct states; three-node
simulation retains 100 sampled traces / 10,769 states; both real traces pass, and
all nine semantic plus five structural trace mutations are rejected. The combined
Python runner/adapter suite now passes 95 tests. These checks do not turn this standalone
ReadIndex design model into implementation trace validation.

A separate [real read trace slice](read-trace.md) now checks actual RawNode
contexts/queues/read states, applied cursors and ProposalTracker callbacks using
mandatory consensus and read/application passes, followed by a
[finite register-history check](register-history.md). It preserves the original
consensus profile's fail-closed contract. The standalone design model still
abstracts away values; the new trace gate checks them for its recorded executions.
