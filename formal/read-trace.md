# Real Safe ReadIndex trace slice

## Run and scope

```bash
mise run prepare-tla
mise run test-tla-read-trace

# Validate already-generated executions:
PYTHONDONTWRITEBYTECODE=1 python3 formal/read_trace_check.py \
  .cache/tla/read-batch.ndjson .cache/tla/read-partition.ndjson .cache/tla/read-repeated.ndjson .cache/tla/read-stale.ndjson \
  --negatives --identity-negatives .cache/tla/read-repeated.ndjson \
  --stale-negatives .cache/tla/read-stale.ndjson
```

This checks four recorded executions of real `RawNode`, `ReadOnly`, `MemoryStorage`,
`ProposalTracker`, and a test register `StateMachine`. It extends the
[consensus trace driver](trace.md), not production code. It is distinct from the exhaustive, conditional
[ReadIndex design model](read-index.md): the trace adapter checks concrete
operation boundaries, not all schedules or a refinement theorem between the two
TLA+ models.

The fixed three-voter profile retains atomic MemoryStorage persistence, messages
released only after persistence, disabled PreVote/check-quorum, no membership
changes, snapshots, compaction, restart or RPC. Reads are local to a leader,
Safe-only, and have globally fresh nonempty UTF-8 contexts. `read_mode` explicitly
selects version 4 of the trace schema with `application = "register-v1"`;
the existing version-1 consensus profile still rejects contexts and read fields.

Unlike the original driver, read scenarios call `advanceAppend` without claiming
application progress. Separate `apply` operations call `advanceApplyTo`. The
driver passes actual Ready read states into the real tracker's `markReadReady`,
then invokes `completeReadyReads` separately. Actual callbacks record their
count, the RawNode applied index and the application value at invocation. No fake
callback is generated from an expected model result. The driver asserts the expected blocked/completed
requests as well as final commit indices.

This is **not** the production Raftor/ReadyProcessor loop. The driver now executes
register commands through a StateMachine vtable, records real write/read
callbacks and checks the resulting [finite register histories](register-history.md).
This is not a claim about all application executions. Timeouts and cancellation
are absent. On leadership loss, Raft drops unconfirmed read queues; this driver intentionally leaves the tracker's request pending. The
isolated read remains incomplete at the end, rather than pretending to verify
Raftor's cancellation behavior.

## Three mandatory checks of the same complete execution

`read_trace_check.py` validates every version-4 field and retains all observations.
All three checks below must pass; there is no option to skip consensus or history.

1. **Consensus projection.** Remove only the additional read snapshots and
   heartbeat contexts. Map `read` to a heartbeat-generating operation and
   `read_ready`/`apply`/`complete_reads` to consensus stutters. Keep every event's
   sequence number, node, full term/vote/role/log/durable state and full consensus
   message bags. Use the contextual consensus profile (version 2,
   `proposal_contexts = true`) so each proposal's recorded input context is bound
   to `ClientRequest`, rather than inferred from the observed resulting entries. Run the existing `RaftzTrace` adapter with all eight invariants.
   It must justify each heartbeat, commit and role change through model actions;
   the read adapter cannot invent consensus transitions.
2. **Read protocol.** `RaftzReadTrace` consumes all the original observations.
   Consensus-checked before/after states supply the environment's terms, roles,
   commits and committed-entry terms. The model derives read state from the
   named operations and inputs, then compares it with the real snapshots.
   Observed read indices, ACKs, callbacks and queue contents are never assigned
   into model state to make a mismatch pass. The model also derives the register
   value from the checked applied log prefix and verifies real write callbacks.
3. **Register history.** Extract real invocation/response intervals, keeping
   pending operations. Require a TLA+ linearization witness for completed
   operations and any needed pending writes; see [history semantics](register-history.md).

The read model compares, for all three nodes after every operation:

- FIFO postponed contexts and registered `(context, index, ACK set)` queues;
- produced read states, including Ready-owned states before tracker handoff;
- tracker requests and their optional ready indices;
- the actual RawNode applied cursor;
- callback `(context, applied index, value)` bags, preserving multiplicity;
- register values and proposal callback counts;
- pending and in-flight contextual heartbeat/request-response bags, preserving
  source, destination, term, context and multiplicity.

The consensus projection validates the other packet fields. The read model
checks context echoing and context-specific causality separately; it is not a
proof of packet allocation identity or byte-for-byte transport serialization.
Callback order is deliberately a bag, because tracker hash iteration order is
not part of the contract. Registration/queue order remains a sequence.

| Concrete boundary | Derived read-model action |
| --- | --- |
| `trackRead` + `RawNode.readIndex(ctx)` | Create a fresh tracked request; postpone unless the current-term commit gate is open; otherwise register at the checked commit index and emit contextual heartbeats |
| Commit becomes eligible in `receive` or `advance` | Replay postponed requests, preserving order, each at the new checked commit index |
| Receive contextual heartbeat | Consume that packet and generate the context-echoing reply, subject to term handling |
| Receive contextual heartbeat response | Consume that response, insert the distinct sender's ACK, and release the queue prefix only on a majority |
| `getReady` | Ownership movement is a stutter in the combined produced-read-state observation |
| `read_ready` | Transfer produced indices to the existing tracker records |
| `apply(index)` | Apply the checked log slice to the register; require increasing progress within committed/durable indices; derive write completions |
| `complete_reads` | Complete eligible requests and record the callback's actual applied index and register value |
| Role/term reset | Invalidate postponed and unconfirmed queues; do not fabricate callbacks |
| Message release/drop/duplication | Move or modify the exact contextual packet bag |

At invocation, an observer-only `fences` entry records the maximum commit index
visible across the consensus-checked nodes. `ReadBarrierInv` requires confirmed
indices and completed callbacks to cover this prefix. The fence does not enable
protocol actions or choose their results. Completion's transition additionally
requires the confirmed index to have been applied. This second pass checks
prefix freshness/barrier and application safety. The third pass separately
checks linearizability of the finite register history.

## Heartbeat mapping discrepancy found during integration

The first run rejected `read-batch` at observation 43: follower 2 had commit 0,
and received a heartbeat carrying commit 1. Zig's `Raft.handleHeartbeat` advances
to 1. The imported `AppendEntriesAlreadyDone` uses its ordinary empty-append
branch when `prevLogIndex + 1 > commitIndex`; at commit 0 this bounds the update
by `prevLogIndex + Len(entries) = 0`, so the upstream model kept commit 0.

The local `RaftzTrace.ReceiveAligned` now uses `ReceiveHeartbeat` for same-term
heartbeats delivered to a follower: commit to the received commit index, bounded
by the model log length, then produce the normal heartbeat response. Higher-term
and candidate transitions still go through upstream receive steps first. This
matches the inspected implementation and retains all eight invariants, including
on intervening consensus microsteps. Upstream source remains checksum-identical;
this is a documented adapter delta, not a silent invariant relaxation or a change
to Zig behavior. The `heartbeat-commit` negative changes the real observation
back to 0 and is rejected by the corrected consensus adapter.

## Evidence and failure discipline

Checked with Zig 0.16.0, the existing pinned TLC jars and OpenJDK 25:

| Execution | Observations | Consensus generated / distinct | Read generated / distinct |
| --- | --- | --- | --- |
| Postponement, batch release, duplication, delayed apply, completed/overlapping writes, pending write | 278 | 548 / 547 | 280 / 279 |
| Isolated old leader, newer leader write, unconfirmed old read, healing/reset, new leader read | 178 | 364 / 363 | 180 / 179 |
| Equal-valued writes, separate acknowledgments, and `same -> other -> same` | 261 | 521 / 520 | 263 / 262 |
| Retained old-term responses after stepdown and reelection; fresh read confirmation | 307 | 627 / 626 | 309 / 308 |

All four complete: the first issues six reads and completes all six, acknowledges
two writes and leaves one write pending; the second issues two reads and
completes only the new leader's read, alongside one successful write. The third
completes four independently identified writes and three reads. The batch scenario
releases `batch-first` using the quorum for `batch-last` before delivering any
heartbeat for `batch-first`. Completion is attempted while applied lags and
succeeds only after explicit application advancement. A duplicated response and
late responses do not produce a second callback.

The fourth scenario retains two copies of a real term-1 heartbeat response in
both observed network bags while node 2 becomes leader in term 2. One copy is
delivered after node 1 steps down; the other after node 1 wins term 3 and issues
a fresh read. Neither copy revives the old request or confirms the new one.
The new read completes only after fresh quorum responses, returning the value
of a completed term-3 write. The old read remains pending in the client history.
`Raft.step` rejects lower-term messages before role dispatch; `Raft.reset` clears
`ReadOnly` and deferred reads. The existing models cover these boundaries without
schema changes or weakened guards. This tests the combination of term/role checks,
reset and fresh contexts, not the independent necessity of the term guard when
contexts cannot repeat. Context reuse and arbitrary delay schedules remain outside
this execution's claim.

Eleven schema-valid semantic mutations are rejected: premature read state,
forged ACK, wrong heartbeat context, wrong tracker index, early callback,
duplicate callback, apply beyond commit, the heartbeat commit discrepancy,
corrupt register state, wrong returned value and early write completion.
Consensus rejects the heartbeat discrepancy; the read/application model rejects
the other ten. Four additional identity mutations reject proposal/log context
substitution, wrong-write acknowledgment and duplicate write acknowledgment.
Three stale-response mutations reject crediting an old ACK to a fresh request,
reviving the reset old request, and completing the new read without confirmation.
All three preserve consensus and fail read/application conformance.
Two history mutations and eight dedicated history regressions exercise the
third pass independently. Malformed-input regressions cover profile mismatch,
unknown fields/events, missing terminal, appended suffix, reordered numbering,
duplicate JSON keys, malformed contexts/ACKs/indices and truncated lines.

Successful consensus/read checks require TLC exit zero, completed search, final empty
queue and the exact final observation/instruction-count marker. Only the expected
read invariant violation or TLC deadlock exit is semantic rejection; timeouts,
parse errors, unrelated invariants and incomplete searches fail the gate rather
than count as negative evidence. Each stage has a 60-second timeout per trace.
The history stage uses an explicit anti-invariant to find an existential witness,
not the conformance pass/fail convention; see its [verdict rules](register-history.md).
There are no state constraints, simulation or ignored trace suffixes. Only the
fully consumed conformance terminal state has an intentional self-loop.

The task also runs 112 Python tests. The default Zig suite passes 791/791 tests;
the separate trace build now executes six scenarios. Existing consensus traces,
9 semantic/5 structural negatives, and the upstream baseline are retained.
Artifacts live under `.cache/tla/read-*.ndjson`, `read-mutations-*/`, and
`identity-mutations-*/`, `stale-mutations-*/`, and `runs/read-trace-*/`; each read run records its input, projection, steps, model,
config, command, log, state files and pointers to consensus/history artifacts.

Remaining work includes broader application histories and production integration,
follower-forwarded reads, arbitrary delayed-response schedules, cancellations, context reuse/incarnations,
leases and joint consensus, production scheduling, partial durability and
unbounded refinement/liveness proofs. Passing these four executions establishes
none of those broader claims.
