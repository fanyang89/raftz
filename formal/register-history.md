# Applied register values and finite-history linearizability

## What is checked

`mise run test-tla-read-trace` now requires three passes over each real execution:
consensus conformance, read/application conformance, and a linearization witness
for its client history. Version 4 declares `application = "register-v1"` and records independent proposal
contexts; older version-2/3 read traces lack required evidence and are rejected
rather than silently supplying default values. Regenerate them with the task. The existing
version-1 consensus-only profile is unchanged.

The test application is a single in-memory last-write-wins register, initially
empty. In `tests/tla_trace_test.zig`, committed nonempty entries are delivered to
an actual `StateMachine.apply` vtable implementation; no-op entries advance the
cursor without changing the value. The driver acknowledges application through
`RawNode.advanceApplyTo` and completes the real `ProposalTracker` proposal after
applying its command. A read callback samples the application's actual register
and the actual applied cursor, not an expected value derived by the serializer.

Writes use nonempty UTF-8 values, which may repeat. Every proposal has a fresh
context (`write-1`, `write-2`, ...), passed to both `ProposalTracker.track` and
`RawNode.propose`. Application completes the request using `entry.context`, never
its data value. The proposal input, replicated/durable log context, tracker
record and client-history ID retain that identity throughout validation. These
are test-generated IDs, not a verification of Raftor's binary request-context
Generator or incarnation/restart semantics. Read contexts are separately unique. The driver supports eight reads and eight
writes, fixed three-voter membership, atomic MemoryStorage and no restart in
these scenarios. Pending callbacks are retained, not synthesized as success or
failure at the end.

The read model independently applies each consensus-checked log slice and derives
the register's last nonempty entry. It compares the application value after every
observation, the proposal callback counts and the value returned by every read
callback. A read can return a value newer than its captured read index: the
callback reads at the **actual applied prefix**, provided that prefix covers the
read barrier. It is incorrect to require it to return the value at exactly the
captured index. The new overlapping read/write scenario exercises this distinction.

No production code is modified. This caller still does not run the production
Raftor/ReadyProcessor loop, a concurrent application, WAL, or an external client.
The results establish linearizability of the four recorded toy-register histories,
not all histories of raftz or arbitrary application semantics.

## History construction

`history_check.extract` records invocation at `propose`/`read` and response at the
first actual proposal/read callback observation. It does not equate a proposal,
commit, or apply cursor alone with a successful write response. Pending operations
have `returned = -1`. Duplicate callbacks, unknown/wrong-owner completions,
changing results and disappearing completed records are errors. All raw snapshots
must already pass the consensus and read/application models before this history
can make the combined task pass.

Operations contain `id`, `kind`, `value`, `result`, `invoked`, and `returned`.
Payloads are injectively UTF-8/hex encoded. The single scheduler's sequence number
is the logical clock. An apply batch may complete several writes at one timestamp;
callback bodies do not invoke further operations, and no other invocation can
interleave inside that batch. Thus this grouping loses no response-before-next-
invocation relation in these executions. The same applies to a batch of read
callbacks. Failed operations, timeout ambiguity and reentrant callbacks are not
modeled.

The history preserves uncompleted writes and reads. Pending reads may be omitted
from a completed history because they have no returned value or state effect.
A pending write may optionally be included: its effect can explain a completed
read even if its own response was never observed. It cannot explain a read that
finished before that write was invoked.

## The TLA+ question is existential

`models/RaftzRegisterHistory.tla` starts with the empty register and an empty set
of linearized operations. It may select an operation only when every completed
operation whose response precedes its invocation has already been selected.
A write replaces the register. A read may be selected only if its recorded
result equals the current register. Every completed operation must be selected
once; pending writes are optional, pending reads are not selected.

The state stores only the selected set and register value. Different orderings
with the same state merge, while TLC's behavior trace retains the witness order.
The checker exports that trace as JSON and independently replays its selected
operation order in Python; it does not add an order variable to the model or
disable state merging. With at most 16 operations this is a finite search;
there are no state constraints or symmetry reductions. The test is existence of **one** valid sequential order,
not correctness of every possible reordering.

Accordingly, `NoLinearization` is an intentional anti-invariant: violating it
means all completed operations have been legally selected. Success requires the
exact invariant-violation exit code, exact invariant name, a concrete multi-state
behavior and `RAFTZ_HISTORY_COMPLETE` with the correct completed-operation count.
Those log conditions are necessary but no longer sufficient: the exact run must
also produce `witness.raw.json` via TLC's `-dumpTrace json` option and pass the
independent replay below. A tool error, a different invariant violation, missing
marker, missing/invalid artifact or timeout is not a witness. TLC may stop as soon
as it finds a witness; a positive verdict is not an exhaustion claim.

A negative verdict requires exit zero, completed exhaustive search, a final empty
queue, no witness marker and no contradictory witness artifact. Deadlock checking
is disabled **only in this history search**: a bad ordering is a failed branch, not evidence that every ordering is
bad. The consensus/read conformance passes retain deadlock checking. A history
with no completed operations is rejected as vacuous before TLC runs.

## Independent positive-witness replay

`history_check.verify_witness` reads the actual TLC JSON counterexample to
`NoLinearization`. It requires consecutive numbered states, an empty initial
register/selected set, and transitions adding exactly one known operation.
Each action must connect the corresponding states and name the same selected
operation. Duplicate keys, unknown variables, disconnected edges, missing steps,
stuttering, removed operations and out-of-range/boolean indices are rejected.
Source-location annotations are diagnostic, not semantic evidence. Artifacts
are bounded to 4 MB and at most 17 states for the existing 16-operation limit.

The operation indices are mapped to original client IDs. A separate Python
`replay` then checks that:

- Every selected ID exists and occurs once; every completed operation is selected.
- Pending writes may be included or omitted; pending reads must be omitted.
- Every completed operation returning strictly before an operation's invocation
  appears earlier in the order. Equal timestamps retain the model's overlap rule.
- Starting with the empty register, each write replaces the value and each read
  returns the current value.
- Every TLC register value agrees with the independently computed prefix value.

Only after these checks does the runner write `linearization.json`, containing
`version`, named operation `order`, prefix `values` (including the initial empty
value), and `omitted` pending operation IDs. Merely finding a success marker is
insufficient. This is validation of a concrete positive certificate, not another
exhaustive solver: negative verdicts still depend on TLC's completed search, and
replay does not prove that history extraction or arbitrary raftz executions are
correct. Consensus/read conformance remain mandatory for implementation traces.

## Commands, evidence and regression cases

```bash
mise run test-tla-read-trace

# Standalone history checking (JSON array, not an implementation conformance claim):
PYTHONDONTWRITEBYTECODE=1 python3 formal/history_check.py path/to/history.json
```

The existing pinned jars, one worker, 2 GiB heap and 60-second per-pass timeout are
reused. Each read run stores `history.json` and `history-run.txt`; the referenced
`.cache/tla/runs/register-history-*/` stores the exact operation input, model,
configuration, command, state files and TLC log. `witness.raw.json` retains any
TLC candidate witness; `linearization.json` is written only after replay succeeds.
Timeouts and incomplete runs never count as either positive or negative results.

| Real scenario | Observations | Operations | Completed / pending | Result |
| --- | --- | --- | --- | --- |
| Gate, batch, delayed apply, read-after-completed-write, overlapping write, unacknowledged isolated write | 278 | 9 | 8 / 1 | All three passes succeed; linearization witness found |
| Isolated old leader, newer leader write/read, healing | 178 | 3 | 2 / 1 | All three passes succeed; linearization witness found |
| Equal-valued writes with independent acknowledgments, then `same -> other -> same` | 261 | 7 | 7 / 0 | All three passes succeed; linearization witness found |
| Delayed old-term responses after stepdown and reelection | 307 | 5 | 4 / 1 | All three passes succeed; linearization witness found |

The first scenario includes six successful reads, two successful writes and one
pending write. The second includes a successful write/read and an old leader's
uncompleted read. There is no fabricated completion for either pending operation.
The third scenario applies only the first of two equal-valued writes and checks
that only its callback fires. It then applies the second, changes the register
to `other`, and writes `same` again under a fourth ID. All four write callbacks
fire exactly once; three reads return the expected values. The fourth scenario
completes three writes and the new-term read, leaving the old-term read pending.
A duplicated old response is delivered once after stepdown and once after
reelection, without fabricating either read callback. Three additional trace
mutations (old ACK credited to a new read, resurrection of the old request,
unconfirmed new-read callback) pass consensus but fail read/application checking.

Eleven full-trace semantic mutations are rejected, including the three new cases:
corrupt application state, wrong returned read value, and a write callback before
application. Two independently mutated histories (a future value and a stale value
after a completed write) have no linearization; they exercise the third pass even
though full-trace value corruption is normally caught by the second pass first.

Four additional identity mutations are rejected even where the data value stays
the same: substitution of a proposal input ID, acknowledging the wrong write,
a duplicate write callback and substitution of a committed log context. The
contextual consensus profile records proposal context as an independent input;
it never infers the submitted ID from the resulting log. The two callback
mutations pass consensus but fail read/application conformance. Correct final
values alone would not establish this request-correlation property.

Eight additional TLC histories cover initial reads, overlapping orders with both
successful and dead-end choices, optional pending writes, an impossible future
pending write, unwritten values, stale reads, repeated values and omission of
pending reads. Five have witnesses; three complete exploration without one.
Four mutations of a real TLC witness/order are also rejected for their expected
semantic reason: wrong register value, omitted completion, reversed real-time
order and repeated operation ID. These fixtures are retained under the history
run's `witness-mutations/` directory. The offline Python suite passes 132 tests,
including malformed certificates, success logs without usable artifacts,
contradictory negative artifacts, optional pending writes, exact verdict handling,
profile boundaries and identity-preserving history extraction. The shared Zig
trace build passes 6/6 tests in Debug and ReleaseSafe; the default Zig suite
passes 791/791 separately.

Remaining boundaries include exhaustive implementation scheduling, empty writes,
follower-forwarded reads, leases, membership changes, ID reuse/incarnations,
cancellation/restart semantics, real transports/storage, production-loop
integration, and unbounded refinement/liveness proofs.
