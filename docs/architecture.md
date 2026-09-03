# Architecture

raftz separates pure consensus decisions from persistence, transport, and
application state. The split supports both a low-level Ready/Advance API and a
complete orchestration layer.

## Layers

```text
Application
  StateMachine, proposals, linearizable reads, membership operations
                              |
                 MultiRaftHost (optional)
          group registry and shared transport routing
                              |
                           Raftor
  lifecycle, ingress queues, ReadyProcessor, status, snapshots
                              |
                           RawNode
  Ready ownership, persistence boundaries, advance cursors
                              |
                Raft + RaftLog + ProgressTracker
  elections, replication, quorum, ReadIndex, configuration changes
                              |
                Storage                 Transport
        MemoryStorage / WAL      Loopback / grpc-lite / custom
```

| Module | Role |
| --- | --- |
| `core/` | Entries, messages, state, snapshots, errors, and configuration-change values. |
| `raft.zig` | Consensus state machine and message handling. |
| `raft_log.zig`, `unstable_log.zig` | Stable and not-yet-persisted log views. |
| `progress*.zig`, quorum modules | Per-peer replication state and quorum calculations. |
| `raw_node.zig` | User-facing Ready/Advance boundary. |
| `ready_processor.zig` | Correct persistence, transport, apply, and advance ordering. |
| `raftor.zig` | One-group lifecycle, request queues, snapshots, membership, and status. |
| `multi_raft.zig`, `multi_transport.zig` | Cooperative group registry, scheduling, and shared envelope routing. |
| `storage.zig`, `memory_storage.zig`, `wal.zig` | Pluggable storage and built-in backends. |
| `transport.zig`, `loopback_transport.zig`, `rpc/` | Pluggable message transport and peer lifecycle. |

## Ready Processing

Raft produces decisions but does not perform external side effects. `Ready`
describes the work an integration must complete. `ReadyProcessor` executes each
batch in this order:

1. Validate entry checksums when enabled.
2. Persist an incoming snapshot as the storage baseline.
3. Persist unstable entries following that snapshot.
4. Persist HardState and sync when required.
5. Restore the durable snapshot into the application state machine.
6. Send messages whose safety depends on persistence.
7. Apply committed entries and complete ReadIndex requests.
8. Advance RawNode and process LightReady entries and messages.

This order is a safety contract. In particular, vote responses and follower
append responses must not become externally visible before the state that
justifies them is durable.

`Ready.must_sync` indicates that the storage backend must establish its durable
boundary. `Ready.is_persisted_msg` distinguishes messages that become sendable
only after persistence; `Ready.messages()` and `Ready.persistedMessages()` split
the two send phases without duplicating a message.

### Async Ready Group Commit

`RaftorConfig.async_ready` enables an opt-in, raft-rs-style group-commit path.
It does not create storage or apply threads. The event-loop thread writes each
Ready's snapshot, entries, and HardState into the same readable storage, calls
`advanceAppendAsync`, and may stage more Ready batches. A durability barrier
performs one `sync` for the group and then acknowledges the highest Ready number
with `onPersistReady`.

Leader messages may be sent after their Ready is staged so replication overlaps
the group-commit window. Follower and candidate messages remain queued until the
barrier succeeds. Snapshot restore, committed-entry apply, ReadState completion,
and `advanceApplyTo` also remain after the barrier. Incoming snapshots,
committed configuration changes, and `async_ready_max_inflight` are forced
barriers. `tick`, `poll`, `campaign`, and `flushReady` return only after their
staged group is flushed; storage access remains single-threaded.

## Raftor Tick

One `Raftor.tick` iteration serializes node mutation and performs bounded work:

1. Expire tracked requests and drain bounded proposal and ReadIndex queues.
2. Advance the Raft logical clock and process resulting Ready batches.
3. Poll bounded inbound transport messages and peer events.
4. Process additional Ready batches produced by inbound work.
5. Trigger automatic snapshots when configured thresholds are met.
6. Publish a synchronized status snapshot when leaving the event loop.

Budgets prevent one input source from monopolizing the event loop. Request data
is copied before entering the queues, so producer threads do not need to retain
their input buffers.

## Multi-Raft Host

`MultiRaftHost` owns stable heap allocations for multiple `Raftor` instances and
drives them in sorted, round-robin order. Each group keeps its own Ready
pipeline, StateMachine, membership, snapshot, and WAL directory. A virtual
single-group Transport adapter adds or removes the group ID at the shared
`MultiTransport` boundary, so a group shutdown cannot stop another group's
physical connection.

Runtime group operations, activity-priority polls, host transport polling, and
round-robin clock driving have independent budgets. Runtime add/remove/restart
commands pass through a bounded thread-safe queue and execute only on the host
event loop. Generation-qualified wake hints reduce ingress latency without
stealing the fair round-robin clock budget. A separate fair cursor and host-wide
budget serialize due group snapshots. Atomic counters and registry locks expose
host and per-group status without moving Raft mutation off the event thread.
Terminal errors are recorded per group and do not stop healthy groups. See
[Multi-Raft](multi-raft.md) for the public API and MVP limits.

## Storage Boundary

The consensus core reads through `Storage`. `Raftor` needs `WritableStorage` to
append entries, persist HardState and snapshots, synchronize, and compact.
`Raftor.create` selects MemoryStorage for an empty `data_dir` and WALStorage for
a non-empty directory. `createWithDependencies` allows another implementation.

Storage and application snapshots form one recovery boundary: the storage
snapshot carries Raft metadata and, in durable mode, address-aware membership,
while the StateMachine owns the application payload and atomic restore behavior.

## Transport Boundary

`Transport` routes outbound messages by node ID and queues inbound work from
foreign threads. Raft callbacks run only when the event loop calls `pollOne`.
This keeps consensus mutation single-threaded even when a transport has network
worker threads.

Peer lifecycle events report unreachable peers, failed snapshot delivery, and
identity rejection. Raftor converts those events into the corresponding
RawNode reports.

## Failure Model

Apply and snapshot-restore errors are terminal because Raftor cannot safely
continue after application state diverges from the committed log. Persistence
failures retain the pending Ready phase so callers can inspect the terminal
error without incorrectly advancing the consensus cursors.

Fast internal invariants run by default in Debug and ReleaseSafe builds. The
test harness adds election-safety, committed-prefix, convergence, filesystem
fault, and crash-recovery checks; see [Testing](testing.md).
