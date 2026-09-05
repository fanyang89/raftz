# Multi-Raft

`MultiRaftHost` runs multiple independent Raft groups on one physical node. The
MVP keeps the existing `Raftor` implementation as the per-group engine and adds
a node-level registry, cooperative scheduler, and shared transport envelope.

## Model

```text
Application
    |
MultiRaftHost
    |-- group 10: Raftor + StateMachine + WAL
    |-- group 20: Raftor + StateMachine + WAL
    `-- group N:  Raftor + StateMachine + WAL
    |
MultiTransport { group_id, Message }
```

Each group has independent consensus state, membership, Ready processing,
proposal tracking, snapshots, and storage. A non-empty host `data_dir` produces
this layout:

```text
<data_dir>/groups/<group_id>/
```

An empty `data_dir` selects one independent `MemoryStorage` per group.

## Construction

Create one shared transport for the physical node, then add groups before
starting `run`:

```zig
const host = try raft.MultiRaftHost.create(allocator, .{
    .node_id = node_id,
    .data_dir = data_dir,
    .group_drive_budget = 64,
}, shared_transport);
defer host.destroy();

var group_config = raft.RaftorConfig{};
group_config.raft.id = node_id;
group_config.initial_peers = peers;

try host.addGroup(.{
    .group_id = 10,
    .raftor = group_config,
}, state_machine);
```

`MultiRaftHost` generates the group WAL directory and filesystem setting.
`MultiRaftGroupConfig.raftor.data_dir` and `file_system` must therefore remain
unset. The StateMachine is borrowed and must remain alive until the group is
removed or the host is destroyed.

`addGroup` and `removeGroup` are synchronous management APIs for a stopped host.
Runtime management uses the thread-safe queue:

- `requestAddGroup`
- `requestRemoveGroup`
- `requestRestartGroup`

Accepted requests complete exactly once through `GroupOperationCallback` on the
host event-loop thread. The queue deep-copies all nested configuration strings,
peer contexts, and legacy migration data. The supplied StateMachine remains
borrowed and must stay alive after a successful add or restart. Queue count and
retained bytes are bounded by `max_queued_group_operations` and
`max_queued_group_operation_bytes`; saturation returns
`GroupOperationBackpressure`. `queuedGroupOperations` reports both values.

Restart destroys the current group and reopens its per-group WAL with the new
configuration and StateMachine. If recreation fails, the group remains removed
and can be added again. `stop` completes every accepted but unprocessed operation
with `ShuttingDown`.

Proposal and ReadIndex submission may run concurrently with `run` and runtime
group management under the same allocator requirements as `Raftor`.

## Scheduling

The host uses one event-loop thread:

1. Process up to `group_operation_budget` runtime management requests.
2. Poll up to `transport_poll_budget` shared envelopes.
3. Poll up to `priority_poll_budget` groups woken by ingress or network events.
4. Drive up to `group_drive_budget` groups in stable round-robin order.

Proposal, ReadIndex, envelope, peer-event, and newly-created group activity adds
a generation-qualified wake to a deduplicated bounded queue. Priority polling
drains ingress without advancing the Raft logical clock, reducing latency for a
busy group that is not next in round-robin order. The normal round-robin pass
still runs independently, so sustained activity cannot starve idle groups or
their election clocks. A full `max_queued_group_wakes` queue drops only the
priority hint; the group remains reachable through fair scheduling.

The budgets bound work per host iteration and prevent a large registry from
monopolizing the caller. A group driven less frequently also advances its Raft
logical clock less frequently; size the round-robin budget and host tick interval
together. Set `priority_poll_budget` to zero to disable activity hints.

Terminal group errors are isolated and stop only that group. Retryable errors
are reported by `tick` or `poll` and retained in `MultiRaftGroupStatus`.

## Observability

`getHostStatus` returns a lock-safe `MultiRaftHostStatus` snapshot containing:

- group counts by lifecycle state
- queued management operation count and retained bytes
- tick, poll, and total host iterations
- round-robin/priority drives, productive group iterations, and group errors
- completed and failed management operations
- routed envelopes, peer events, unknown-group messages, queued wakes, and dropped wake hints

`getStatus(group_id)` adds per-group scheduler counters and pending message/event
counts to the underlying `NodeStatus`. `listGroupStatuses` allocates a sorted
snapshot for the full registry; the caller owns the returned slice. Metrics are
monotonic process-lifetime counters and snapshots may span adjacent event-loop
operations rather than one global transaction.

## Snapshot Scheduling

`MultiRaftHost` disables per-Raftor end-of-tick snapshot execution and applies a
host-wide `snapshot_budget` instead. Due groups are inspected from a separate
round-robin cursor, so one group cannot consume every automatic snapshot slot.
The original per-group entry threshold, interval, and retry-rate settings remain
in effect. A zero host budget disables automatic snapshots without changing
manual snapshots.

`requestSnapshot(group_id, callback)` queues a manual snapshot on the host event
loop and reports it as the `snapshot` operation kind. Host and group status
include snapshot attempts, successes, and failures. An automatic snapshot error
is logged and counted but does not stop the group or prevent another group from
using the next snapshot slot.

Snapshot creation and persistence remain synchronous on the cooperative event
thread. The budget bounds how many snapshots start in one iteration; applications
must still keep StateMachine snapshot work bounded or provide suitably sparse
thresholds.

## Online Replica Migration

`requestReplicaMigration` runs a bounded, in-memory replacement workflow on the
current group leader:

1. add the target as a learner;
2. wait until it is active and fully matched to the leader commit index for the
   configured number of stable ticks;
3. promote it to a voter;
4. re-check voter catch-up stability;
5. remove the source voter.

The target application must create the same Group in join mode first, with the
same cluster ID, seed peers, and an appropriate StateMachine. The Host does not
provide a remote control plane for constructing the target Group.

`getReplicaMigrationStatus` reports the current stage, elapsed logical ticks,
replication indexes, and target activity. `cancelReplicaMigration` stops the
workflow without undoing membership changes that already committed. Leader loss
pauses new actions while the total timeout continues. Timeout, cancellation,
shutdown, or a conflicting external configuration change leave the last safe
membership in place; the coordinator never automatically removes an added
learner or voter.

When the local source is the leader and proposal forwarding is enabled, the
coordinator transfers leadership to the caught-up target before forwarding the
source-removal change. This avoids an election gap on the normal path. With
proposal forwarding disabled it removes the source directly and the remaining
voters perform a normal election.

With a non-empty Host `data_dir`, migration intent is persisted before the
request is accepted. Each CRC-protected record is replaced atomically under
`<data_dir>/migrations/<group_id>.intent`. Completion, cancellation, and timeout
remove the record; shutdown preserves it. Corrupt records fail Host startup
instead of being ignored.

Callbacks and transient stages are not persisted. After restart, the application
recreates the Group and calls `resumeReplicaMigration`; the workflow reloads the
original IDs, address, timeout, and stability requirement, then infers whether
the target is absent, already a learner, or already a voter. The timeout starts
again when resumed. `getRecoveredReplicaMigrationStatus` exposes intents waiting
for their Group and callback. Memory-backed Hosts retain the existing in-memory
behavior without recovery.

Every Host tracks whether its local node has entered each Group's membership.
Once a committed change later retires that node, the Host stops the local Group while
preserving its WAL. A join-mode target is not stopped while it is still waiting
to be added.

`migration_step_budget` and `max_active_migrations` bound coordinator work and
memory. Host metrics expose active, started, completed, failed, timed-out,
cancelled, and leader-transfer counts.

## Shared Transport

`MultiTransport` is parallel to the existing single-group `Transport`. Its
outbound and inbound values are `RaftEnvelope`:

```zig
pub const RaftEnvelope = struct {
    group_id: u64,
    message: Message,
};
```

Peer registration includes `group_id`, allowing a physical implementation to
reference-count one connection across group memberships. Implementations must
reject conflicting addresses for the same physical peer. Peer events also carry
a group ID so snapshot and reachability failures reach the correct `Raftor`.

`encodeEnvelope` and `decodeEnvelope` define the outer binary envelope while
reusing the existing Message codec unchanged. Built-in implementations are:

- `LoopbackMultiNetwork` for deterministic process-local routing.
- `GrpcLiteMultiTransport` for persistent node-to-node streams shared by all
  groups.

The grpc implementation opens one directed stream per physical peer, validates
node and cluster identity, reference-counts group peer registrations, and
rejects conflicting addresses declared by different groups. Snapshot failures
remain group-qualified.

A networked host typically owns the transport separately:

```zig
const transport = try raft.GrpcLiteMultiTransport.create(allocator, .{
    .identity = .{ .cluster_id = cluster_id, .node_id = node_id },
    .listen_addr = "0.0.0.0:9000",
});
defer transport.destroy();

const host = try raft.MultiRaftHost.create(
    allocator,
    .{ .node_id = node_id, .data_dir = data_dir },
    transport.transport(),
);
defer host.destroy();
```

Destroy the host before the caller-owned transport. All groups sharing one
transport must use the same physical cluster identity and consistent addresses
for each node.

Unknown inbound group IDs are dropped and counted by
`unknownGroupMessageCount`; they do not fail other groups.

## Lifecycle

`stop` is idempotent, stops every group, and then stops the shared physical
transport. `destroy` waits for an active `run` loop, destroys every per-group
Raftor and WAL, clears callbacks, and releases the host. The shared transport
object itself remains caller-owned.
