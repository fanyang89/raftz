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
3. Drive up to `group_drive_budget` groups in stable round-robin order.
4. Each selected group runs one `Raftor.tick` or `Raftor.poll` iteration.

The budget bounds work per host iteration and prevents a large registry from
monopolizing the caller. A group driven less frequently also advances its Raft
logical clock less frequently; size the budget and host tick interval together.

Terminal group errors are isolated and stop only that group. Retryable errors
are reported by `tick` or `poll` and retained in `MultiRaftGroupStatus`.

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
