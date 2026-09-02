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

Group management is serialized and is rejected while `run` is active. Proposal
and ReadIndex submission may run concurrently with `run` under the same allocator
requirements as `Raftor`.

## Scheduling

The host uses one event-loop thread:

1. Poll up to `transport_poll_budget` shared envelopes.
2. Drive up to `group_drive_budget` groups in stable round-robin order.
3. Each selected group runs one `Raftor.tick` or `Raftor.poll` iteration.

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
reusing the existing Message codec unchanged. `LoopbackMultiNetwork` provides a
built-in process-local shared transport. A multiplexed grpc-lite backend is not
part of this MVP; applications can implement `MultiTransport` with the envelope
codec without changing Raft core messages.

Unknown inbound group IDs are dropped and counted by
`unknownGroupMessageCount`; they do not fail other groups.

## Lifecycle

`stop` is idempotent, stops every group, and then stops the shared physical
transport. `destroy` waits for an active `run` loop, destroys every per-group
Raftor and WAL, clears callbacks, and releases the host. The shared transport
object itself remains caller-owned.
