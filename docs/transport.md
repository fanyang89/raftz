# Transport

Raft consensus operates on node IDs and messages. The `Transport` interface
maps those IDs to application-defined delivery and peer lifecycle behavior.

## Contract

The type-erased `Transport` vtable provides:

| Method | Responsibility |
| --- | --- |
| `start`, `stop` | Manage transport lifecycle. |
| `addPeer`, `removePeer` | Maintain the current ID-to-address routing table. |
| `send` | Queue or transmit messages according to each message's `to` field. |
| `setMessageCallback` | Register inbound message delivery. |
| `setPeerEventCallback` | Register unreachable, snapshot-failure, and identity-rejection events. |
| `pollOne` | Deliver at most one queued message or event on the Raft event loop. |
| `identity` | Optionally expose the transport's cluster and local-node identity. |
| `setWakeup` | Optionally register a borrowed `*raft.Wakeup` for driver notifications. |

Foreign network threads must queue input rather than invoke Raft directly.
Callbacks run only through `pollOne` on the owner event loop. Message ownership
transfers to `MessageCallback` when invoked, including when it returns an error.
Messages passed to `send` remain borrowed; an asynchronous implementation must
clone or share retained data before `send` returns.

## Built-In Transports

| Implementation | Use |
| --- | --- |
| `NoopTransport` | Single-node `Raftor.create` and direct tests. |
| `LoopbackTransport` | Deterministic in-process multi-node tests and simulations. |
| `GrpcLiteTransport` | Persistent raw-stream network transport. |

Applications may provide another implementation through `Transport.VTable`.

## Event-Driven Raftor

`Raftor.run()` processes proposals, ReadIndex requests, inbound messages, and
peer events as they arrive. The single driver thread still owns all Raft
mutation. Apply, read-confirmation, and timeout callbacks run on that thread.
`stop()` invokes detached shutdown callbacks synchronously on its calling
thread, potentially concurrently with normal callbacks on the driver.
Callbacks must be thread-safe, must not block, and must not assume one fixed
execution thread. `tick_interval_ms` controls logical ticks
(100 ms by default, must be nonzero), independently of request processing.
The driver checks its monotonic deadline between work batches. After a delayed
tick it schedules the next interval from completion, without catch-up ticks.

With `GrpcLiteTransport` or `NoopTransport`, an idle driver blocks on a Linux
futex until notified or the next tick is due. Notifications coalesce. The
driver resets the notification before inspecting queues, so an enqueue racing
with entry into the wait remains observable. `stop()` also wakes the driver;
the caller must join its driver thread before releasing borrowed resources.
Synchronous storage and state-machine work can still delay the driver.

Custom transports opt in through `Transport.VTable.set_wakeup`. Publish each
inbound message or peer event before calling `Wakeup.notify()`. Registration
must notify if work may already be queued. Synchronize registration and
notification, and ensure clearing the registration waits for in-flight users
of the borrowed pointer. Raftor registers before transport startup and clears
the registration on startup failure or destruction. The built-in gRPC queues
use their queue locks for this lifetime guarantee.

Transports without this optional hook retain a bounded 1 ms polling fallback.
`tick()` and `poll()` remain available for application-owned drivers and
deterministic simulation; `poll()` never advances logical time. MultiRaftHost
retains its existing cooperative driver.

## Shared Multi-Raft Transport

`MultiTransport` is a node-level interface for multiple groups. It carries
`RaftEnvelope` values and makes peer registration group-aware:

| Method | Responsibility |
| --- | --- |
| `addPeer`, `removePeer` | Add or release one group's reference to a physical peer. |
| `send` | Send borrowed `{ group_id, Message }` envelopes. |
| `setEnvelopeCallback` | Deliver owned inbound envelopes through `pollOne`. |
| `setPeerEventCallback` | Deliver group-qualified peer failures. |

`MultiRaftHost` creates one virtual `Transport` adapter per group, while only the
shared transport owns physical lifecycle. `LoopbackMultiNetwork` is the built-in
process-local implementation; group-scoped link blocking, partitions, healing,
drop counters, and stopped-node transport recreation support deterministic fault
tests. `GrpcLiteMultiTransport` multiplexes envelopes
for every group over one persistent directed stream per physical peer.
`encodeEnvelope` and `decodeEnvelope` define its outer wire format. The original
`GrpcLiteTransport` remains the single-group backend.

## grpc-lite Setup

`GrpcLiteTransport.create` requires a thread-safe allocator because server
callbacks and peer workers encode, decode, and enqueue data concurrently.

```zig
const identity = raft.TransportIdentity{
    .cluster_id = cluster_id,
    .node_id = node_id,
};

const grpc_transport = try raft.GrpcLiteTransport.create(allocator, .{
    .identity = identity,
    .listen_addr = "0.0.0.0:9000",
});
defer grpc_transport.destroy();

const node = try raft.Raftor.createWithTransport(
    allocator,
    config,
    state_machine,
    grpc_transport.transport(),
);
defer node.destroy();
```

The transport is caller-owned and must outlive the Raftor. Destroy the Raftor
before destroying the transport.

`Raftor` starts and stops the borrowed transport. Durable membership populates
its peer table from bootstrap state, committed configuration changes, or
restart state.

## Runtime and Addresses

IPv4 literals require no global DNS setup. Hostname peer addresses need one
process-wide `raft.GrpcRuntime` passed through
`GrpcLiteTransportConfig.runtime`. Initialize it before application threads and
deinitialize it after every transport:

```zig
var runtime = try raft.GrpcRuntime.init();
defer runtime.deinit();

const grpc_transport = try raft.GrpcLiteTransport.create(allocator, .{
    .identity = identity,
    .listen_addr = "0.0.0.0:9000",
    .runtime = &runtime,
});
```

The current grpc-lite integration uses IPv4 addresses. `listen_addr` is the
local bind address; durable peer membership should contain addresses reachable
from other nodes.

## Backpressure and Recovery

The grpc-lite backend uses one persistent directed raw stream per peer and
bounded buffering at every ingress layer.

`stream_limits` controls grpc-lite message and stream queues.
`mailbox_max_messages` and `mailbox_max_bytes` bound decoded inbound Raft
messages. Outbound saturation returns `TransportBackpressure`. An inbound
mailbox overflow terminates the affected stream with `InboundMailboxFull` so
memory cannot grow without limit; peer recovery then follows reconnect policy.

Peer workers reconnect with bounded exponential delays configured by
`reconnect_initial_delay_ns` and `reconnect_max_delay_ns`. Transport recovery
does not make application proposals idempotent; applications remain responsible
for request identity and retry policy.

Peer lifecycle events report unreachable peers and failed snapshot delivery to
Raft. Identity rejection is surfaced separately so operators can distinguish a
configuration error from an ordinary connection failure.

## Security Boundary

The raftz grpc-lite backend does not expose TLS or peer authentication.
Stream metadata includes the protocol version, cluster ID, source node ID, and
target node ID to detect misconfiguration, but a network attacker can forge it.

Deploy the transport only on a trusted network or behind a separately secured
network boundary that provides encryption, authentication, authorization, and
traffic isolation. Do not treat successful identity validation as proof of peer
identity.

## Logging and Shutdown

The transport shares raftz's process-global logger. Initialize it before
starting transports; otherwise logs are discarded.

`stop` prevents new delivery and begins bounded graceful shutdown. The transport
must remain alive until Raftor event-loop work and callbacks have stopped.
Follow the lifecycle order in [Getting Started](getting-started.md).
