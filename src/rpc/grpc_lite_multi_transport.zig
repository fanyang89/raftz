//! Persistent grpc-lite transport multiplexing multiple Raft groups per stream.
//!
//! The allocator passed to `create` must be thread-safe.

const std = @import("std");
const grpc = @import("grpc_lite");

const Error = @import("../core/error.zig").Error;
const raw_node = @import("../raw_node.zig");
const transport_mod = @import("../transport.zig");
const multi_transport_mod = @import("../multi_transport.zig");
const codec = @import("../codec.zig");
const mailbox_mod = @import("multi_inbound_mailbox.zig");
const event_queue_mod = @import("multi_peer_event_queue.zig");
const peer_manager = @import("peer_manager.zig");

const TransportIdentity = transport_mod.TransportIdentity;
const GroupId = multi_transport_mod.GroupId;
const Envelope = multi_transport_mod.Envelope;
const EnvelopeCallback = multi_transport_mod.EnvelopeCallback;
const MultiPeerEvent = multi_transport_mod.PeerEvent;
const MultiPeerEventCallback = multi_transport_mod.PeerEventCallback;
const MultiTransport = multi_transport_mod.MultiTransport;
const MultiInboundMailbox = mailbox_mod.MultiInboundMailbox;
const MultiPeerEventQueue = event_queue_mod.MultiPeerEventQueue;
const PeerManager = peer_manager.PeerManager;

const log = grpc.log;
const stream_method_path = "/raft.MultiRaft/StreamEnvelopes";

const PeerGroupKey = struct {
    group_id: GroupId,
    peer_id: u64,
};

pub const Config = struct {
    identity: TransportIdentity,
    listen_addr: []const u8,
    stream_limits: grpc.StreamBufferLimits = .{},
    mailbox_max_messages: usize = 16_384,
    mailbox_max_bytes: usize = 128 * 1024 * 1024,
    reconnect_initial_delay_ns: u64 = 20 * std.time.ns_per_ms,
    reconnect_max_delay_ns: u64 = 2 * std.time.ns_per_s,
    graceful_shutdown_timeout_ns: u64 = 5 * std.time.ns_per_s,
    runtime: ?*grpc.Runtime = null,

    pub fn validate(self: Config) !void {
        if (self.identity.node_id == 0) return error.InvalidNodeId;
        if (std.mem.allEqual(u8, &self.identity.cluster_id, 0)) return error.ClusterIdRequired;
        if (self.listen_addr.len == 0) return error.ListenAddressEmpty;
        try self.stream_limits.validate();
        try (MultiInboundMailbox.Limits{
            .max_messages = self.mailbox_max_messages,
            .max_bytes = self.mailbox_max_bytes,
        }).validate();
        if (self.reconnect_initial_delay_ns == 0 or
            self.reconnect_max_delay_ns < self.reconnect_initial_delay_ns or
            self.graceful_shutdown_timeout_ns == 0)
        {
            return error.InvalidConfig;
        }
    }
};

pub const GrpcLiteMultiTransport = struct {
    const State = enum { initialized, starting, started, stopping, stopped };

    allocator: std.mem.Allocator,
    config: Config,
    listen_addr: []u8,
    server: grpc.Server,
    peer_manager: PeerManager,
    mailbox: MultiInboundMailbox,
    peer_events: MultiPeerEventQueue,
    registrations: std.AutoHashMap(PeerGroupKey, []u8),
    snapshot_pending: std.AutoHashMap(PeerGroupKey, void),
    registration_mutex: std.atomic.Mutex = .unlocked,
    callback: ?EnvelopeCallback = null,
    peer_event_callback: ?MultiPeerEventCallback = null,
    lifecycle_mutex: std.atomic.Mutex = .unlocked,
    state: State = .initialized,
    accepting: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    pub fn create(allocator: std.mem.Allocator, config: Config) !*GrpcLiteMultiTransport {
        try config.validate();
        const parsed = try parseAddress(config.listen_addr);
        const listen_addr = try allocator.dupe(u8, config.listen_addr);
        errdefer allocator.free(listen_addr);
        const self = try allocator.create(GrpcLiteMultiTransport);
        errdefer allocator.destroy(self);

        var mailbox = try MultiInboundMailbox.init(allocator, .{
            .max_messages = config.mailbox_max_messages,
            .max_bytes = config.mailbox_max_bytes,
        });
        errdefer mailbox.deinit();
        var events = try MultiPeerEventQueue.init(allocator, config.mailbox_max_messages);
        errdefer events.deinit();
        var server = try grpc.Server.init(allocator, .{
            .host = parsed.host,
            .port = parsed.port,
            .max_request_size = config.stream_limits.max_message_size,
            .stream_limits = config.stream_limits,
        });
        errdefer server.deinit();

        var owned_config = config;
        owned_config.listen_addr = listen_addr;
        self.* = .{
            .allocator = allocator,
            .config = owned_config,
            .listen_addr = listen_addr,
            .server = server,
            .peer_manager = undefined,
            .mailbox = mailbox,
            .peer_events = events,
            .registrations = std.AutoHashMap(PeerGroupKey, []u8).init(allocator),
            .snapshot_pending = std.AutoHashMap(PeerGroupKey, void).init(allocator),
        };
        self.peer_manager = PeerManager.init(allocator, .{
            .identity = config.identity,
            .method_path = stream_method_path,
            .stream_limits = config.stream_limits,
            .reconnect_initial_delay_ns = config.reconnect_initial_delay_ns,
            .reconnect_max_delay_ns = config.reconnect_max_delay_ns,
            .runtime = config.runtime,
            .event_sink = .{ .ctx = self, .function = queuePhysicalPeerEvent },
        });
        errdefer self.peer_manager.deinit();
        errdefer self.registrations.deinit();
        errdefer self.snapshot_pending.deinit();
        try self.server.registerStream(stream_method_path, .{
            .context = self,
            .on_start = onStreamStart,
            .on_message = onStreamMessage,
            .on_remote_end = onStreamRemoteEnd,
            .on_cancel = onStreamCancel,
        });
        return self;
    }

    pub fn destroy(self: *GrpcLiteMultiTransport) void {
        self.stop();
        self.callback = null;
        self.peer_event_callback = null;
        self.peer_manager.deinit();
        self.server.deinit();
        self.mailbox.deinit();
        self.peer_events.deinit();
        var iterator = self.registrations.valueIterator();
        while (iterator.next()) |address| self.allocator.free(address.*);
        self.registrations.deinit();
        self.snapshot_pending.deinit();
        self.allocator.free(self.listen_addr);
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn start(self: *GrpcLiteMultiTransport) Error!void {
        return startImpl(self);
    }

    pub fn stop(self: *GrpcLiteMultiTransport) void {
        stopImpl(self);
    }

    pub fn localAddress(self: *const GrpcLiteMultiTransport) !grpc.ServerLocalAddress {
        return self.server.localAddress();
    }

    pub fn port(self: *const GrpcLiteMultiTransport) !u16 {
        return self.server.port();
    }

    pub fn peerOpenCount(self: *GrpcLiteMultiTransport, id: u64) u64 {
        return self.peer_manager.openCount(id);
    }

    pub fn peerState(self: *GrpcLiteMultiTransport, id: u64) ?peer_manager.LifecycleState {
        return self.peer_manager.peerState(id);
    }

    pub fn peerCount(self: *GrpcLiteMultiTransport) usize {
        return self.peer_manager.count();
    }

    pub fn pollOne(self: *GrpcLiteMultiTransport) Error!bool {
        if (!self.accepting.load(.acquire)) return false;
        if (self.callback) |callback| {
            if (self.mailbox.pop()) |envelope| {
                try callback.invoke(envelope);
                return true;
            }
        }
        if (self.peer_event_callback) |callback| {
            if (self.peer_events.pop()) |event| {
                try callback.invoke(event);
                return true;
            }
        }
        return false;
    }

    fn startImpl(context: *anyopaque) Error!void {
        const self: *GrpcLiteMultiTransport = @ptrCast(@alignCast(context));
        lock(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        if (self.state != .initialized) return error.AlreadyStarted;
        self.state = .starting;
        peer_manager.lockTsanLifecycle();
        const start_result = self.server.start();
        peer_manager.unlockTsanLifecycle();
        start_result catch |err| {
            self.state = .stopped;
            return mapStartError(err);
        };
        self.accepting.store(true, .release);
        self.peer_manager.startAll() catch |err| {
            self.accepting.store(false, .release);
            self.peer_manager.stopAll();
            self.server.shutdown();
            self.server.wait();
            self.state = .stopped;
            return err;
        };
        self.state = .started;
        const address = self.server.localAddress() catch return;
        log.info(@src(), "grpc multi transport listening on {s}:{}", .{ address.host, address.port });
    }

    fn stopImpl(context: *anyopaque) void {
        const self: *GrpcLiteMultiTransport = @ptrCast(@alignCast(context));
        while (true) {
            switch (beginStop(self)) {
                .done => return,
                .wait => {
                    std.atomic.spinLoopHint();
                    continue;
                },
                .stop => break,
            }
        }
        self.server.shutdownGracefully(self.config.graceful_shutdown_timeout_ns);
        self.peer_manager.stopAll();
        peer_manager.lockTsanLifecycle();
        self.server.wait();
        peer_manager.unlockTsanLifecycle();
        self.mailbox.clear();
        self.peer_events.clear();
        lock(&self.lifecycle_mutex);
        self.state = .stopped;
        self.lifecycle_mutex.unlock();
    }

    fn addPeerImpl(context: *anyopaque, group_id: GroupId, id: u64, address: []const u8) Error!bool {
        const self: *GrpcLiteMultiTransport = @ptrCast(@alignCast(context));
        if (group_id == 0) return error.InvalidGroupId;
        lock(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        if (self.state == .stopping or self.state == .stopped) return error.ConnectionClosed;

        const key = PeerGroupKey{ .group_id = group_id, .peer_id = id };
        lock(&self.registration_mutex);
        defer self.registration_mutex.unlock();
        if (self.registrations.get(key)) |existing_address| {
            if (!std.mem.eql(u8, existing_address, address)) return error.ConflictingPeerAddress;
            return false;
        }
        var first_reference = true;
        var iterator = self.registrations.iterator();
        while (iterator.next()) |entry| {
            if (entry.key_ptr.peer_id != id) continue;
            first_reference = false;
            if (!std.mem.eql(u8, entry.value_ptr.*, address)) return error.ConflictingPeerAddress;
        }
        try self.registrations.ensureUnusedCapacity(1);
        const address_copy = try self.allocator.dupe(u8, address);
        errdefer self.allocator.free(address_copy);
        if (first_reference) _ = try self.peer_manager.addPeer(id, address);
        self.registrations.putAssumeCapacity(key, address_copy);
        return first_reference;
    }

    fn removePeerImpl(context: *anyopaque, group_id: GroupId, id: u64) Error!void {
        const self: *GrpcLiteMultiTransport = @ptrCast(@alignCast(context));
        lock(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        if (self.state == .stopping or self.state == .stopped) return error.ConnectionClosed;
        const key = PeerGroupKey{ .group_id = group_id, .peer_id = id };

        lock(&self.registration_mutex);
        const removed = self.registrations.fetchRemove(key);
        if (removed == null) {
            self.registration_mutex.unlock();
            return;
        }
        self.allocator.free(removed.?.value);
        _ = self.snapshot_pending.remove(key);
        var has_reference = false;
        var iterator = self.registrations.keyIterator();
        while (iterator.next()) |registered| {
            if (registered.peer_id == id) {
                has_reference = true;
                break;
            }
        }
        self.registration_mutex.unlock();
        if (!has_reference) try self.peer_manager.removePeer(id);
    }

    fn sendImpl(context: *anyopaque, envelopes: []const Envelope) Error!void {
        const self: *GrpcLiteMultiTransport = @ptrCast(@alignCast(context));
        lock(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        if (self.state != .started) return error.ConnectionClosed;
        for (envelopes) |envelope| {
            const message = envelope.message;
            if (envelope.group_id == 0) return error.InvalidGroupId;
            if (message.to == 0 or message.to == self.config.identity.node_id) return error.MessageDestinationMismatch;
            if (raw_node.isLocalMessage(message.msg_type)) return error.LocalMessageOnTransport;
            if (!self.validOutboundSource(envelope.group_id, message)) return error.MessageSourceMismatch;
            if (!self.isRegistered(envelope.group_id, message.to)) return error.ConnectionClosed;

            const key = PeerGroupKey{ .group_id = envelope.group_id, .peer_id = message.to };
            const snapshot = message.msg_type == .snapshot;
            if (snapshot) {
                lock(&self.registration_mutex);
                self.snapshot_pending.put(key, {}) catch |err| {
                    self.registration_mutex.unlock();
                    return err;
                };
                self.registration_mutex.unlock();
            }
            const payload = codec.encodeEnvelope(self.allocator, envelope) catch |err| {
                if (snapshot) self.clearSnapshotPending(key);
                return mapCodecError(err);
            };
            defer self.allocator.free(payload);
            self.peer_manager.send(message.to, payload, false) catch |err| {
                if (snapshot) self.clearSnapshotPending(key);
                return err;
            };
        }
    }

    fn setEnvelopeCallbackImpl(context: *anyopaque, callback: ?EnvelopeCallback) void {
        const self: *GrpcLiteMultiTransport = @ptrCast(@alignCast(context));
        self.callback = callback;
    }

    fn setPeerEventCallbackImpl(context: *anyopaque, callback: ?MultiPeerEventCallback) void {
        const self: *GrpcLiteMultiTransport = @ptrCast(@alignCast(context));
        self.peer_event_callback = callback;
    }

    fn pollOneImpl(context: *anyopaque) Error!bool {
        const self: *GrpcLiteMultiTransport = @ptrCast(@alignCast(context));
        return self.pollOne();
    }

    fn identityImpl(context: *anyopaque) TransportIdentity {
        const self: *GrpcLiteMultiTransport = @ptrCast(@alignCast(context));
        return self.config.identity;
    }

    pub const vtable: MultiTransport.VTable = .{
        .start = startImpl,
        .stop = stopImpl,
        .add_peer = addPeerImpl,
        .remove_peer = removePeerImpl,
        .send = sendImpl,
        .set_envelope_callback = setEnvelopeCallbackImpl,
        .set_peer_event_callback = setPeerEventCallbackImpl,
        .poll_one = pollOneImpl,
        .identity = identityImpl,
    };

    pub fn transport(self: *GrpcLiteMultiTransport) MultiTransport {
        return .{ .ctx = self, .vtable = &vtable };
    }

    fn isRegistered(self: *GrpcLiteMultiTransport, group_id: GroupId, peer_id: u64) bool {
        lock(&self.registration_mutex);
        defer self.registration_mutex.unlock();
        return self.registrations.contains(.{ .group_id = group_id, .peer_id = peer_id });
    }

    fn validOutboundSource(self: *GrpcLiteMultiTransport, group_id: GroupId, message: @import("../core/types.zig").Message) bool {
        if (message.msg_type != .transfer_leader) return message.from == self.config.identity.node_id;
        return message.from != 0 and
            (message.from == self.config.identity.node_id or self.isRegistered(group_id, message.from));
    }

    fn clearSnapshotPending(self: *GrpcLiteMultiTransport, key: PeerGroupKey) void {
        lock(&self.registration_mutex);
        _ = self.snapshot_pending.remove(key);
        self.registration_mutex.unlock();
    }
};

fn onStreamStart(
    context: ?*anyopaque,
    stream: grpc.ServerStream,
    server_context: *grpc.ServerContext,
) !void {
    const self: *GrpcLiteMultiTransport = @ptrCast(@alignCast(context.?));
    const identity = validateInboundIdentity(self, &server_context.request_metadata) catch |err| {
        finishIdentityError(stream, err);
        return;
    };
    var source_bytes: [8]u8 = undefined;
    var target_bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &source_bytes, self.config.identity.node_id, .little);
    std.mem.writeInt(u64, &target_bytes, identity.source_node, .little);
    try server_context.addInitialMetadata(peer_manager.protocol_version_key, "1");
    try server_context.addInitialMetadata(peer_manager.cluster_id_key, &self.config.identity.cluster_id);
    try server_context.addInitialMetadata(peer_manager.source_node_key, &source_bytes);
    try server_context.addInitialMetadata(peer_manager.target_node_key, &target_bytes);
}

fn onStreamMessage(
    context: ?*anyopaque,
    stream: grpc.ServerStream,
    server_context: *grpc.ServerContext,
    payload: []const u8,
    _: grpc.Compression,
) !grpc.StreamReceiveAction {
    const self: *GrpcLiteMultiTransport = @ptrCast(@alignCast(context.?));
    if (!self.accepting.load(.acquire)) return error.TransportStopping;
    const identity = validateInboundIdentity(self, &server_context.request_metadata) catch |err| {
        finishIdentityError(stream, err);
        return .continue_receiving;
    };
    return receiveStreamEnvelope(self, identity.source_node, payload);
}

fn receiveStreamEnvelope(
    self: *GrpcLiteMultiTransport,
    source_node: u64,
    payload: []const u8,
) !grpc.StreamReceiveAction {
    if (!self.mailbox.canAccept(payload.len)) return error.InboundMailboxFull;
    var envelope = codec.decodeEnvelope(self.allocator, payload) catch return error.InvalidRaftMessage;
    if (!validRoute(self, source_node, envelope)) {
        envelope.deinit(self.allocator);
        return error.InvalidRaftMessageRoute;
    }
    if (envelope.message.msg_type == .append_response) {
        self.clearSnapshotPending(.{ .group_id = envelope.group_id, .peer_id = source_node });
    }
    self.mailbox.push(envelope, payload.len) catch |err| {
        envelope.deinit(self.allocator);
        return if (err == error.TransportBackpressure) error.InboundMailboxFull else error.InboundMailboxFailed;
    };
    return .continue_receiving;
}

fn onStreamRemoteEnd(
    context: ?*anyopaque,
    stream: grpc.ServerStream,
    server_context: *grpc.ServerContext,
) !void {
    const self: *GrpcLiteMultiTransport = @ptrCast(@alignCast(context.?));
    _ = validateInboundIdentity(self, &server_context.request_metadata) catch |err| {
        finishIdentityError(stream, err);
        return;
    };
    tryFinish(stream, grpc.Status.ok);
}

fn onStreamCancel(_: ?*anyopaque, _: grpc.ServerStream, _: *grpc.ServerContext) void {}

fn validateInboundIdentity(
    self: *GrpcLiteMultiTransport,
    metadata: *const grpc.Metadata,
) !peer_manager.StreamIdentity {
    const identity = peer_manager.parseStreamIdentity(metadata) catch return error.InvalidIdentityMetadata;
    if (!self.accepting.load(.acquire) or
        !std.mem.eql(u8, &identity.cluster_id, &self.config.identity.cluster_id) or
        identity.target_node != self.config.identity.node_id or
        !self.peer_manager.hasPeer(identity.source_node))
    {
        return error.IdentityRejected;
    }
    return identity;
}

fn validRoute(self: *GrpcLiteMultiTransport, source: u64, envelope: Envelope) bool {
    const message = envelope.message;
    if (envelope.group_id == 0) return false;
    if (message.to != self.config.identity.node_id or raw_node.isLocalMessage(message.msg_type)) return false;
    if (message.msg_type != .transfer_leader) return message.from == source;
    return message.from != 0 and
        (message.from == self.config.identity.node_id or self.isRegistered(envelope.group_id, message.from));
}

fn queuePhysicalPeerEvent(context: *anyopaque, event: transport_mod.PeerEvent, generation: u64) void {
    const self: *GrpcLiteMultiTransport = @ptrCast(@alignCast(context));
    if (!self.accepting.load(.acquire)) return;
    lock(&self.registration_mutex);
    defer self.registration_mutex.unlock();
    var iterator = self.registrations.keyIterator();
    while (iterator.next()) |key| {
        if (key.peer_id != event.peer_id) continue;
        self.peer_events.push(.{
            .group_id = key.group_id,
            .peer_id = key.peer_id,
            .kind = event.kind,
        }, generation);
        if (event.kind == .@"unreachable" and self.snapshot_pending.remove(key.*)) {
            self.peer_events.push(.{
                .group_id = key.group_id,
                .peer_id = key.peer_id,
                .kind = .snapshot_failure,
            }, generation);
        }
    }
}

const StopAction = enum { done, wait, stop };

fn beginStop(self: *GrpcLiteMultiTransport) StopAction {
    lock(&self.lifecycle_mutex);
    defer self.lifecycle_mutex.unlock();
    return switch (self.state) {
        .stopped => .done,
        .stopping => .wait,
        .initialized, .starting, .started => action: {
            self.state = .stopping;
            self.accepting.store(false, .release);
            break :action .stop;
        },
    };
}

fn finishIdentityError(stream: grpc.ServerStream, err: anyerror) void {
    tryFinish(stream, if (err == error.InvalidIdentityMetadata)
        .init(.invalid_argument, "invalid multi-raft stream identity")
    else
        .init(.failed_precondition, "multi-raft stream identity rejected"));
}

fn tryFinish(stream: grpc.ServerStream, status: grpc.Status) void {
    stream.finish(status) catch {};
}

const ParsedAddress = struct { host: []const u8, port: u16 };

fn parseAddress(address: []const u8) !ParsedAddress {
    const colon = std.mem.lastIndexOfScalar(u8, address, ':') orelse return error.AddressPortMissing;
    if (colon == 0 or colon + 1 == address.len) return error.AddressPortInvalid;
    const port = std.fmt.parseInt(u16, address[colon + 1 ..], 10) catch return error.AddressPortInvalid;
    return .{ .host = address[0..colon], .port = port };
}

fn mapStartError(err: anyerror) Error {
    return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        error.BindFailed => error.BindFailed,
        error.ListenFailed => error.ListenFailed,
        else => error.ConnectionClosed,
    };
}

fn mapCodecError(err: anyerror) Error {
    return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        error.MessageTooLarge => error.MessageTooLarge,
        else => error.PayloadParseFailed,
    };
}

fn lock(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

// KCOV_EXCL_START
fn validTestConfig() Config {
    return .{
        .identity = .{ .cluster_id = [_]u8{1} ** 16, .node_id = 1 },
        .listen_addr = "127.0.0.1:0",
    };
}

fn checkCreateAllocationFailures(allocator: std.mem.Allocator) !void {
    const transport = try GrpcLiteMultiTransport.create(allocator, validTestConfig());
    defer transport.destroy();
    _ = try transport.transport().addPeer(1, 2, "127.0.0.1:9002");
    _ = try transport.transport().addPeer(2, 2, "127.0.0.1:9002");
}

test "grpc multi transport validates configuration" {
    var config = validTestConfig();
    config.identity.node_id = 0;
    try std.testing.expectError(error.InvalidNodeId, config.validate());
    config = validTestConfig();
    config.identity.cluster_id = .{0} ** 16;
    try std.testing.expectError(error.ClusterIdRequired, config.validate());
    config = validTestConfig();
    config.listen_addr = "";
    try std.testing.expectError(error.ListenAddressEmpty, config.validate());
    config = validTestConfig();
    config.mailbox_max_messages = 0;
    try std.testing.expectError(error.InvalidConfig, config.validate());
}

test "grpc multi transport reference-counts physical peers" {
    const transport = try GrpcLiteMultiTransport.create(std.testing.allocator, validTestConfig());
    defer transport.destroy();
    try std.testing.expect(try transport.transport().addPeer(1, 2, "127.0.0.1:9002"));
    try std.testing.expect(!(try transport.transport().addPeer(2, 2, "127.0.0.1:9002")));
    try std.testing.expectEqual(@as(usize, 1), transport.peerCount());
    try transport.transport().removePeer(1, 2);
    try std.testing.expectEqual(@as(usize, 1), transport.peerCount());
    try transport.transport().removePeer(2, 2);
    try std.testing.expectEqual(@as(usize, 0), transport.peerCount());
}

test "grpc multi transport create unwinds allocation failures" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        checkCreateAllocationFailures,
        .{},
    );
}
// KCOV_EXCL_STOP
