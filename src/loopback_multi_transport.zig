//! Process-local shared-envelope transport for MultiRaftHost tests and embeds.

const std = @import("std");

const error_model = @import("core/error.zig");
const types = @import("core/types.zig");
const storage_mod = @import("storage.zig");
const multi_transport_mod = @import("multi_transport.zig");

const Error = error_model.Error;
const MessageType = types.MessageType;
const GroupId = multi_transport_mod.GroupId;
const Envelope = multi_transport_mod.Envelope;
const EnvelopeCallback = multi_transport_mod.EnvelopeCallback;
const PeerEventCallback = multi_transport_mod.PeerEventCallback;
const MultiTransport = multi_transport_mod.MultiTransport;

pub const LoopbackMultiNetwork = struct {
    nodes: std.AutoHashMap(u64, *LoopbackMultiTransport),
    allocator: std.mem.Allocator,
    drop_filter: ?*const fn (group_id: GroupId, from: u64, to: u64, message_type: MessageType) bool = null,

    pub fn create(allocator: std.mem.Allocator) !*LoopbackMultiNetwork {
        const self = try allocator.create(LoopbackMultiNetwork);
        self.* = .{
            .nodes = std.AutoHashMap(u64, *LoopbackMultiTransport).init(allocator),
            .allocator = allocator,
        };
        return self;
    }

    pub fn destroy(self: *LoopbackMultiNetwork) void {
        var iterator = self.nodes.valueIterator();
        while (iterator.next()) |transport| {
            transport.*.deinit();
            self.allocator.destroy(transport.*);
        }
        self.nodes.deinit();
        self.allocator.destroy(self);
    }

    pub fn createTransport(self: *LoopbackMultiNetwork, node_id: u64) !*LoopbackMultiTransport {
        if (node_id == 0) return error.InvalidNodeId;
        if (self.nodes.contains(node_id)) return error.DuplicatePeer;
        const transport = try self.allocator.create(LoopbackMultiTransport);
        errdefer self.allocator.destroy(transport);
        transport.* = LoopbackMultiTransport.init(self.allocator, self, node_id);
        try self.nodes.put(node_id, transport);
        return transport;
    }

    pub fn getTransport(self: *LoopbackMultiNetwork, node_id: u64) ?*LoopbackMultiTransport {
        return self.nodes.get(node_id);
    }

    fn route(self: *LoopbackMultiNetwork, envelope: Envelope) Error!void {
        if (self.drop_filter) |filter| {
            const message = envelope.message;
            if (filter(envelope.group_id, message.from, message.to, message.msg_type)) {
                var owned = envelope;
                owned.deinit(self.allocator);
                return;
            }
        }
        const target = self.nodes.get(envelope.message.to) orelse {
            var owned = envelope;
            owned.deinit(self.allocator);
            return;
        };
        target.inbox.append(self.allocator, envelope) catch |err| {
            var owned = envelope;
            owned.deinit(self.allocator);
            return err;
        };
    }

    pub fn pollAll(self: *LoopbackMultiNetwork) Error!bool {
        var had_work = false;
        var iterator = self.nodes.valueIterator();
        while (iterator.next()) |transport| {
            if (try transport.*.pollOne()) had_work = true;
        }
        return had_work;
    }
};

pub const LoopbackMultiTransport = struct {
    network: *LoopbackMultiNetwork,
    node_id: u64,
    inbox: std.ArrayList(Envelope) = .empty,
    callback: ?EnvelopeCallback = null,
    peer_event_callback: ?PeerEventCallback = null,
    stopped: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    allocator: std.mem.Allocator,

    pub fn init(
        allocator: std.mem.Allocator,
        network: *LoopbackMultiNetwork,
        node_id: u64,
    ) LoopbackMultiTransport {
        return .{
            .network = network,
            .node_id = node_id,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *LoopbackMultiTransport) void {
        for (self.inbox.items) |*envelope| envelope.deinit(self.allocator);
        self.inbox.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn pollOne(self: *LoopbackMultiTransport) Error!bool {
        if (self.stopped.load(.acquire) or self.inbox.items.len == 0) return false;
        const callback = self.callback orelse return false;
        try callback.invoke(self.inbox.orderedRemove(0));
        return true;
    }

    fn startImpl(ctx: *anyopaque) Error!void {
        const self: *LoopbackMultiTransport = @ptrCast(@alignCast(ctx));
        if (self.stopped.load(.acquire)) return error.AlreadyStarted;
    }

    fn stopImpl(ctx: *anyopaque) void {
        const self: *LoopbackMultiTransport = @ptrCast(@alignCast(ctx));
        self.stopped.store(true, .release);
    }

    fn addPeerImpl(_: *anyopaque, _: GroupId, _: u64, _: []const u8) Error!bool {
        return true;
    }

    fn removePeerImpl(_: *anyopaque, _: GroupId, _: u64) Error!void {}

    fn sendImpl(ctx: *anyopaque, envelopes: []const Envelope) Error!void {
        const self: *LoopbackMultiTransport = @ptrCast(@alignCast(ctx));
        if (self.stopped.load(.acquire)) return error.ShuttingDown;
        for (envelopes) |envelope| {
            if (envelope.group_id == 0) return error.InvalidGroupId;
            const cloned_message = try storage_mod.shareMessage(self.allocator, envelope.message);
            try self.network.route(.{ .group_id = envelope.group_id, .message = cloned_message });
        }
    }

    fn setEnvelopeCallbackImpl(ctx: *anyopaque, callback: ?EnvelopeCallback) void {
        const self: *LoopbackMultiTransport = @ptrCast(@alignCast(ctx));
        self.callback = callback;
    }

    fn setPeerEventCallbackImpl(ctx: *anyopaque, callback: ?PeerEventCallback) void {
        const self: *LoopbackMultiTransport = @ptrCast(@alignCast(ctx));
        self.peer_event_callback = callback;
    }

    fn pollOneImpl(ctx: *anyopaque) Error!bool {
        const self: *LoopbackMultiTransport = @ptrCast(@alignCast(ctx));
        return self.pollOne();
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
    };

    pub fn transport(self: *LoopbackMultiTransport) MultiTransport {
        return .{ .ctx = self, .vtable = &vtable };
    }
};

// KCOV_EXCL_START
test "loopback multi transport preserves group identity" {
    const allocator = std.testing.allocator;
    const network = try LoopbackMultiNetwork.create(allocator);
    defer network.destroy();
    const source = try network.createTransport(1);
    const target = try network.createTransport(2);

    var received_group: GroupId = 0;
    const Callback = struct {
        group_id: *GroupId,
        allocator: std.mem.Allocator,

        fn invoke(ctx: *anyopaque, envelope: Envelope) Error!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.group_id.* = envelope.group_id;
            var owned = envelope;
            owned.deinit(self.allocator);
        }
    };
    var callback = Callback{ .group_id = &received_group, .allocator = allocator };
    target.transport().setEnvelopeCallback(.{ .ctx = &callback, .function = Callback.invoke });

    try source.transport().send(&.{.{
        .group_id = 42,
        .message = .{ .msg_type = .heartbeat, .from = 1, .to = 2 },
    }});
    try std.testing.expect(try target.pollOne());
    try std.testing.expectEqual(@as(GroupId, 42), received_group);
}
// KCOV_EXCL_STOP
