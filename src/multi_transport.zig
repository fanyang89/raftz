//! Shared transport boundary for hosting multiple Raft groups per node.

const std = @import("std");

const error_model = @import("core/error.zig");
const types = @import("core/types.zig");
const transport_mod = @import("transport.zig");

const Error = error_model.Error;
const Message = types.Message;

pub const GroupId = u64;
pub const TransportIdentity = transport_mod.TransportIdentity;
pub const PeerEventKind = transport_mod.PeerEventKind;

pub const Envelope = struct {
    group_id: GroupId,
    message: Message,

    pub fn deinit(self: *Envelope, allocator: std.mem.Allocator) void {
        self.message.deinit(allocator);
        self.* = undefined;
    }
};

pub const PeerEvent = struct {
    group_id: GroupId,
    peer_id: u64,
    kind: PeerEventKind,
};

/// Ownership of the envelope transfers when the callback is invoked.
pub const EnvelopeCallback = struct {
    ctx: *anyopaque,
    function: *const fn (ctx: *anyopaque, envelope: Envelope) Error!void,

    pub fn invoke(self: EnvelopeCallback, envelope: Envelope) Error!void {
        return self.function(self.ctx, envelope);
    }
};

pub const PeerEventCallback = struct {
    ctx: *anyopaque,
    function: *const fn (ctx: *anyopaque, event: PeerEvent) Error!void,

    pub fn invoke(self: PeerEventCallback, event: PeerEvent) Error!void {
        return self.function(self.ctx, event);
    }
};

/// Node-level transport shared by every local Raft group.
///
/// Peer registration is group-aware so implementations can retain one physical
/// connection until the final local group releases that peer.
pub const MultiTransport = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        start: *const fn (ctx: *anyopaque) Error!void,
        stop: *const fn (ctx: *anyopaque) void,
        add_peer: *const fn (ctx: *anyopaque, group_id: GroupId, id: u64, addr: []const u8) Error!bool,
        remove_peer: *const fn (ctx: *anyopaque, group_id: GroupId, id: u64) Error!void,
        /// Envelopes and nested messages are borrowed for this call.
        send: *const fn (ctx: *anyopaque, envelopes: []const Envelope) Error!void,
        set_envelope_callback: *const fn (ctx: *anyopaque, callback: ?EnvelopeCallback) void,
        set_peer_event_callback: *const fn (ctx: *anyopaque, callback: ?PeerEventCallback) void,
        poll_one: *const fn (ctx: *anyopaque) Error!bool,
        identity: ?*const fn (ctx: *anyopaque) TransportIdentity = null,
    };

    pub fn start(self: MultiTransport) Error!void {
        return self.vtable.start(self.ctx);
    }

    pub fn stop(self: MultiTransport) void {
        self.vtable.stop(self.ctx);
    }

    pub fn addPeer(self: MultiTransport, group_id: GroupId, id: u64, addr: []const u8) Error!bool {
        return self.vtable.add_peer(self.ctx, group_id, id, addr);
    }

    pub fn removePeer(self: MultiTransport, group_id: GroupId, id: u64) Error!void {
        return self.vtable.remove_peer(self.ctx, group_id, id);
    }

    pub fn send(self: MultiTransport, envelopes: []const Envelope) Error!void {
        return self.vtable.send(self.ctx, envelopes);
    }

    pub fn setEnvelopeCallback(self: MultiTransport, callback: ?EnvelopeCallback) void {
        self.vtable.set_envelope_callback(self.ctx, callback);
    }

    pub fn setPeerEventCallback(self: MultiTransport, callback: ?PeerEventCallback) void {
        self.vtable.set_peer_event_callback(self.ctx, callback);
    }

    pub fn pollOne(self: MultiTransport) Error!bool {
        return self.vtable.poll_one(self.ctx);
    }

    pub fn identity(self: MultiTransport) ?TransportIdentity {
        const get_identity = self.vtable.identity orelse return null;
        return get_identity(self.ctx);
    }
};
