//! Cooperative single-threaded host for multiple Raftor groups.

const std = @import("std");
const linux = std.os.linux;

const error_model = @import("core/error.zig");
const types = @import("core/types.zig");
const storage_mod = @import("storage.zig");
const fs_mod = @import("fs.zig");
const raftor_mod = @import("raftor.zig");
const raftor_config_mod = @import("raftor_config.zig");
const state_machine_mod = @import("state_machine.zig");
const transport_mod = @import("transport.zig");
const multi_transport_mod = @import("multi_transport.zig");
const proposal_tracker_mod = @import("proposal_tracker.zig");

const Error = error_model.Error;
const Message = types.Message;
const StateMachine = state_machine_mod.StateMachine;
const Transport = transport_mod.Transport;
const MessageCallback = transport_mod.MessageCallback;
const PeerEvent = transport_mod.PeerEvent;
const PeerEventCallback = transport_mod.PeerEventCallback;
const Raftor = raftor_mod.Raftor;
const RaftorConfig = raftor_config_mod.RaftorConfig;
const MultiTransport = multi_transport_mod.MultiTransport;
const Envelope = multi_transport_mod.Envelope;
const MultiPeerEvent = multi_transport_mod.PeerEvent;
const GroupId = multi_transport_mod.GroupId;

const log = @import("grpc_lite").log;

fn spinLock(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

fn currentThreadId() usize {
    return @intCast(std.Thread.getCurrentId());
}

fn sleepNanoseconds(nanoseconds: u64) void {
    var request = linux.timespec{
        .sec = std.math.cast(isize, nanoseconds / std.time.ns_per_s) orelse std.math.maxInt(isize),
        .nsec = @intCast(nanoseconds % std.time.ns_per_s),
    };
    var remaining: linux.timespec = undefined;
    while (true) {
        const rc = linux.nanosleep(&request, &remaining);
        switch (linux.errno(rc)) {
            .SUCCESS => return,
            .INTR => request = remaining,
            else => return,
        }
    }
}

pub const MultiRaftConfig = struct {
    node_id: u64,
    /// Root directory. Each group uses `<data_dir>/groups/<group_id>`.
    /// Empty selects one independent MemoryStorage per group.
    data_dir: []const u8 = "",
    file_system: ?fs_mod.Fs = null,
    tick_interval_ms: u64 = 100,
    transport_poll_budget: usize = 1024,
    group_drive_budget: usize = 64,
    max_groups: usize = 1024,
    max_group_inbox_messages: usize = 4096,

    pub fn validate(self: MultiRaftConfig) Error!void {
        if (self.node_id == 0) return error.InvalidNodeId;
        if (self.tick_interval_ms == 0 or self.transport_poll_budget == 0) return error.InvalidConfig;
        if (self.group_drive_budget == 0 or self.max_groups == 0) return error.InvalidConfig;
        if (self.max_group_inbox_messages == 0) return error.InvalidConfig;
    }
};

pub const MultiRaftGroupConfig = struct {
    group_id: GroupId,
    raftor: RaftorConfig,
};

pub const GroupLifecycle = enum {
    active,
    retryable_error,
    terminal,
    stopping,
};

pub const MultiRaftGroupStatus = struct {
    group_id: GroupId,
    lifecycle: GroupLifecycle,
    node: raftor_mod.NodeStatus,
    last_error: ?Error,
};

const GroupTransport = struct {
    group_id: GroupId,
    shared: MultiTransport,
    message_callback: ?MessageCallback = null,
    peer_event_callback: ?PeerEventCallback = null,
    inbox: std.ArrayList(Message) = .empty,
    peer_events: std.ArrayList(PeerEvent) = .empty,
    peers: std.AutoHashMap(u64, void),
    max_inbox_messages: usize,
    started: bool = false,
    stopped: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    allocator: std.mem.Allocator,

    fn init(
        allocator: std.mem.Allocator,
        group_id: GroupId,
        shared: MultiTransport,
        max_inbox_messages: usize,
    ) GroupTransport {
        return .{
            .group_id = group_id,
            .shared = shared,
            .peers = std.AutoHashMap(u64, void).init(allocator),
            .max_inbox_messages = max_inbox_messages,
            .allocator = allocator,
        };
    }

    fn deinit(self: *GroupTransport) void {
        self.releasePeers();
        for (self.inbox.items) |*message| message.deinit(self.allocator);
        self.inbox.deinit(self.allocator);
        self.peer_events.deinit(self.allocator);
        self.peers.deinit();
        self.* = undefined;
    }

    fn enqueueMessage(self: *GroupTransport, message: Message) Error!void {
        const stopped = self.stopped.load(.acquire);
        if (stopped or self.inbox.items.len >= self.max_inbox_messages) {
            var owned = message;
            owned.deinit(self.allocator);
            return if (stopped) error.ShuttingDown else error.TransportBackpressure;
        }
        self.inbox.append(self.allocator, message) catch |err| {
            var owned = message;
            owned.deinit(self.allocator);
            return err;
        };
    }

    fn enqueuePeerEvent(self: *GroupTransport, event: PeerEvent) Error!void {
        const stopped = self.stopped.load(.acquire);
        if (stopped or self.peer_events.items.len >= self.max_inbox_messages) {
            return if (stopped) error.ShuttingDown else error.TransportBackpressure;
        }
        try self.peer_events.append(self.allocator, event);
    }

    fn releasePeers(self: *GroupTransport) void {
        var iterator = self.peers.keyIterator();
        while (iterator.next()) |peer_id| {
            self.shared.removePeer(self.group_id, peer_id.*) catch {};
        }
        self.peers.clearRetainingCapacity();
    }

    fn startImpl(ctx: *anyopaque) Error!void {
        const self: *GroupTransport = @ptrCast(@alignCast(ctx));
        if (self.stopped.load(.acquire) or self.started) return error.AlreadyStarted;
        self.started = true;
    }

    fn stopImpl(ctx: *anyopaque) void {
        const self: *GroupTransport = @ptrCast(@alignCast(ctx));
        self.stopped.store(true, .release);
    }

    fn addPeerImpl(ctx: *anyopaque, id: u64, address: []const u8) Error!bool {
        const self: *GroupTransport = @ptrCast(@alignCast(ctx));
        if (self.stopped.load(.acquire)) return error.ShuttingDown;
        if (self.peers.contains(id)) return false;
        try self.peers.ensureUnusedCapacity(1);
        const added = try self.shared.addPeer(self.group_id, id, address);
        self.peers.putAssumeCapacity(id, {});
        return added;
    }

    fn removePeerImpl(ctx: *anyopaque, id: u64) Error!void {
        const self: *GroupTransport = @ptrCast(@alignCast(ctx));
        if (!self.peers.contains(id)) return;
        try self.shared.removePeer(self.group_id, id);
        _ = self.peers.remove(id);
    }

    fn sendImpl(ctx: *anyopaque, messages: []const Message) Error!void {
        const self: *GroupTransport = @ptrCast(@alignCast(ctx));
        if (self.stopped.load(.acquire)) return error.ShuttingDown;
        for (messages) |message| {
            try self.shared.send(&.{.{ .group_id = self.group_id, .message = message }});
        }
    }

    fn setMessageCallbackImpl(ctx: *anyopaque, callback: ?MessageCallback) void {
        const self: *GroupTransport = @ptrCast(@alignCast(ctx));
        self.message_callback = callback;
    }

    fn setPeerEventCallbackImpl(ctx: *anyopaque, callback: ?PeerEventCallback) void {
        const self: *GroupTransport = @ptrCast(@alignCast(ctx));
        self.peer_event_callback = callback;
    }

    fn pollOneImpl(ctx: *anyopaque) Error!bool {
        const self: *GroupTransport = @ptrCast(@alignCast(ctx));
        if (self.stopped.load(.acquire)) return false;
        if (self.inbox.items.len != 0) {
            const callback = self.message_callback orelse return false;
            try callback.invoke(self.inbox.orderedRemove(0));
            return true;
        }
        if (self.peer_events.items.len != 0) {
            const callback = self.peer_event_callback orelse return false;
            try callback.invoke(self.peer_events.orderedRemove(0));
            return true;
        }
        return false;
    }

    fn identityImpl(ctx: *anyopaque) transport_mod.TransportIdentity {
        const self: *GroupTransport = @ptrCast(@alignCast(ctx));
        return self.shared.identity().?;
    }

    const vtable: Transport.VTable = .{
        .start = startImpl,
        .stop = stopImpl,
        .add_peer = addPeerImpl,
        .remove_peer = removePeerImpl,
        .send = sendImpl,
        .set_message_callback = setMessageCallbackImpl,
        .set_peer_event_callback = setPeerEventCallbackImpl,
        .poll_one = pollOneImpl,
    };

    const identity_vtable: Transport.VTable = .{
        .start = startImpl,
        .stop = stopImpl,
        .add_peer = addPeerImpl,
        .remove_peer = removePeerImpl,
        .send = sendImpl,
        .set_message_callback = setMessageCallbackImpl,
        .set_peer_event_callback = setPeerEventCallbackImpl,
        .poll_one = pollOneImpl,
        .identity = identityImpl,
    };

    fn transport(self: *GroupTransport) Transport {
        return .{
            .ctx = self,
            .vtable = if (self.shared.identity() == null) &vtable else &identity_vtable,
        };
    }
};

const Group = struct {
    id: GroupId,
    transport: GroupTransport,
    raftor: *Raftor,
    data_dir: ?[:0]u8,
    status_mutex: std.atomic.Mutex = .unlocked,
    lifecycle: GroupLifecycle = .active,
    last_error: ?Error = null,
    allocator: std.mem.Allocator,

    fn create(
        allocator: std.mem.Allocator,
        host_config: MultiRaftConfig,
        group_config: MultiRaftGroupConfig,
        shared_transport: MultiTransport,
        state_machine: StateMachine,
    ) Error!*Group {
        const self = try allocator.create(Group);
        errdefer allocator.destroy(self);

        var data_dir: ?[:0]u8 = null;
        errdefer if (data_dir) |path| allocator.free(path);
        if (host_config.data_dir.len != 0) {
            data_dir = try std.fmt.allocPrintSentinel(
                allocator,
                "{s}/groups/{}",
                .{ host_config.data_dir, group_config.group_id },
                0,
            );
        }

        self.* = .{
            .id = group_config.group_id,
            .transport = GroupTransport.init(
                allocator,
                group_config.group_id,
                shared_transport,
                host_config.max_group_inbox_messages,
            ),
            .raftor = undefined,
            .data_dir = data_dir,
            .allocator = allocator,
        };
        errdefer self.transport.deinit();

        var config = group_config.raftor;
        config.data_dir = if (data_dir) |path| path else "";
        config.file_system = host_config.file_system;
        self.raftor = try Raftor.createWithTransport(
            allocator,
            config,
            state_machine,
            self.transport.transport(),
        );
        return self;
    }

    fn destroy(self: *Group) void {
        self.raftor.destroy();
        self.transport.deinit();
        if (self.data_dir) |path| self.allocator.free(path);
        self.allocator.destroy(self);
    }

    fn markActive(self: *Group) void {
        spinLock(&self.status_mutex);
        defer self.status_mutex.unlock();
        if (self.lifecycle == .stopping) return;
        self.lifecycle = .active;
        self.last_error = null;
    }

    fn markRetryable(self: *Group, err: Error) void {
        spinLock(&self.status_mutex);
        defer self.status_mutex.unlock();
        if (self.lifecycle == .stopping) return;
        self.lifecycle = .retryable_error;
        self.last_error = err;
    }

    fn markTerminal(self: *Group, err: Error) void {
        spinLock(&self.status_mutex);
        defer self.status_mutex.unlock();
        if (self.lifecycle == .stopping) return;
        self.lifecycle = .terminal;
        self.last_error = err;
    }

    fn markStopping(self: *Group) void {
        spinLock(&self.status_mutex);
        defer self.status_mutex.unlock();
        self.lifecycle = .stopping;
    }

    fn lifecycleSnapshot(self: *Group) struct { lifecycle: GroupLifecycle, last_error: ?Error } {
        spinLock(&self.status_mutex);
        defer self.status_mutex.unlock();
        return .{ .lifecycle = self.lifecycle, .last_error = self.last_error };
    }
};

pub const MultiRaftHost = struct {
    allocator: std.mem.Allocator,
    config: MultiRaftConfig,
    transport: MultiTransport,
    groups: std.AutoHashMap(GroupId, *Group),
    group_ids: std.ArrayList(GroupId) = .empty,
    lifecycle_mutex: std.atomic.Mutex = .unlocked,
    event_loop_active: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    event_loop_thread_id: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    run_active: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    stop_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    stopped: bool = false,
    round_robin_cursor: usize = 0,
    unknown_group_messages: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    pub fn create(
        allocator: std.mem.Allocator,
        config: MultiRaftConfig,
        transport: MultiTransport,
    ) Error!*MultiRaftHost {
        try config.validate();
        if (transport.identity()) |identity| {
            if (identity.node_id != config.node_id) return error.TransportIdentityMismatch;
        }
        if (config.data_dir.len != 0) try ensureGroupRoot(allocator, config);
        const self = try allocator.create(MultiRaftHost);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .config = config,
            .transport = transport,
            .groups = std.AutoHashMap(GroupId, *Group).init(allocator),
        };
        errdefer self.groups.deinit();

        self.transport.setEnvelopeCallback(.{ .ctx = self, .function = onEnvelope });
        self.transport.setPeerEventCallback(.{ .ctx = self, .function = onPeerEvent });
        self.transport.start() catch |err| {
            self.transport.setEnvelopeCallback(null);
            self.transport.setPeerEventCallback(null);
            self.transport.stop();
            return err;
        };
        return self;
    }

    pub fn destroy(self: *MultiRaftHost) void {
        if (self.event_loop_thread_id.load(.acquire) == currentThreadId()) {
            @panic("MultiRaftHost.destroy cannot be called from a group callback");
        }
        self.stop();
        while (self.run_active.load(.acquire) or self.event_loop_active.load(.acquire)) {
            sleepNanoseconds(std.time.ns_per_ms);
        }

        var iterator = self.groups.valueIterator();
        while (iterator.next()) |group| group.*.destroy();
        self.groups.deinit();
        self.group_ids.deinit(self.allocator);
        self.transport.setEnvelopeCallback(null);
        self.transport.setPeerEventCallback(null);
        self.allocator.destroy(self);
    }

    pub fn addGroup(
        self: *MultiRaftHost,
        config: MultiRaftGroupConfig,
        state_machine: StateMachine,
    ) Error!void {
        try self.enterManagement();
        defer self.leaveManagement();
        if (config.group_id == 0) return error.InvalidGroupId;
        if (config.raftor.nodeId() != self.config.node_id) return error.InvalidNodeId;
        if (config.raftor.data_dir.len != 0 or config.raftor.file_system != null) return error.InvalidConfig;
        if (self.groups.contains(config.group_id)) return error.GroupAlreadyExists;
        if (self.groups.count() >= self.config.max_groups) return error.GroupLimitReached;
        try self.groups.ensureUnusedCapacity(1);
        try self.group_ids.ensureUnusedCapacity(self.allocator, 1);

        const group = try Group.create(
            self.allocator,
            self.config,
            config,
            self.transport,
            state_machine,
        );
        self.groups.putAssumeCapacity(config.group_id, group);
        self.group_ids.appendAssumeCapacity(config.group_id);
        std.mem.sort(GroupId, self.group_ids.items, {}, std.sort.asc(GroupId));
    }

    pub fn removeGroup(self: *MultiRaftHost, group_id: GroupId) Error!void {
        try self.enterManagement();
        defer self.leaveManagement();
        const removed = self.groups.fetchRemove(group_id) orelse return error.GroupNotFound;
        for (self.group_ids.items, 0..) |id, index| {
            if (id == group_id) {
                _ = self.group_ids.orderedRemove(index);
                break;
            }
        }
        removed.value.destroy();
    }

    pub fn tick(self: *MultiRaftHost) Error!bool {
        try self.enterEventLoop();
        defer self.leaveEventLoop();
        return self.drive(true);
    }

    pub fn poll(self: *MultiRaftHost) Error!bool {
        try self.enterEventLoop();
        defer self.leaveEventLoop();
        return self.drive(false);
    }

    pub fn run(self: *MultiRaftHost) Error!void {
        if (self.run_active.cmpxchgStrong(false, true, .acquire, .monotonic) != null) {
            return error.AlreadyStarted;
        }
        defer self.run_active.store(false, .release);
        while (!self.stop_requested.load(.acquire)) {
            _ = self.tick() catch |err| {
                log.warn(@src(), "MultiRaftHost tick failed: {s}", .{@errorName(err)});
            };
            if (self.stop_requested.load(.acquire)) break;
            sleepNanoseconds(self.config.tick_interval_ms *| std.time.ns_per_ms);
        }
    }

    pub fn isRunning(self: *const MultiRaftHost) bool {
        return self.run_active.load(.acquire);
    }

    pub fn stop(self: *MultiRaftHost) void {
        spinLock(&self.lifecycle_mutex);
        if (self.stopped) {
            self.lifecycle_mutex.unlock();
            return;
        }
        self.stopped = true;
        self.stop_requested.store(true, .release);
        self.lifecycle_mutex.unlock();

        if (self.event_loop_thread_id.load(.acquire) != currentThreadId()) {
            while (self.event_loop_active.load(.acquire)) sleepNanoseconds(std.time.ns_per_ms);
        }
        var iterator = self.groups.valueIterator();
        while (iterator.next()) |group| {
            group.*.markStopping();
            group.*.raftor.stop();
        }
        self.transport.stop();
    }

    pub fn propose(
        self: *MultiRaftHost,
        group_id: GroupId,
        data: []const u8,
        callback: proposal_tracker_mod.ProposalCallback,
    ) Error!void {
        const group = self.groups.get(group_id) orelse return error.GroupNotFound;
        const status = group.lifecycleSnapshot();
        if (status.lifecycle == .terminal) return status.last_error.?;
        if (status.lifecycle == .stopping) return error.ShuttingDown;
        return group.raftor.propose(data, callback);
    }

    pub fn readIndex(
        self: *MultiRaftHost,
        group_id: GroupId,
        context: []const u8,
        callback: proposal_tracker_mod.ReadIndexCallback,
    ) Error!void {
        const group = self.groups.get(group_id) orelse return error.GroupNotFound;
        const status = group.lifecycleSnapshot();
        if (status.lifecycle == .terminal) return status.last_error.?;
        if (status.lifecycle == .stopping) return error.ShuttingDown;
        return group.raftor.readIndex(context, callback);
    }

    pub fn campaign(self: *MultiRaftHost, group_id: GroupId) Error!void {
        if (self.run_active.load(.acquire)) return error.EventLoopBusy;
        try self.enterEventLoop();
        defer self.leaveEventLoop();
        const group = self.groups.get(group_id) orelse return error.GroupNotFound;
        try group.raftor.campaign();
    }

    pub fn getStatus(self: *const MultiRaftHost, group_id: GroupId) ?MultiRaftGroupStatus {
        const group = self.groups.get(group_id) orelse return null;
        const status = group.lifecycleSnapshot();
        return .{
            .group_id = group_id,
            .lifecycle = status.lifecycle,
            .node = group.raftor.getStatus(),
            .last_error = status.last_error,
        };
    }

    pub fn getRaftor(self: *MultiRaftHost, group_id: GroupId) ?*Raftor {
        const group = self.groups.get(group_id) orelse return null;
        return group.raftor;
    }

    pub fn groupCount(self: *const MultiRaftHost) usize {
        return self.groups.count();
    }

    pub fn unknownGroupMessageCount(self: *const MultiRaftHost) usize {
        return self.unknown_group_messages.load(.acquire);
    }

    fn drive(self: *MultiRaftHost, advance_clock: bool) Error!bool {
        var had_work = false;
        for (0..self.config.transport_poll_budget) |_| {
            if (!try self.transport.pollOne()) break;
            had_work = true;
        }

        var first_error: ?Error = null;
        const drive_count = @min(self.config.group_drive_budget, self.group_ids.items.len);
        for (0..drive_count) |offset| {
            const index = (self.round_robin_cursor + offset) % self.group_ids.items.len;
            const group = self.groups.get(self.group_ids.items[index]).?;
            const status = group.lifecycleSnapshot();
            if (status.lifecycle == .terminal or status.lifecycle == .stopping) continue;
            const worked = if (advance_clock) group.raftor.tick() else group.raftor.poll();
            if (worked) |value| {
                had_work = had_work or value;
                group.markActive();
            } else |err| {
                if (group.raftor.getTerminalError()) |terminal| {
                    group.markTerminal(terminal);
                } else {
                    group.markRetryable(err);
                    if (first_error == null) first_error = err;
                }
            }
        }
        if (self.group_ids.items.len != 0) {
            self.round_robin_cursor = (self.round_robin_cursor + drive_count) % self.group_ids.items.len;
        }
        if (first_error) |err| return err;
        return had_work;
    }

    fn onEnvelope(ctx: *anyopaque, envelope: Envelope) Error!void {
        const self: *MultiRaftHost = @ptrCast(@alignCast(ctx));
        const group = self.groups.get(envelope.group_id) orelse {
            var owned = envelope;
            owned.deinit(self.allocator);
            _ = self.unknown_group_messages.fetchAdd(1, .monotonic);
            return;
        };
        return group.transport.enqueueMessage(envelope.message);
    }

    fn onPeerEvent(ctx: *anyopaque, event: MultiPeerEvent) Error!void {
        const self: *MultiRaftHost = @ptrCast(@alignCast(ctx));
        const group = self.groups.get(event.group_id) orelse return;
        try group.transport.enqueuePeerEvent(.{ .peer_id = event.peer_id, .kind = event.kind });
    }

    fn ensureGroupRoot(allocator: std.mem.Allocator, config: MultiRaftConfig) Error!void {
        const fs = config.file_system orelse fs_mod.realFileSystem();
        const root = try allocator.dupeZ(u8, config.data_dir);
        defer allocator.free(root);
        _ = fs.makeDir(root) catch |err| return switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            else => error.WalCreateDirectoryFailed,
        };
        const groups = try std.fmt.allocPrintSentinel(allocator, "{s}/groups", .{config.data_dir}, 0);
        defer allocator.free(groups);
        _ = fs.makeDir(groups) catch |err| return switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            else => error.WalCreateDirectoryFailed,
        };
    }

    fn enterManagement(self: *MultiRaftHost) Error!void {
        if (self.run_active.load(.acquire)) return error.EventLoopBusy;
        return self.enterEventLoop();
    }

    fn leaveManagement(self: *MultiRaftHost) void {
        self.leaveEventLoop();
    }

    fn enterEventLoop(self: *MultiRaftHost) Error!void {
        spinLock(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        if (self.stopped) return error.ShuttingDown;
        if (self.event_loop_active.cmpxchgStrong(false, true, .acquire, .monotonic) != null) {
            return error.EventLoopBusy;
        }
        self.event_loop_thread_id.store(currentThreadId(), .release);
    }

    fn leaveEventLoop(self: *MultiRaftHost) void {
        self.event_loop_thread_id.store(0, .release);
        self.event_loop_active.store(false, .release);
    }
};
