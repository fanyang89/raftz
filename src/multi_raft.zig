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
const group_config_mod = @import("multi_raft_group_config.zig");
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
const OwnedGroupConfig = group_config_mod.OwnedGroupConfig;

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
    group_operation_budget: usize = 64,
    max_groups: usize = 1024,
    max_group_inbox_messages: usize = 4096,
    max_queued_group_operations: usize = 256,
    max_queued_group_operation_bytes: usize = 16 * 1024 * 1024,

    pub fn validate(self: MultiRaftConfig) Error!void {
        if (self.node_id == 0) return error.InvalidNodeId;
        if (self.tick_interval_ms == 0 or self.transport_poll_budget == 0) return error.InvalidConfig;
        if (self.group_drive_budget == 0 or self.group_operation_budget == 0) return error.InvalidConfig;
        if (self.max_groups == 0 or self.max_group_inbox_messages == 0) return error.InvalidConfig;
        if (self.max_queued_group_operations == 0 or self.max_queued_group_operation_bytes == 0) return error.InvalidConfig;
    }
};

pub const MultiRaftGroupConfig = group_config_mod.MultiRaftGroupConfig;

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
    pending_messages: usize,
    pending_peer_events: usize,
    scheduled_iterations: u64,
    productive_iterations: u64,
    error_iterations: u64,
    last_host_iteration: u64,
};

pub const MultiRaftHostStatus = struct {
    node_id: u64,
    groups: usize,
    active_groups: usize,
    retryable_groups: usize,
    terminal_groups: usize,
    stopping_groups: usize,
    queued_group_operations: GroupOperationQueueStats,
    unknown_group_messages: usize,
    host_iterations: u64,
    tick_iterations: u64,
    poll_iterations: u64,
    groups_driven: u64,
    groups_with_work: u64,
    group_error_iterations: u64,
    group_operations_completed: u64,
    group_operations_failed: u64,
    envelopes_routed: u64,
    peer_events_routed: u64,
};

pub const GroupOperationKind = enum {
    add,
    remove,
    restart,
};

pub const GroupOperationResult = struct {
    group_id: GroupId,
    operation: GroupOperationKind,
    err: ?Error = null,
};

pub const GroupOperationCallback = struct {
    ctx: *anyopaque,
    function: *const fn (ctx: *anyopaque, result: GroupOperationResult) void,

    pub fn invoke(self: GroupOperationCallback, result: GroupOperationResult) void {
        self.function(self.ctx, result);
    }
};

pub const GroupOperationQueueStats = struct {
    count: usize,
    bytes: usize,
};

const GroupOperationCommand = union(GroupOperationKind) {
    add: struct {
        config: OwnedGroupConfig,
        state_machine: StateMachine,
        callback: GroupOperationCallback,
    },
    remove: struct {
        group_id: GroupId,
        callback: GroupOperationCallback,
    },
    restart: struct {
        config: OwnedGroupConfig,
        state_machine: StateMachine,
        callback: GroupOperationCallback,
    },

    fn groupId(self: GroupOperationCommand) GroupId {
        return switch (self) {
            .add => |value| value.config.value.group_id,
            .remove => |value| value.group_id,
            .restart => |value| value.config.value.group_id,
        };
    }

    fn callback(self: GroupOperationCommand) GroupOperationCallback {
        return switch (self) {
            .add => |value| value.callback,
            .remove => |value| value.callback,
            .restart => |value| value.callback,
        };
    }

    fn deinit(self: *GroupOperationCommand, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .add => |*value| value.config.deinit(allocator),
            .restart => |*value| value.config.deinit(allocator),
            .remove => {},
        }
    }
};

const GroupOperationQueue = struct {
    const Item = struct {
        command: GroupOperationCommand,
        bytes: usize,
    };

    mutex: std.atomic.Mutex = .unlocked,
    items: std.Deque(Item) = .empty,
    queued_bytes: usize = 0,
    max_items: usize,
    max_bytes: usize,
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator, max_items: usize, max_bytes: usize) GroupOperationQueue {
        return .{
            .allocator = allocator,
            .max_items = max_items,
            .max_bytes = max_bytes,
        };
    }

    fn deinit(self: *GroupOperationQueue) void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        while (self.items.popFront()) |item_value| {
            var item = item_value;
            item.command.deinit(self.allocator);
        }
        self.items.deinit(self.allocator);
    }

    /// Ownership transfers only on success.
    fn push(self: *GroupOperationQueue, command: GroupOperationCommand, bytes: usize) Error!void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        if (self.items.len >= self.max_items or bytes > self.max_bytes -| self.queued_bytes) {
            return error.GroupOperationBackpressure;
        }
        try self.items.pushBack(self.allocator, .{ .command = command, .bytes = bytes });
        self.queued_bytes += bytes;
    }

    fn pop(self: *GroupOperationQueue) ?Item {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        const item = self.items.popFront() orelse return null;
        self.queued_bytes -= item.bytes;
        return item;
    }

    fn takeAll(self: *GroupOperationQueue) std.Deque(Item) {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        const items = self.items;
        self.items = .empty;
        self.queued_bytes = 0;
        return items;
    }

    fn stats(self: *const GroupOperationQueue) GroupOperationQueueStats {
        const mutex = @constCast(&self.mutex);
        spinLock(mutex);
        defer mutex.unlock();
        return .{ .count = self.items.len, .bytes = self.queued_bytes };
    }
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
    pending_messages: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    pending_peer_events: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
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
        if (self.shared.identity()) |identity| {
            const valid_source = if (message.msg_type == .transfer_leader)
                message.from == identity.node_id or self.peers.contains(message.from)
            else
                self.peers.contains(message.from);
            if (!valid_source) {
                var owned = message;
                owned.deinit(self.allocator);
                return;
            }
        }
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
        _ = self.pending_messages.fetchAdd(1, .release);
    }

    fn enqueuePeerEvent(self: *GroupTransport, event: PeerEvent) Error!void {
        const stopped = self.stopped.load(.acquire);
        if (stopped or self.peer_events.items.len >= self.max_inbox_messages) {
            return if (stopped) error.ShuttingDown else error.TransportBackpressure;
        }
        try self.peer_events.append(self.allocator, event);
        _ = self.pending_peer_events.fetchAdd(1, .release);
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
            const message = self.inbox.orderedRemove(0);
            _ = self.pending_messages.fetchSub(1, .release);
            try callback.invoke(message);
            return true;
        }
        if (self.peer_events.items.len != 0) {
            const callback = self.peer_event_callback orelse return false;
            const event = self.peer_events.orderedRemove(0);
            _ = self.pending_peer_events.fetchSub(1, .release);
            try callback.invoke(event);
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
    config: OwnedGroupConfig,
    status_mutex: std.atomic.Mutex = .unlocked,
    lifecycle: GroupLifecycle = .active,
    last_error: ?Error = null,
    scheduled_iterations: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    productive_iterations: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    error_iterations: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    last_host_iteration: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    allocator: std.mem.Allocator,

    fn create(
        allocator: std.mem.Allocator,
        host_config: MultiRaftConfig,
        group_config: *OwnedGroupConfig,
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
                .{ host_config.data_dir, group_config.value.group_id },
                0,
            );
        }

        const owned_config = group_config.transfer();
        self.* = .{
            .id = owned_config.value.group_id,
            .transport = GroupTransport.init(
                allocator,
                owned_config.value.group_id,
                shared_transport,
                host_config.max_group_inbox_messages,
            ),
            .raftor = undefined,
            .data_dir = data_dir,
            .config = owned_config,
            .allocator = allocator,
        };
        errdefer self.config.deinit(allocator);
        errdefer self.transport.deinit();

        var config = self.config.value.raftor;
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
        self.config.deinit(self.allocator);
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

    fn recordScheduled(self: *Group, host_iteration: u64) void {
        _ = self.scheduled_iterations.fetchAdd(1, .monotonic);
        self.last_host_iteration.store(host_iteration, .release);
    }

    fn recordProductive(self: *Group) void {
        _ = self.productive_iterations.fetchAdd(1, .monotonic);
    }

    fn recordError(self: *Group) void {
        _ = self.error_iterations.fetchAdd(1, .monotonic);
    }
};

fn makeGroupStatus(group: *Group) MultiRaftGroupStatus {
    const status = group.lifecycleSnapshot();
    return .{
        .group_id = group.id,
        .lifecycle = status.lifecycle,
        .node = group.raftor.getStatus(),
        .last_error = status.last_error,
        .pending_messages = group.transport.pending_messages.load(.acquire),
        .pending_peer_events = group.transport.pending_peer_events.load(.acquire),
        .scheduled_iterations = group.scheduled_iterations.load(.acquire),
        .productive_iterations = group.productive_iterations.load(.acquire),
        .error_iterations = group.error_iterations.load(.acquire),
        .last_host_iteration = group.last_host_iteration.load(.acquire),
    };
}

pub const MultiRaftHost = struct {
    allocator: std.mem.Allocator,
    config: MultiRaftConfig,
    transport: MultiTransport,
    groups: std.AutoHashMap(GroupId, *Group),
    groups_mutex: std.atomic.Mutex = .unlocked,
    group_ids: std.ArrayList(GroupId) = .empty,
    group_operations: GroupOperationQueue,
    lifecycle_mutex: std.atomic.Mutex = .unlocked,
    event_loop_active: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    event_loop_thread_id: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    shutdown_callback_thread_id: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    run_active: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    stop_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    stopped: bool = false,
    round_robin_cursor: usize = 0,
    unknown_group_messages: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    host_iterations: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    tick_iterations: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    poll_iterations: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    groups_driven: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    groups_with_work: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    group_error_iterations: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    group_operations_completed: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    group_operations_failed: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    envelopes_routed: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    peer_events_routed: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

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
            .group_operations = GroupOperationQueue.init(
                allocator,
                config.max_queued_group_operations,
                config.max_queued_group_operation_bytes,
            ),
        };
        errdefer self.groups.deinit();
        errdefer self.group_operations.deinit();

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
        const thread_id = currentThreadId();
        if (self.event_loop_thread_id.load(.acquire) == thread_id or
            self.shutdown_callback_thread_id.load(.acquire) == thread_id)
        {
            @panic("MultiRaftHost.destroy cannot be called from a host callback");
        }
        self.stop();
        while (self.run_active.load(.acquire) or self.event_loop_active.load(.acquire)) {
            sleepNanoseconds(std.time.ns_per_ms);
        }

        var iterator = self.groups.valueIterator();
        while (iterator.next()) |group| group.*.destroy();
        self.groups.deinit();
        self.group_ids.deinit(self.allocator);
        self.group_operations.deinit();
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
        try self.validateGroupConfig(config);
        var owned = try OwnedGroupConfig.clone(self.allocator, config);
        defer owned.deinit(self.allocator);
        try self.addGroupOwned(&owned, state_machine);
    }

    pub fn removeGroup(self: *MultiRaftHost, group_id: GroupId) Error!void {
        try self.enterManagement();
        defer self.leaveManagement();
        try self.removeGroupInternal(group_id);
    }

    /// Queue a group creation for the host event-loop thread.
    pub fn requestAddGroup(
        self: *MultiRaftHost,
        config: MultiRaftGroupConfig,
        state_machine: StateMachine,
        callback: GroupOperationCallback,
    ) Error!void {
        try self.validateGroupConfig(config);
        const bytes = try group_config_mod.estimatedSize(config);
        if (bytes > self.config.max_queued_group_operation_bytes) return error.GroupOperationBackpressure;
        var command = GroupOperationCommand{ .add = .{
            .config = try OwnedGroupConfig.clone(self.allocator, config),
            .state_machine = state_machine,
            .callback = callback,
        } };
        self.enqueueGroupOperation(command, bytes) catch |err| {
            command.deinit(self.allocator);
            return err;
        };
    }

    /// Queue a group removal for the host event-loop thread.
    pub fn requestRemoveGroup(
        self: *MultiRaftHost,
        group_id: GroupId,
        callback: GroupOperationCallback,
    ) Error!void {
        if (group_id == 0) return error.InvalidGroupId;
        try self.enqueueGroupOperation(.{ .remove = .{
            .group_id = group_id,
            .callback = callback,
        } }, 0);
    }

    /// Replace a group using a new configuration and StateMachine.
    /// If recreation fails, the old group remains removed and can be added again.
    pub fn requestRestartGroup(
        self: *MultiRaftHost,
        config: MultiRaftGroupConfig,
        state_machine: StateMachine,
        callback: GroupOperationCallback,
    ) Error!void {
        try self.validateGroupConfig(config);
        const bytes = try group_config_mod.estimatedSize(config);
        if (bytes > self.config.max_queued_group_operation_bytes) return error.GroupOperationBackpressure;
        var command = GroupOperationCommand{ .restart = .{
            .config = try OwnedGroupConfig.clone(self.allocator, config),
            .state_machine = state_machine,
            .callback = callback,
        } };
        self.enqueueGroupOperation(command, bytes) catch |err| {
            command.deinit(self.allocator);
            return err;
        };
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

        const thread_id = currentThreadId();
        if (self.event_loop_thread_id.load(.acquire) != thread_id) {
            while (self.event_loop_active.load(.acquire)) sleepNanoseconds(std.time.ns_per_ms);
        }
        if (self.shutdown_callback_thread_id.cmpxchgStrong(0, thread_id, .acq_rel, .acquire) != null) {
            @panic("concurrent MultiRaftHost stop callbacks");
        }
        defer self.shutdown_callback_thread_id.store(0, .release);
        self.failQueuedGroupOperations(error.ShuttingDown);
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
        spinLock(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        if (self.stopped) return error.ShuttingDown;
        spinLock(&self.groups_mutex);
        defer self.groups_mutex.unlock();
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
        spinLock(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        if (self.stopped) return error.ShuttingDown;
        spinLock(&self.groups_mutex);
        defer self.groups_mutex.unlock();
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
        spinLock(&self.groups_mutex);
        const group = self.groups.get(group_id) orelse {
            self.groups_mutex.unlock();
            return error.GroupNotFound;
        };
        self.groups_mutex.unlock();
        try group.raftor.campaign();
    }

    pub fn getStatus(self: *const MultiRaftHost, group_id: GroupId) ?MultiRaftGroupStatus {
        const mutex = @constCast(&self.groups_mutex);
        spinLock(mutex);
        defer mutex.unlock();
        const group = self.groups.get(group_id) orelse return null;
        return makeGroupStatus(group);
    }

    /// Event-loop escape hatch. Unavailable while `run` or queued management is active.
    pub fn getRaftor(self: *MultiRaftHost, group_id: GroupId) ?*Raftor {
        if (self.run_active.load(.acquire) or self.group_operations.stats().count != 0) return null;
        spinLock(&self.groups_mutex);
        defer self.groups_mutex.unlock();
        const group = self.groups.get(group_id) orelse return null;
        return group.raftor;
    }

    pub fn groupCount(self: *const MultiRaftHost) usize {
        const mutex = @constCast(&self.groups_mutex);
        spinLock(mutex);
        defer mutex.unlock();
        return self.groups.count();
    }

    pub fn getHostStatus(self: *const MultiRaftHost) MultiRaftHostStatus {
        var result = MultiRaftHostStatus{
            .node_id = self.config.node_id,
            .groups = 0,
            .active_groups = 0,
            .retryable_groups = 0,
            .terminal_groups = 0,
            .stopping_groups = 0,
            .queued_group_operations = self.group_operations.stats(),
            .unknown_group_messages = self.unknown_group_messages.load(.acquire),
            .host_iterations = self.host_iterations.load(.acquire),
            .tick_iterations = self.tick_iterations.load(.acquire),
            .poll_iterations = self.poll_iterations.load(.acquire),
            .groups_driven = self.groups_driven.load(.acquire),
            .groups_with_work = self.groups_with_work.load(.acquire),
            .group_error_iterations = self.group_error_iterations.load(.acquire),
            .group_operations_completed = self.group_operations_completed.load(.acquire),
            .group_operations_failed = self.group_operations_failed.load(.acquire),
            .envelopes_routed = self.envelopes_routed.load(.acquire),
            .peer_events_routed = self.peer_events_routed.load(.acquire),
        };
        const mutex = @constCast(&self.groups_mutex);
        spinLock(mutex);
        defer mutex.unlock();
        result.groups = self.groups.count();
        var iterator = self.groups.valueIterator();
        while (iterator.next()) |group| switch (group.*.lifecycleSnapshot().lifecycle) {
            .active => result.active_groups += 1,
            .retryable_error => result.retryable_groups += 1,
            .terminal => result.terminal_groups += 1,
            .stopping => result.stopping_groups += 1,
        };
        return result;
    }

    pub fn listGroupStatuses(
        self: *const MultiRaftHost,
        allocator: std.mem.Allocator,
    ) Error![]MultiRaftGroupStatus {
        const mutex = @constCast(&self.groups_mutex);
        spinLock(mutex);
        defer mutex.unlock();
        const statuses = try allocator.alloc(MultiRaftGroupStatus, self.group_ids.items.len);
        for (self.group_ids.items, statuses) |group_id, *status| {
            status.* = makeGroupStatus(self.groups.get(group_id).?);
        }
        return statuses;
    }

    pub fn queuedGroupOperations(self: *const MultiRaftHost) GroupOperationQueueStats {
        return self.group_operations.stats();
    }

    pub fn unknownGroupMessageCount(self: *const MultiRaftHost) usize {
        return self.unknown_group_messages.load(.acquire);
    }

    fn drive(self: *MultiRaftHost, advance_clock: bool) Error!bool {
        const host_iteration = self.host_iterations.fetchAdd(1, .monotonic) +% 1;
        if (advance_clock) {
            _ = self.tick_iterations.fetchAdd(1, .monotonic);
        } else {
            _ = self.poll_iterations.fetchAdd(1, .monotonic);
        }
        var had_work = self.processGroupOperations();
        if (self.stop_requested.load(.acquire)) return had_work;
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
            group.recordScheduled(host_iteration);
            _ = self.groups_driven.fetchAdd(1, .monotonic);
            const worked = if (advance_clock) group.raftor.tick() else group.raftor.poll();
            if (worked) |value| {
                had_work = had_work or value;
                if (value) {
                    group.recordProductive();
                    _ = self.groups_with_work.fetchAdd(1, .monotonic);
                }
                group.markActive();
            } else |err| {
                group.recordError();
                _ = self.group_error_iterations.fetchAdd(1, .monotonic);
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

    fn processGroupOperations(self: *MultiRaftHost) bool {
        var had_work = false;
        for (0..self.config.group_operation_budget) |_| {
            var item = self.group_operations.pop() orelse break;
            had_work = true;
            const group_id = item.command.groupId();
            const operation = std.meta.activeTag(item.command);
            const operation_error: ?Error = switch (item.command) {
                .add => |*value| if (self.addGroupOwned(&value.config, value.state_machine)) |_| null else |err| err,
                .remove => |value| if (self.removeGroupInternal(value.group_id)) |_| null else |err| err,
                .restart => |*value| if (self.restartGroupOwned(&value.config, value.state_machine)) |_| null else |err| err,
            };
            _ = self.group_operations_completed.fetchAdd(1, .monotonic);
            if (operation_error != null) _ = self.group_operations_failed.fetchAdd(1, .monotonic);
            item.command.callback().invoke(.{
                .group_id = group_id,
                .operation = operation,
                .err = operation_error,
            });
            item.command.deinit(self.allocator);
            if (self.stop_requested.load(.acquire)) break;
        }
        return had_work;
    }

    fn addGroupOwned(
        self: *MultiRaftHost,
        config: *OwnedGroupConfig,
        state_machine: StateMachine,
    ) Error!void {
        const group_id = config.value.group_id;
        spinLock(&self.groups_mutex);
        if (self.groups.contains(group_id)) {
            self.groups_mutex.unlock();
            return error.GroupAlreadyExists;
        }
        if (self.groups.count() >= self.config.max_groups) {
            self.groups_mutex.unlock();
            return error.GroupLimitReached;
        }
        self.groups.ensureUnusedCapacity(1) catch |err| {
            self.groups_mutex.unlock();
            return err;
        };
        self.groups_mutex.unlock();
        try self.group_ids.ensureUnusedCapacity(self.allocator, 1);
        const group = try Group.create(
            self.allocator,
            self.config,
            config,
            self.transport,
            state_machine,
        );
        spinLock(&self.groups_mutex);
        self.groups.putAssumeCapacity(group_id, group);
        self.group_ids.appendAssumeCapacity(group_id);
        std.mem.sort(GroupId, self.group_ids.items, {}, std.sort.asc(GroupId));
        self.groups_mutex.unlock();
    }

    fn removeGroupInternal(self: *MultiRaftHost, group_id: GroupId) Error!void {
        spinLock(&self.groups_mutex);
        const removed = self.groups.fetchRemove(group_id) orelse {
            self.groups_mutex.unlock();
            return error.GroupNotFound;
        };
        self.removeGroupId(group_id);
        self.groups_mutex.unlock();
        removed.value.destroy();
    }

    fn restartGroupOwned(
        self: *MultiRaftHost,
        config: *OwnedGroupConfig,
        state_machine: StateMachine,
    ) Error!void {
        spinLock(&self.groups_mutex);
        const exists = self.groups.contains(config.value.group_id);
        self.groups_mutex.unlock();
        if (!exists) return error.GroupNotFound;
        try self.removeGroupInternal(config.value.group_id);
        return self.addGroupOwned(config, state_machine);
    }

    fn removeGroupId(self: *MultiRaftHost, group_id: GroupId) void {
        for (self.group_ids.items, 0..) |id, index| {
            if (id == group_id) {
                _ = self.group_ids.orderedRemove(index);
                if (self.group_ids.items.len == 0) {
                    self.round_robin_cursor = 0;
                } else if (self.round_robin_cursor >= self.group_ids.items.len) {
                    self.round_robin_cursor %= self.group_ids.items.len;
                }
                return;
            }
        }
    }

    fn enqueueGroupOperation(
        self: *MultiRaftHost,
        command: GroupOperationCommand,
        bytes: usize,
    ) Error!void {
        spinLock(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        if (self.stopped) return error.ShuttingDown;
        return self.group_operations.push(command, bytes);
    }

    fn failQueuedGroupOperations(self: *MultiRaftHost, err: Error) void {
        var queued = self.group_operations.takeAll();
        defer queued.deinit(self.allocator);
        while (queued.popFront()) |item_value| {
            var item = item_value;
            _ = self.group_operations_completed.fetchAdd(1, .monotonic);
            _ = self.group_operations_failed.fetchAdd(1, .monotonic);
            item.command.callback().invoke(.{
                .group_id = item.command.groupId(),
                .operation = std.meta.activeTag(item.command),
                .err = err,
            });
            item.command.deinit(self.allocator);
        }
    }

    fn validateGroupConfig(self: *const MultiRaftHost, config: MultiRaftGroupConfig) Error!void {
        if (config.group_id == 0) return error.InvalidGroupId;
        if (config.raftor.nodeId() != self.config.node_id) return error.InvalidNodeId;
        if (config.raftor.data_dir.len != 0 or config.raftor.file_system != null) return error.InvalidConfig;
    }

    fn onEnvelope(ctx: *anyopaque, envelope: Envelope) Error!void {
        const self: *MultiRaftHost = @ptrCast(@alignCast(ctx));
        const group = self.groups.get(envelope.group_id) orelse {
            var owned = envelope;
            owned.deinit(self.allocator);
            _ = self.unknown_group_messages.fetchAdd(1, .monotonic);
            return;
        };
        try group.transport.enqueueMessage(envelope.message);
        _ = self.envelopes_routed.fetchAdd(1, .monotonic);
    }

    fn onPeerEvent(ctx: *anyopaque, event: MultiPeerEvent) Error!void {
        const self: *MultiRaftHost = @ptrCast(@alignCast(ctx));
        const group = self.groups.get(event.group_id) orelse return;
        try group.transport.enqueuePeerEvent(.{ .peer_id = event.peer_id, .kind = event.kind });
        _ = self.peer_events_routed.fetchAdd(1, .monotonic);
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
