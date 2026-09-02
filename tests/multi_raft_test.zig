const std = @import("std");
const raft = @import("raftz");

const allocator = std.testing.allocator;

const FailingStateMachine = struct {
    inner: *raft.MockStateMachine,
    fail_data: []const u8,

    fn apply(ctx: *anyopaque, entry: raft.Entry) raft.Error!raft.ApplyResult {
        const self: *FailingStateMachine = @ptrCast(@alignCast(ctx));
        if (std.mem.eql(u8, entry.data, self.fail_data)) return error.OutOfMemory;
        return raft.MockStateMachine.applyImpl(self.inner, entry);
    }

    fn takeSnapshot(
        ctx: *anyopaque,
        test_allocator: std.mem.Allocator,
        applied_index: u64,
        applied_term: u64,
        conf_state: raft.ConfState,
    ) raft.Error!raft.Snapshot {
        const self: *FailingStateMachine = @ptrCast(@alignCast(ctx));
        return raft.MockStateMachine.takeSnapshotImpl(
            self.inner,
            test_allocator,
            applied_index,
            applied_term,
            conf_state,
        );
    }

    fn restoreSnapshot(
        ctx: *anyopaque,
        metadata: raft.SnapshotMetadata,
        reader: raft.SnapshotReader,
    ) raft.Error!void {
        const self: *FailingStateMachine = @ptrCast(@alignCast(ctx));
        return raft.MockStateMachine.restoreSnapshotImpl(self.inner, metadata, reader);
    }

    fn stateMachine(self: *FailingStateMachine) raft.StateMachine {
        return .{ .ctx = self, .vtable = &vtable };
    }

    const vtable: raft.StateMachine.VTable = .{
        .apply = apply,
        .take_snapshot = takeSnapshot,
        .restore_snapshot = restoreSnapshot,
    };
};

const ProposalResult = struct {
    completed: bool = false,
    err: ?raft.Error = null,

    fn invoke(ctx: *anyopaque, result: raft.ProposalResult) void {
        const self: *ProposalResult = @ptrCast(@alignCast(ctx));
        self.completed = true;
        switch (result) {
            .ok => {},
            .err => |err| self.err = err,
        }
    }

    fn callback(self: *ProposalResult) raft.ProposalCallback {
        return .{ .ctx = self, .function = invoke };
    }
};

fn groupConfig(group_id: raft.GroupId, node_id: u64, peers: []const raft.Peer) raft.MultiRaftGroupConfig {
    var config = raft.RaftorConfig{};
    config.raft.id = node_id;
    config.raft.election_tick = 10;
    config.raft.heartbeat_tick = 1;
    config.raft.election_timeout_seed = group_id * 1000 + node_id;
    config.initial_peers = peers;
    config.snapshot_entries_threshold = 0;
    return .{ .group_id = group_id, .raftor = config };
}

fn drive(hosts: []const *raft.MultiRaftHost, iterations: usize) !void {
    for (0..iterations) |_| {
        for (hosts) |host| _ = try host.tick();
    }
}

test "multi raft: validates and manages group lifecycle" {
    const network = try raft.LoopbackMultiNetwork.create(allocator);
    defer network.destroy();
    const transport = try network.createTransport(1);
    const host = try raft.MultiRaftHost.create(allocator, .{ .node_id = 1 }, transport.transport());
    defer host.destroy();
    var machine = raft.MockStateMachine.init(allocator);
    defer machine.deinit();

    var invalid = groupConfig(0, 1, &.{});
    try std.testing.expectError(error.InvalidGroupId, host.addGroup(invalid, machine.stateMachine()));
    invalid = groupConfig(1, 2, &.{});
    try std.testing.expectError(error.InvalidNodeId, host.addGroup(invalid, machine.stateMachine()));

    const config = groupConfig(1, 1, &.{});
    try host.addGroup(config, machine.stateMachine());
    try std.testing.expectEqual(@as(usize, 1), host.groupCount());
    try std.testing.expectError(error.GroupAlreadyExists, host.addGroup(config, machine.stateMachine()));
    try host.removeGroup(1);
    try std.testing.expectEqual(@as(usize, 0), host.groupCount());
    try std.testing.expectError(error.GroupNotFound, host.removeGroup(1));
}

test "multi raft: one host isolates independent groups" {
    const network = try raft.LoopbackMultiNetwork.create(allocator);
    defer network.destroy();
    const transport = try network.createTransport(1);
    const host = try raft.MultiRaftHost.create(allocator, .{ .node_id = 1 }, transport.transport());
    defer host.destroy();
    var group_ten = raft.MockStateMachine.init(allocator);
    defer group_ten.deinit();
    var group_twenty = raft.MockStateMachine.init(allocator);
    defer group_twenty.deinit();

    try host.addGroup(groupConfig(10, 1, &.{}), group_ten.stateMachine());
    try host.addGroup(groupConfig(20, 1, &.{}), group_twenty.stateMachine());
    try host.campaign(10);
    try host.campaign(20);

    var ten_result = ProposalResult{};
    var twenty_result = ProposalResult{};
    try host.propose(10, "ten", ten_result.callback());
    try host.propose(20, "twenty", twenty_result.callback());
    try drive(&.{host}, 10);

    try std.testing.expect(ten_result.completed and ten_result.err == null);
    try std.testing.expect(twenty_result.completed and twenty_result.err == null);
    try std.testing.expectEqualStrings("ten", group_ten.applied.items[group_ten.applied.items.len - 1]);
    try std.testing.expectEqualStrings("twenty", group_twenty.applied.items[group_twenty.applied.items.len - 1]);
    try std.testing.expectEqual(raft.StateRole.leader, host.getStatus(10).?.node.role);
    try std.testing.expectEqual(raft.StateRole.leader, host.getStatus(20).?.node.role);
}

test "multi raft: shared transport routes two groups across two nodes" {
    const network = try raft.LoopbackMultiNetwork.create(allocator);
    defer network.destroy();
    const transport_one = try network.createTransport(1);
    const transport_two = try network.createTransport(2);
    const host_one = try raft.MultiRaftHost.create(allocator, .{ .node_id = 1 }, transport_one.transport());
    defer host_one.destroy();
    const host_two = try raft.MultiRaftHost.create(allocator, .{ .node_id = 2 }, transport_two.transport());
    defer host_two.destroy();

    var one_ten = raft.MockStateMachine.init(allocator);
    defer one_ten.deinit();
    var two_ten = raft.MockStateMachine.init(allocator);
    defer two_ten.deinit();
    var one_twenty = raft.MockStateMachine.init(allocator);
    defer one_twenty.deinit();
    var two_twenty = raft.MockStateMachine.init(allocator);
    defer two_twenty.deinit();
    const peers = [_]raft.Peer{ .{ .id = 1 }, .{ .id = 2 } };

    try host_one.addGroup(groupConfig(10, 1, &peers), one_ten.stateMachine());
    try host_two.addGroup(groupConfig(10, 2, &peers), two_ten.stateMachine());
    try host_one.addGroup(groupConfig(20, 1, &peers), one_twenty.stateMachine());
    try host_two.addGroup(groupConfig(20, 2, &peers), two_twenty.stateMachine());

    try host_one.campaign(10);
    try host_two.campaign(20);
    try drive(&.{ host_one, host_two }, 20);
    try std.testing.expectEqual(raft.StateRole.leader, host_one.getStatus(10).?.node.role);
    try std.testing.expectEqual(raft.StateRole.leader, host_two.getStatus(20).?.node.role);

    var ten_result = ProposalResult{};
    var twenty_result = ProposalResult{};
    try host_one.propose(10, "group-ten", ten_result.callback());
    try host_two.propose(20, "group-twenty", twenty_result.callback());
    var iterations: usize = 0;
    while ((!ten_result.completed or !twenty_result.completed) and iterations < 100) : (iterations += 1) {
        try drive(&.{ host_one, host_two }, 1);
    }

    try std.testing.expect(ten_result.completed and ten_result.err == null);
    try std.testing.expect(twenty_result.completed and twenty_result.err == null);
    try drive(&.{ host_one, host_two }, 10);
    try std.testing.expectEqualStrings("group-ten", one_ten.applied.items[one_ten.applied.items.len - 1]);
    try std.testing.expectEqualStrings("group-ten", two_ten.applied.items[two_ten.applied.items.len - 1]);
    try std.testing.expectEqualStrings("group-twenty", one_twenty.applied.items[one_twenty.applied.items.len - 1]);
    try std.testing.expectEqualStrings("group-twenty", two_twenty.applied.items[two_twenty.applied.items.len - 1]);
}

test "multi raft: group drive budget schedules groups round robin" {
    const network = try raft.LoopbackMultiNetwork.create(allocator);
    defer network.destroy();
    const transport = try network.createTransport(1);
    const host = try raft.MultiRaftHost.create(allocator, .{
        .node_id = 1,
        .group_drive_budget = 1,
    }, transport.transport());
    defer host.destroy();
    var first_machine = raft.MockStateMachine.init(allocator);
    defer first_machine.deinit();
    var second_machine = raft.MockStateMachine.init(allocator);
    defer second_machine.deinit();
    var third_machine = raft.MockStateMachine.init(allocator);
    defer third_machine.deinit();

    try host.addGroup(groupConfig(10, 1, &.{}), first_machine.stateMachine());
    try host.addGroup(groupConfig(20, 1, &.{}), second_machine.stateMachine());
    try host.addGroup(groupConfig(30, 1, &.{}), third_machine.stateMachine());
    try host.campaign(10);
    try host.campaign(20);
    try host.campaign(30);
    var first = ProposalResult{};
    var second = ProposalResult{};
    var third = ProposalResult{};
    try host.propose(10, "first", first.callback());
    try host.propose(20, "second", second.callback());
    try host.propose(30, "third", third.callback());

    _ = try host.tick();
    try std.testing.expect(first.completed and !second.completed and !third.completed);
    _ = try host.tick();
    try std.testing.expect(second.completed and !third.completed);
    _ = try host.tick();
    try std.testing.expect(third.completed);
}

test "multi raft: terminal group failure does not stop healthy groups" {
    const network = try raft.LoopbackMultiNetwork.create(allocator);
    defer network.destroy();
    const transport = try network.createTransport(1);
    const host = try raft.MultiRaftHost.create(allocator, .{ .node_id = 1 }, transport.transport());
    defer host.destroy();
    var failed_inner = raft.MockStateMachine.init(allocator);
    defer failed_inner.deinit();
    var failed_machine = FailingStateMachine{ .inner = &failed_inner, .fail_data = "fail" };
    var healthy_machine = raft.MockStateMachine.init(allocator);
    defer healthy_machine.deinit();

    try host.addGroup(groupConfig(10, 1, &.{}), failed_machine.stateMachine());
    try host.addGroup(groupConfig(20, 1, &.{}), healthy_machine.stateMachine());
    try host.campaign(10);
    try host.campaign(20);
    var failed = ProposalResult{};
    var healthy = ProposalResult{};
    try host.propose(10, "fail", failed.callback());
    try host.propose(20, "healthy", healthy.callback());
    try drive(&.{host}, 10);

    try std.testing.expect(failed.completed and failed.err.? == error.OutOfMemory);
    try std.testing.expect(healthy.completed and healthy.err == null);
    try std.testing.expectEqual(raft.MultiRaftGroupLifecycle.terminal, host.getStatus(10).?.lifecycle);
    try std.testing.expectEqual(raft.MultiRaftGroupLifecycle.active, host.getStatus(20).?.lifecycle);

    var second = ProposalResult{};
    try host.propose(20, "still-healthy", second.callback());
    try drive(&.{host}, 5);
    try std.testing.expect(second.completed and second.err == null);
}

test "multi raft: concurrent stop joins the shared run loop" {
    const thread_allocator = std.heap.smp_allocator;
    const network = try raft.LoopbackMultiNetwork.create(thread_allocator);
    defer network.destroy();
    const transport = try network.createTransport(1);
    const host = try raft.MultiRaftHost.create(thread_allocator, .{
        .node_id = 1,
        .tick_interval_ms = 1,
    }, transport.transport());
    defer host.destroy();
    var machine = raft.MockStateMachine.init(thread_allocator);
    defer machine.deinit();
    try host.addGroup(groupConfig(1, 1, &.{}), machine.stateMachine());

    const RunState = struct {
        host: *raft.MultiRaftHost,
        err: ?raft.Error = null,

        fn run(self: *@This()) void {
            self.host.run() catch |err| {
                self.err = err;
            };
        }
    };
    var run_state = RunState{ .host = host };
    const thread = try std.Thread.spawn(.{}, RunState.run, .{&run_state});
    while (!host.isRunning()) std.atomic.spinLoopHint();
    host.stop();
    thread.join();
    try std.testing.expect(run_state.err == null);
}

test "multi raft: unknown group envelopes are isolated" {
    const network = try raft.LoopbackMultiNetwork.create(allocator);
    defer network.destroy();
    const source = try network.createTransport(1);
    const target = try network.createTransport(2);
    const host = try raft.MultiRaftHost.create(allocator, .{ .node_id = 2 }, target.transport());
    defer host.destroy();

    try source.transport().send(&.{.{
        .group_id = 99,
        .message = .{ .msg_type = .heartbeat, .from = 1, .to = 2 },
    }});
    try std.testing.expect(try host.poll());
    try std.testing.expectEqual(@as(usize, 1), host.unknownGroupMessageCount());
}

test "multi raft: durable groups use independent WAL directories" {
    var fixture = try raft.FsTestFixture.init(allocator, .real);
    defer fixture.deinit();
    const network = try raft.LoopbackMultiNetwork.create(allocator);
    defer network.destroy();
    const transport = try network.createTransport(1);
    {
        const host = try raft.MultiRaftHost.create(allocator, .{
            .node_id = 1,
            .data_dir = fixture.root(),
            .file_system = fixture.fs(),
        }, transport.transport());
        defer host.destroy();
        var group_one = raft.MockStateMachine.init(allocator);
        defer group_one.deinit();
        var group_two = raft.MockStateMachine.init(allocator);
        defer group_two.deinit();

        try host.addGroup(groupConfig(1, 1, &.{}), group_one.stateMachine());
        try host.addGroup(groupConfig(2, 1, &.{}), group_two.stateMachine());
        try host.campaign(1);
        try host.campaign(2);
        var first = ProposalResult{};
        var second = ProposalResult{};
        try host.propose(1, "first", first.callback());
        try host.propose(2, "second", second.callback());
        try drive(&.{host}, 10);
        try std.testing.expect(first.completed and second.completed);
    }

    for ([_]raft.GroupId{ 1, 2 }) |group_id| {
        const path = try std.fmt.allocPrintSentinel(allocator, "{s}/groups/{}", .{ fixture.root(), group_id }, 0);
        defer allocator.free(path);
        const storage = try raft.WALStorage.openWithFs(allocator, path, fixture.fs());
        defer storage.deinit();
        try std.testing.expect(try storage.asWritableStorage().lastIndex() >= 2);
    }

    const restart_network = try raft.LoopbackMultiNetwork.create(allocator);
    defer restart_network.destroy();
    const restart_transport = try restart_network.createTransport(1);
    const restarted = try raft.MultiRaftHost.create(allocator, .{
        .node_id = 1,
        .data_dir = fixture.root(),
        .file_system = fixture.fs(),
    }, restart_transport.transport());
    defer restarted.destroy();
    var restored_one = raft.MockStateMachine.init(allocator);
    defer restored_one.deinit();
    var restored_two = raft.MockStateMachine.init(allocator);
    defer restored_two.deinit();
    try restarted.addGroup(groupConfig(1, 1, &.{}), restored_one.stateMachine());
    try restarted.addGroup(groupConfig(2, 1, &.{}), restored_two.stateMachine());
    try drive(&.{restarted}, 5);
    try std.testing.expectEqualStrings("first", restored_one.applied.items[restored_one.applied.items.len - 1]);
    try std.testing.expectEqualStrings("second", restored_two.applied.items[restored_two.applied.items.len - 1]);
}

fn exerciseMultiRaftAllocations(test_allocator: std.mem.Allocator) !void {
    const network = try raft.LoopbackMultiNetwork.create(test_allocator);
    defer network.destroy();
    const transport = try network.createTransport(1);
    const host = try raft.MultiRaftHost.create(test_allocator, .{ .node_id = 1 }, transport.transport());
    defer host.destroy();
    var machine = raft.MockStateMachine.init(test_allocator);
    defer machine.deinit();
    try host.addGroup(groupConfig(1, 1, &.{}), machine.stateMachine());
}

test "multi raft: allocation failures unwind host and group ownership" {
    try std.testing.checkAllAllocationFailures(allocator, exerciseMultiRaftAllocations, .{});
}

test "multi raft: envelope codec round trips group and message" {
    const encoded = try raft.encodeEnvelope(allocator, .{
        .group_id = 77,
        .message = .{ .msg_type = .append, .from = 1, .to = 2, .term = 3, .index = 4 },
    });
    defer allocator.free(encoded);
    var decoded = try raft.decodeEnvelope(allocator, encoded);
    defer decoded.deinit(allocator);

    try std.testing.expectEqual(@as(raft.GroupId, 77), decoded.group_id);
    try std.testing.expectEqual(raft.MessageType.append, decoded.message.msg_type);
    try std.testing.expectEqual(@as(u64, 4), decoded.message.index);
}
