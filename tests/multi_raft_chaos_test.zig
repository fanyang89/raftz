const std = @import("std");
const raft = @import("raftz");

const allocator = std.testing.allocator;

const ProposalCapture = struct {
    completed: bool = false,
    err: ?raft.Error = null,

    fn invoke(ctx: *anyopaque, result: raft.ProposalResult) void {
        const self: *ProposalCapture = @ptrCast(@alignCast(ctx));
        self.completed = true;
        switch (result) {
            .ok => {},
            .err => |err| self.err = err,
        }
    }

    fn callback(self: *ProposalCapture) raft.ProposalCallback {
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
    config.proposal_timeout_ticks = 30;
    return .{ .group_id = group_id, .raftor = config };
}

fn drive(hosts: []const *raft.MultiRaftHost, iterations: usize) !void {
    for (0..iterations) |_| {
        for (hosts) |host| _ = try host.tick();
    }
}

fn waitProposal(hosts: []const *raft.MultiRaftHost, capture: *ProposalCapture, limit: usize) !void {
    for (0..limit) |_| {
        if (capture.completed) return;
        try drive(hosts, 1);
    }
    return error.TestTimeout;
}

fn configuredSoakRounds() usize {
    const raw = std.c.getenv("RAFTZ_MULTI_RAFT_SOAK_ROUNDS") orelse return 300;
    return std.fmt.parseInt(usize, std.mem.span(raw), 10) catch 300;
}

test "multi raft chaos: group-scoped partition isolates failure and heals" {
    const network = try raft.LoopbackMultiNetwork.create(allocator);
    defer network.destroy();
    const transport_one = try network.createTransport(1);
    const transport_two = try network.createTransport(2);
    const transport_three = try network.createTransport(3);
    const host_one = try raft.MultiRaftHost.create(allocator, .{ .node_id = 1 }, transport_one.transport());
    defer host_one.destroy();
    const host_two = try raft.MultiRaftHost.create(allocator, .{ .node_id = 2 }, transport_two.transport());
    defer host_two.destroy();
    const host_three = try raft.MultiRaftHost.create(allocator, .{ .node_id = 3 }, transport_three.transport());
    defer host_three.destroy();
    const hosts = [_]*raft.MultiRaftHost{ host_one, host_two, host_three };
    const peers = [_]raft.Peer{ .{ .id = 1 }, .{ .id = 2 }, .{ .id = 3 } };
    var machines: [6]raft.MockStateMachine = undefined;
    var initialized: usize = 0;
    defer {
        for (machines[0..initialized]) |*machine| machine.deinit();
    }
    for (&machines) |*machine| {
        machine.* = raft.MockStateMachine.init(allocator);
        initialized += 1;
    }
    for (hosts, 0..) |host, node_index| {
        try host.addGroup(groupConfig(201, node_index + 1, &peers), machines[node_index].stateMachine());
        try host.addGroup(groupConfig(202, node_index + 1, &peers), machines[3 + node_index].stateMachine());
    }
    try host_one.campaign(201);
    try host_one.campaign(202);
    try drive(&hosts, 20);

    try network.partition(201, 1, 2);
    try network.partition(201, 1, 3);
    var isolated = ProposalCapture{};
    try host_one.propose(201, "isolated-old-leader", isolated.callback());
    var healthy = ProposalCapture{};
    try host_one.propose(202, "healthy-group", healthy.callback());
    try waitProposal(&hosts, &healthy, 30);
    try std.testing.expect(healthy.err == null);
    try std.testing.expect(!isolated.completed);

    try host_two.campaign(201);
    try drive(&hosts, 20);
    try std.testing.expectEqual(raft.StateRole.leader, host_two.getStatus(201).?.node.role);
    var replacement = ProposalCapture{};
    try host_two.propose(201, "partition-majority", replacement.callback());
    try waitProposal(&hosts, &replacement, 30);
    try std.testing.expect(replacement.err == null);

    network.healGroup(201);
    try drive(&hosts, 30);
    try std.testing.expect(network.droppedEnvelopeCount() > 0);
    try std.testing.expectEqualStrings(
        "partition-majority",
        machines[0].applied.items[machines[0].applied.items.len - 1],
    );
    for (machines[3..6]) |machine| {
        try std.testing.expectEqualStrings("healthy-group", machine.applied.items[machine.applied.items.len - 1]);
    }
}

test "multi raft chaos: durable replica restarts and catches up" {
    var fixture = try raft.FsTestFixture.init(allocator, .real);
    defer fixture.deinit();
    const node_one_dir = try std.fmt.allocPrintSentinel(allocator, "{s}/node-1", .{fixture.root()}, 0);
    defer allocator.free(node_one_dir);
    const node_two_dir = try std.fmt.allocPrintSentinel(allocator, "{s}/node-2", .{fixture.root()}, 0);
    defer allocator.free(node_two_dir);
    const node_three_dir = try std.fmt.allocPrintSentinel(allocator, "{s}/node-3", .{fixture.root()}, 0);
    defer allocator.free(node_three_dir);
    const network = try raft.LoopbackMultiNetwork.create(allocator);
    defer network.destroy();
    const transport_one = try network.createTransport(1);
    const transport_two = try network.createTransport(2);
    const transport_three = try network.createTransport(3);
    const host_one = try raft.MultiRaftHost.create(allocator, .{
        .node_id = 1,
        .data_dir = node_one_dir,
        .file_system = fixture.fs(),
    }, transport_one.transport());
    defer host_one.destroy();
    const host_two = try raft.MultiRaftHost.create(allocator, .{
        .node_id = 2,
        .data_dir = node_two_dir,
        .file_system = fixture.fs(),
    }, transport_two.transport());
    defer host_two.destroy();
    var host_three: ?*raft.MultiRaftHost = try raft.MultiRaftHost.create(allocator, .{
        .node_id = 3,
        .data_dir = node_three_dir,
        .file_system = fixture.fs(),
    }, transport_three.transport());
    defer if (host_three) |host| host.destroy();
    var machine_one = raft.MockStateMachine.init(allocator);
    defer machine_one.deinit();
    var machine_two = raft.MockStateMachine.init(allocator);
    defer machine_two.deinit();
    var machine_three_before = raft.MockStateMachine.init(allocator);
    defer machine_three_before.deinit();
    const cluster_id = [_]u8{0x41} ** 16;
    const peers = [_]raft.Peer{
        .{ .id = 1, .context = "node-1" },
        .{ .id = 2, .context = "node-2" },
        .{ .id = 3, .context = "node-3" },
    };
    var configs: [3]raft.MultiRaftGroupConfig = undefined;
    for (&configs, 0..) |*config, node_index| {
        config.* = groupConfig(401, node_index + 1, &peers);
        config.raftor.cluster_id = cluster_id;
    }
    try host_one.addGroup(configs[0], machine_one.stateMachine());
    try host_two.addGroup(configs[1], machine_two.stateMachine());
    try host_three.?.addGroup(configs[2], machine_three_before.stateMachine());
    try host_one.campaign(401);
    try drive(&.{ host_one, host_two, host_three.? }, 30);
    var before_restart = ProposalCapture{};
    try host_one.propose(401, "before-restart", before_restart.callback());
    try waitProposal(&.{ host_one, host_two, host_three.? }, &before_restart, 30);
    try drive(&.{ host_one, host_two, host_three.? }, 10);

    host_three.?.destroy();
    host_three = null;
    var while_offline = ProposalCapture{};
    try host_one.propose(401, "while-offline", while_offline.callback());
    try waitProposal(&.{ host_one, host_two }, &while_offline, 30);
    try drive(&.{ host_one, host_two }, 10);

    const replacement_transport = try network.recreateTransport(3);
    host_three = try raft.MultiRaftHost.create(allocator, .{
        .node_id = 3,
        .data_dir = node_three_dir,
        .file_system = fixture.fs(),
    }, replacement_transport.transport());
    var machine_three_after = raft.MockStateMachine.init(allocator);
    defer machine_three_after.deinit();
    try host_three.?.addGroup(configs[2], machine_three_after.stateMachine());
    try drive(&.{ host_one, host_two, host_three.? }, 50);

    try std.testing.expectEqualStrings(
        "while-offline",
        machine_three_after.applied.items[machine_three_after.applied.items.len - 1],
    );
    try std.testing.expectEqual(
        host_one.getStatus(401).?.node.commit_index,
        host_three.?.getStatus(401).?.node.commit_index,
    );
}

test "multi raft chaos: deterministic partition soak converges" {
    const rounds = configuredSoakRounds();
    const network = try raft.LoopbackMultiNetwork.create(allocator);
    defer network.destroy();
    var transports = [_]*raft.LoopbackMultiTransport{
        try network.createTransport(1),
        try network.createTransport(2),
        try network.createTransport(3),
    };
    var hosts = [_]*raft.MultiRaftHost{
        try raft.MultiRaftHost.create(allocator, .{ .node_id = 1 }, transports[0].transport()),
        try raft.MultiRaftHost.create(allocator, .{ .node_id = 2 }, transports[1].transport()),
        try raft.MultiRaftHost.create(allocator, .{ .node_id = 3 }, transports[2].transport()),
    };
    defer for (&hosts) |host| host.destroy();
    const peers = [_]raft.Peer{ .{ .id = 1 }, .{ .id = 2 }, .{ .id = 3 } };
    const group_ids = [_]raft.GroupId{ 301, 302, 303 };
    var machines: [9]raft.MockStateMachine = undefined;
    var initialized: usize = 0;
    defer {
        for (machines[0..initialized]) |*machine| machine.deinit();
    }
    for (&machines) |*machine| {
        machine.* = raft.MockStateMachine.init(allocator);
        initialized += 1;
    }
    for (group_ids, 0..) |group_id, group_index| {
        for (hosts, 0..) |host, node_index| {
            try host.addGroup(
                groupConfig(group_id, node_index + 1, &peers),
                machines[group_index * hosts.len + node_index].stateMachine(),
            );
        }
        try hosts[group_index].campaign(group_id);
    }
    try drive(&hosts, 30);

    var payload_buffer: [64]u8 = undefined;
    for (0..rounds) |round| {
        const group_index = round % group_ids.len;
        const group_id = group_ids[group_index];
        const leader_index = group_index;
        const blocked_index = (leader_index + 1 + (round / group_ids.len) % 2) % hosts.len;
        if (round % 7 == 0) try network.partition(
            group_id,
            leader_index + 1,
            blocked_index + 1,
        );
        const payload = try std.fmt.bufPrint(&payload_buffer, "group-{}-round-{}", .{ group_id, round });
        var proposal = ProposalCapture{};
        try hosts[leader_index].propose(group_id, payload, proposal.callback());
        try waitProposal(&hosts, &proposal, 40);
        try std.testing.expect(proposal.err == null);
        network.healGroup(group_id);
        try drive(&hosts, 1);
    }

    network.healAll();
    try drive(&hosts, 40);
    try std.testing.expect(network.droppedEnvelopeCount() > 0);
    for (group_ids, 0..) |group_id, group_index| {
        const expected = machines[group_index * hosts.len].applied.items;
        try std.testing.expect(expected.len > 0);
        const last = expected[expected.len - 1];
        for (0..hosts.len) |node_index| {
            const machine = &machines[group_index * hosts.len + node_index];
            try std.testing.expect(machine.applied.items.len > 0);
            try std.testing.expectEqualStrings(last, machine.applied.items[machine.applied.items.len - 1]);
            const status = hosts[node_index].getStatus(group_id).?;
            try std.testing.expect(status.lifecycle != .terminal);
        }
    }
}
