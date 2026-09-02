const std = @import("std");
const raft = @import("raftz");

const allocator = std.heap.smp_allocator;
const cluster_id = [_]u8{7} ** 16;

const ProposalResult = struct {
    completed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    err: ?raft.Error = null,

    fn invoke(ctx: *anyopaque, result: raft.ProposalResult) void {
        const self: *ProposalResult = @ptrCast(@alignCast(ctx));
        switch (result) {
            .ok => {},
            .err => |err| self.err = err,
        }
        self.completed.store(true, .release);
    }

    fn callback(self: *ProposalResult) raft.ProposalCallback {
        return .{ .ctx = self, .function = invoke };
    }
};

fn transportConfig(node_id: u64) raft.GrpcLiteMultiTransportConfig {
    return .{
        .identity = .{ .cluster_id = cluster_id, .node_id = node_id },
        .listen_addr = "127.0.0.1:0",
        .reconnect_initial_delay_ns = 2 * std.time.ns_per_ms,
        .reconnect_max_delay_ns = 20 * std.time.ns_per_ms,
        .graceful_shutdown_timeout_ns = 20 * std.time.ns_per_ms,
    };
}

fn groupConfig(
    group_id: raft.GroupId,
    node_id: u64,
    local_address: []const u8,
    peers: []const raft.Peer,
) raft.MultiRaftGroupConfig {
    var config = raft.RaftorConfig{};
    config.raft.id = node_id;
    config.raft.election_tick = 10;
    config.raft.heartbeat_tick = 1;
    config.raft.election_timeout_seed = group_id * 1000 + node_id;
    config.cluster_id = cluster_id;
    config.advertise_addr = local_address;
    config.initial_peers = peers;
    config.snapshot_entries_threshold = 0;
    return .{ .group_id = group_id, .raftor = config };
}

fn addressOf(transport: *raft.GrpcLiteMultiTransport, buffer: []u8) ![]const u8 {
    return std.fmt.bufPrint(buffer, "127.0.0.1:{}", .{try transport.port()});
}

fn waitActive(transport: *raft.GrpcLiteMultiTransport, peer_id: u64) !void {
    for (0..1000) |_| {
        if (transport.peerState(peer_id) == .active) return;
        try std.testing.io.sleep(.fromNanoseconds(std.time.ns_per_ms), .awake);
    }
    return error.TestTimeout;
}

fn drive(first: *raft.MultiRaftHost, second: *raft.MultiRaftHost, iterations: usize) !void {
    for (0..iterations) |_| {
        _ = try first.tick();
        _ = try second.tick();
        try std.testing.io.sleep(.fromNanoseconds(std.time.ns_per_ms), .awake);
    }
}

test "grpc multi raft: two groups share one peer stream" {
    const first_transport = try raft.GrpcLiteMultiTransport.create(allocator, transportConfig(1));
    defer first_transport.destroy();
    const second_transport = try raft.GrpcLiteMultiTransport.create(allocator, transportConfig(2));
    defer second_transport.destroy();
    const first_host = try raft.MultiRaftHost.create(allocator, .{ .node_id = 1 }, first_transport.transport());
    defer first_host.destroy();
    const second_host = try raft.MultiRaftHost.create(allocator, .{ .node_id = 2 }, second_transport.transport());
    defer second_host.destroy();

    var first_address_buffer: [64]u8 = undefined;
    var second_address_buffer: [64]u8 = undefined;
    const first_address = try addressOf(first_transport, &first_address_buffer);
    const second_address = try addressOf(second_transport, &second_address_buffer);
    const peers = [_]raft.Peer{
        .{ .id = 1, .context = first_address },
        .{ .id = 2, .context = second_address },
    };

    var first_ten = raft.MockStateMachine.init(allocator);
    defer first_ten.deinit();
    var second_ten = raft.MockStateMachine.init(allocator);
    defer second_ten.deinit();
    var first_twenty = raft.MockStateMachine.init(allocator);
    defer first_twenty.deinit();
    var second_twenty = raft.MockStateMachine.init(allocator);
    defer second_twenty.deinit();

    try first_host.addGroup(groupConfig(10, 1, first_address, &peers), first_ten.stateMachine());
    try second_host.addGroup(groupConfig(10, 2, second_address, &peers), second_ten.stateMachine());
    try first_host.addGroup(groupConfig(20, 1, first_address, &peers), first_twenty.stateMachine());
    try second_host.addGroup(groupConfig(20, 2, second_address, &peers), second_twenty.stateMachine());
    try std.testing.expectEqual(@as(usize, 1), first_transport.peerCount());
    try std.testing.expectEqual(@as(usize, 1), second_transport.peerCount());
    try waitActive(first_transport, 2);
    try waitActive(second_transport, 1);
    const first_open_count = first_transport.peerOpenCount(2);
    const second_open_count = second_transport.peerOpenCount(1);

    try std.testing.expect(!(try first_transport.transport().addPeer(99, 2, second_address)));
    try first_transport.transport().send(&.{.{
        .group_id = 99,
        .message = .{ .msg_type = .heartbeat, .from = 1, .to = 2 },
    }});
    for (0..1000) |_| {
        _ = try second_host.poll();
        if (second_host.unknownGroupMessageCount() == 1) break;
        try std.testing.io.sleep(.fromNanoseconds(std.time.ns_per_ms), .awake);
    }
    try std.testing.expectEqual(@as(usize, 1), second_host.unknownGroupMessageCount());
    try first_transport.transport().removePeer(99, 2);

    try first_host.campaign(10);
    try second_host.campaign(20);
    var elected = false;
    for (0..1000) |_| {
        try drive(first_host, second_host, 1);
        if (first_host.getStatus(10).?.node.role == .leader and
            second_host.getStatus(20).?.node.role == .leader)
        {
            elected = true;
            break;
        }
    }
    try std.testing.expect(elected);

    var ten_result = ProposalResult{};
    var twenty_result = ProposalResult{};
    try first_host.propose(10, "ten-grpc", ten_result.callback());
    try second_host.propose(20, "twenty-grpc", twenty_result.callback());
    for (0..1000) |_| {
        try drive(first_host, second_host, 1);
        if (ten_result.completed.load(.acquire) and twenty_result.completed.load(.acquire) and
            second_ten.applied.items.len > 1 and first_twenty.applied.items.len > 1)
        {
            break;
        }
    }

    try std.testing.expect(ten_result.completed.load(.acquire) and ten_result.err == null);
    try std.testing.expect(twenty_result.completed.load(.acquire) and twenty_result.err == null);
    try std.testing.expectEqualStrings("ten-grpc", first_ten.applied.items[first_ten.applied.items.len - 1]);
    try std.testing.expectEqualStrings("ten-grpc", second_ten.applied.items[second_ten.applied.items.len - 1]);
    try std.testing.expectEqualStrings("twenty-grpc", first_twenty.applied.items[first_twenty.applied.items.len - 1]);
    try std.testing.expectEqualStrings("twenty-grpc", second_twenty.applied.items[second_twenty.applied.items.len - 1]);
    try std.testing.expectEqual(first_open_count, first_transport.peerOpenCount(2));
    try std.testing.expectEqual(second_open_count, second_transport.peerOpenCount(1));

    try first_host.removeGroup(10);
    try second_host.removeGroup(10);
    try std.testing.expectEqual(@as(usize, 1), first_transport.peerCount());
    try std.testing.expectEqual(@as(usize, 1), second_transport.peerCount());
    try first_host.removeGroup(20);
    try second_host.removeGroup(20);
    try std.testing.expectEqual(@as(usize, 0), first_transport.peerCount());
    try std.testing.expectEqual(@as(usize, 0), second_transport.peerCount());
}

test "grpc multi raft: conflicting group peer addresses are rejected" {
    const transport = try raft.GrpcLiteMultiTransport.create(allocator, transportConfig(1));
    defer transport.destroy();
    const host = try raft.MultiRaftHost.create(allocator, .{ .node_id = 1 }, transport.transport());
    defer host.destroy();
    var address_buffer: [64]u8 = undefined;
    const local_address = try addressOf(transport, &address_buffer);
    var first_machine = raft.MockStateMachine.init(allocator);
    defer first_machine.deinit();
    var second_machine = raft.MockStateMachine.init(allocator);
    defer second_machine.deinit();
    const first_peers = [_]raft.Peer{
        .{ .id = 1, .context = local_address },
        .{ .id = 2, .context = "127.0.0.1:12345" },
    };
    const conflicting_peers = [_]raft.Peer{
        .{ .id = 1, .context = local_address },
        .{ .id = 2, .context = "127.0.0.1:54321" },
    };

    try host.addGroup(groupConfig(10, 1, local_address, &first_peers), first_machine.stateMachine());
    try std.testing.expectError(
        error.ConflictingPeerAddress,
        host.addGroup(groupConfig(20, 1, local_address, &conflicting_peers), second_machine.stateMachine()),
    );
}
