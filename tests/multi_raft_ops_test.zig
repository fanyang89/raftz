const std = @import("std");
const raft = @import("raftz");

const allocator = std.testing.allocator;

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

const ReplicaMigrationCapture = struct {
    completed: bool = false,

    fn invoke(ctx: *anyopaque, result: raft.ReplicaMigrationResult) void {
        const self: *ReplicaMigrationCapture = @ptrCast(@alignCast(ctx));
        self.completed = result.err != null;
    }

    fn callback(self: *ReplicaMigrationCapture) raft.ReplicaMigrationCallback {
        return .{ .ctx = self, .function = invoke };
    }
};

test "multi raft ops: report renders host, groups, and migrations" {
    const network = try raft.LoopbackMultiNetwork.create(allocator);
    defer network.destroy();
    const transport = try network.createTransport(1);
    const host = try raft.MultiRaftHost.create(allocator, .{ .node_id = 1 }, transport.transport());
    defer host.destroy();
    var machine = raft.MockStateMachine.init(allocator);
    defer machine.deinit();
    const peers = [_]raft.Peer{.{ .id = 1, .context = "node-1" }};
    var config = groupConfig(701, 1, &peers);
    config.raftor.cluster_id = [_]u8{0x71} ** 16;
    try host.addGroup(config, machine.stateMachine());
    try host.campaign(701);
    for (0..10) |_| _ = try host.tick();

    var migration = ReplicaMigrationCapture{};
    try host.requestReplicaMigration(.{
        .group_id = 701,
        .source_node_id = 1,
        .target_node_id = 2,
        .target_address = "node-2",
        .timeout_ticks = 500,
        .stable_catch_up_ticks = 1,
    }, migration.callback());
    for (0..10) |_| _ = try host.tick();

    const listed = try host.listReplicaMigrations(allocator);
    defer allocator.free(listed);
    try std.testing.expectEqual(@as(usize, 1), listed.len);
    try std.testing.expectEqual(@as(u64, 701), listed[0].group_id);
    try std.testing.expectEqual(@as(u64, 500), listed[0].timeout_ticks);
    try std.testing.expectEqual(@as(u32, 1), listed[0].required_stable_ticks);
    try std.testing.expectEqual(raft.ReplicaMigrationStage.catching_up, listed[0].stage);
    try std.testing.expect(!migration.completed);

    const report = try raft.encodeOpsReport(host, allocator);
    defer allocator.free(report);
    try std.testing.expect(std.mem.indexOf(u8, report, "raftz node=1 groups=1 active=1") != null);
    try std.testing.expect(std.mem.indexOf(u8, report, "group id=701 lifecycle=active role=leader") != null);
    try std.testing.expect(std.mem.indexOf(u8, report, "group id=701 quota inbox_messages=") != null);
    try std.testing.expect(std.mem.indexOf(u8, report, "last_error=none") != null);
    try std.testing.expect(std.mem.indexOf(u8, report, "migrations active=1 recovered=0") != null);
    try std.testing.expect(std.mem.indexOf(u8, report, "migration id=701 stage=catching_up source=1 target=2") != null);
    try std.testing.expect(std.mem.indexOf(u8, report, "stable=0/1") != null);
}

test "multi raft ops: future migration intent version fails as unsupported" {
    var fixture = try raft.FsTestFixture.init(allocator, .real);
    defer fixture.deinit();
    const initial_peers = [_]raft.Peer{.{ .id = 1, .context = "node-1" }};
    var config = groupConfig(702, 1, &initial_peers);
    config.raftor.cluster_id = [_]u8{0x72} ** 16;
    {
        const network = try raft.LoopbackMultiNetwork.create(allocator);
        defer network.destroy();
        const transport = try network.createTransport(1);
        const host = try raft.MultiRaftHost.create(allocator, .{
            .node_id = 1,
            .data_dir = fixture.root(),
            .file_system = fixture.fs(),
        }, transport.transport());
        defer host.destroy();
        var machine = raft.MockStateMachine.init(allocator);
        defer machine.deinit();
        try host.addGroup(config, machine.stateMachine());
        var migration = ReplicaMigrationCapture{};
        try host.requestReplicaMigration(.{
            .group_id = 702,
            .source_node_id = 1,
            .target_node_id = 2,
            .target_address = "node-2",
        }, migration.callback());
        host.stop();
    }

    const path = try std.fmt.allocPrintSentinel(
        allocator,
        "{s}/migrations/702.intent",
        .{fixture.root()},
        0,
    );
    defer allocator.free(path);
    const fs = fixture.fs();
    const handle = try fs.open(path, .read_write);
    var version_bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &version_bytes, 99, .little);
    try fs.pwriteAll(handle, &version_bytes, 4);
    try fs.syncFile(handle);
    try fs.close(handle);

    const network = try raft.LoopbackMultiNetwork.create(allocator);
    defer network.destroy();
    const transport = try network.createTransport(1);
    try std.testing.expectError(
        error.UnsupportedVersion,
        raft.MultiRaftHost.create(allocator, .{
            .node_id = 1,
            .data_dir = fixture.root(),
            .file_system = fs,
        }, transport.transport()),
    );
}
