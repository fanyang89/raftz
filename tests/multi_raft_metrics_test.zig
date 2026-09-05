const std = @import("std");
const raft = @import("raftz");

const allocator = std.testing.allocator;

fn groupConfig(group_id: raft.GroupId, node_id: u64) raft.MultiRaftGroupConfig {
    var config = raft.RaftorConfig{};
    config.raft.id = node_id;
    config.raft.election_tick = 10;
    config.raft.heartbeat_tick = 1;
    config.snapshot_entries_threshold = 0;
    return .{ .group_id = group_id, .raftor = config };
}

fn drive(host: *raft.MultiRaftHost, iterations: usize) !void {
    for (0..iterations) |_| _ = try host.tick();
}

const MetricCapture = struct {
    points: usize = 0,
    counters: usize = 0,
    gauges: usize = 0,
    saw_host_iterations: bool = false,
    saw_group_info: bool = false,
    saw_group_id: bool = false,

    fn emit(ctx: *anyopaque, point: raft.MetricPoint) raft.Error!void {
        const self: *MetricCapture = @ptrCast(@alignCast(ctx));
        self.points += 1;
        switch (point.descriptor.kind) {
            .counter => self.counters += 1,
            .gauge => self.gauges += 1,
        }
        if (std.mem.eql(u8, point.descriptor.name, "raftz.multi_raft.host.iterations")) {
            self.saw_host_iterations = true;
        }
        if (std.mem.eql(u8, point.descriptor.name, "raftz.multi_raft.group.info")) {
            self.saw_group_info = true;
            for (point.attributes) |attribute| {
                if (!std.mem.eql(u8, attribute.key, "group_id")) continue;
                switch (attribute.value) {
                    .integer => |value| self.saw_group_id = value == 501,
                    else => {},
                }
            }
        }
    }

    fn sink(self: *MetricCapture) raft.OpenTelemetryMetricSink {
        return .{ .ctx = self, .function = emit };
    }
};

test "multi raft metrics: Prometheus exposition is typed and labeled" {
    const network = try raft.LoopbackMultiNetwork.create(allocator);
    defer network.destroy();
    const transport = try network.createTransport(1);
    const host = try raft.MultiRaftHost.create(allocator, .{ .node_id = 1 }, transport.transport());
    defer host.destroy();
    var machine = raft.MockStateMachine.init(allocator);
    defer machine.deinit();
    try host.addGroup(groupConfig(501, 1), machine.stateMachine());
    try host.campaign(501);
    try drive(host, 3);

    const encoded = try raft.encodePrometheusMetrics(host, allocator);
    defer allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "# HELP raftz_multi_raft_groups ") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "# TYPE raftz_multi_raft_groups gauge") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "# TYPE raftz_multi_raft_host_iterations_total counter") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "node_id=\"1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "group_id=\"501\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "lifecycle=\"active\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "role=\"leader\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "raftz_multi_raft_group_limit_bytes") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "raftz_multi_raft_group_dropped_bytes_total") != null);

    var metadata = std.StringHashMap(void).init(allocator);
    defer metadata.deinit();
    var lines = std.mem.splitScalar(u8, encoded, '\n');
    while (lines.next()) |line| {
        if (!std.mem.startsWith(u8, line, "# HELP ")) continue;
        var tokens = std.mem.tokenizeScalar(u8, line, ' ');
        _ = tokens.next();
        _ = tokens.next();
        const name = tokens.next().?;
        try std.testing.expect(!metadata.contains(name));
        try metadata.put(name, {});
    }
}

test "multi raft metrics: OpenTelemetry sink exports during concurrent run" {
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
    try host.addGroup(groupConfig(501, 1), machine.stateMachine());
    try host.campaign(501);

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

    for (0..20) |_| {
        var capture = MetricCapture{};
        try raft.exportOpenTelemetryMetrics(host, thread_allocator, capture.sink());
        try std.testing.expect(capture.points > 0);
        try std.testing.expect(capture.counters > 0);
        try std.testing.expect(capture.gauges > 0);
        try std.testing.expect(capture.saw_host_iterations);
        try std.testing.expect(capture.saw_group_info);
        try std.testing.expect(capture.saw_group_id);
        const encoded = try raft.encodePrometheusMetrics(host, thread_allocator);
        thread_allocator.free(encoded);
    }

    host.stop();
    thread.join();
    try std.testing.expect(run_state.err == null);
}
