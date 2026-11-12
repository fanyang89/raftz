//! Multi-node Raftor integration tests.
//!
//! Unlike multi_node_test.zig (which drives RawNode directly), these tests
//! exercise the full Raftor pipeline: Transport → RawNode → ReadyProcessor →
//! StateMachine. Three Raftor instances connect via LoopbackTransport.

const std = @import("std");
const raft = @import("raftz");

const allocator = std.testing.allocator;

const Raftor = raft.Raftor;
const RaftorConfig = raft.RaftorConfig;
const MockStateMachine = raft.MockStateMachine;
const LoopbackNetwork = raft.LoopbackNetwork;
const StateRole = raft.StateRole;

var isolated_node = std.atomic.Value(u64).init(0);

fn dropIsolatedNode(from: u64, to: u64, _: raft.MessageType) bool {
    const isolated = isolated_node.load(.acquire);
    return isolated != 0 and (from == isolated or to == isolated);
}

fn makeConfig(id: u64) RaftorConfig {
    var rc = RaftorConfig{};
    rc.raft.id = id;
    rc.raft.election_tick = 10;
    rc.raft.heartbeat_tick = 1;
    rc.raft.election_timeout_seed = id * 777 + 3;
    return rc;
}

const Cluster = struct {
    net: *raft.LoopbackNetwork,
    raftors: [3]*Raftor,
    sms: *[3]MockStateMachine,

    fn destroy(self: *Cluster) void {
        for (self.raftors) |r| r.destroy();
        for (self.sms) |*sm| sm.deinit();
        allocator.destroy(self.sms);
        self.net.destroy();
    }
};

fn createCluster() !Cluster {
    return createClusterWithCheckQuorum(false);
}

fn createClusterWithCheckQuorum(check_quorum: bool) !Cluster {
    const net = try LoopbackNetwork.create(allocator);

    const sms = try allocator.create([3]MockStateMachine);
    sms.* = .{
        MockStateMachine.init(allocator),
        MockStateMachine.init(allocator),
        MockStateMachine.init(allocator),
    };

    var transports: [3]*raft.LoopbackTransport = undefined;
    for (1..4) |i| {
        transports[i - 1] = try net.createTransport(@intCast(i));
    }

    // Heap-allocate peers slice so the address survives createWithTransport's
    // internal bootstrap (which reads config.initial_peers).
    const peers = try allocator.alloc(raft.Peer, 3);
    defer allocator.free(peers);
    peers[0] = .{ .id = 1 };
    peers[1] = .{ .id = 2 };
    peers[2] = .{ .id = 3 };

    var raftors: [3]*Raftor = undefined;
    for (1..4) |i| {
        const idx = i - 1;
        var config = makeConfig(@intCast(i));
        config.raft.check_quorum = check_quorum;
        config.initial_peers = peers;
        raftors[idx] = try Raftor.createWithTransport(
            allocator,
            config,
            sms[idx].stateMachine(),
            transports[idx].transport(),
        );
    }

    return .{ .net = net, .raftors = raftors, .sms = sms };
}

/// Drive one event-loop cycle for every Raftor.
fn tickCluster(c: *Cluster) !void {
    for (c.raftors) |r| _ = try r.tick();
}

fn countLeaders(c: *Cluster) usize {
    var count: usize = 0;
    for (c.raftors) |r| {
        if (r.isLeader()) count += 1;
    }
    return count;
}

test "raftor multi-node: 3-node leader election" {
    var cluster = try createCluster();
    defer cluster.destroy();

    // Campaign node 1.
    try cluster.raftors[0].campaign();

    // Drive election.
    var i: usize = 0;
    while (i < 30 and countLeaders(&cluster) == 0) : (i += 1) {
        try tickCluster(&cluster);
    }

    try std.testing.expectEqual(@as(usize, 1), countLeaders(&cluster));
}

test "raftor multi-node: propose replicates to all state machines" {
    var cluster = try createCluster();
    defer cluster.destroy();

    try cluster.raftors[0].campaign();
    var i: usize = 0;
    while (i < 30 and countLeaders(&cluster) == 0) : (i += 1) {
        try tickCluster(&cluster);
    }
    try std.testing.expectEqual(@as(usize, 1), countLeaders(&cluster));

    // Propose from the leader.
    const Tester = struct {
        applied: bool = false,
        fn cb(ctx: *anyopaque, result: raft.ProposalResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (result == .ok) self.applied = true;
        }
    };
    var tester = Tester{};
    for (cluster.raftors) |r| {
        if (r.isLeader()) {
            try r.propose("hello", .{ .ctx = &tester, .function = Tester.cb });
            break;
        }
    }

    // Drive replication.
    i = 0;
    while (i < 30) : (i += 1) try tickCluster(&cluster);

    try std.testing.expect(tester.applied);

    // All state machines should have applied the proposed entry (plus noop).
    // The leader's SM gets noop + proposal; followers get replicated entries.
    for (cluster.sms) |*sm| {
        try std.testing.expect(sm.applied.items.len >= 1);
    }
}

test "raftor multi-node: leader election with transport message routing" {
    var cluster = try createCluster();
    defer cluster.destroy();

    // Don't call campaign — let election timeouts fire naturally.
    // With distinct seeds, one node times out first and wins.
    var i: usize = 0;
    while (i < 100 and countLeaders(&cluster) == 0) : (i += 1) {
        try tickCluster(&cluster);
    }

    // A leader should eventually emerge via natural election timeout.
    try std.testing.expectEqual(@as(usize, 1), countLeaders(&cluster));
}

test "raftor multi-node: leadership loss terminates tracked requests" {
    var cluster = try createClusterWithCheckQuorum(true);
    defer cluster.destroy();
    isolated_node.store(0, .release);
    defer isolated_node.store(0, .release);

    try cluster.raftors[0].campaign();
    var rounds: usize = 0;
    while (rounds < 40 and countLeaders(&cluster) == 0) : (rounds += 1) try tickCluster(&cluster);
    var leader: ?*Raftor = null;
    for (cluster.raftors) |raftor| {
        if (raftor.isLeader()) leader = raftor;
    }
    const old_leader = leader orelse return error.LeaderNotElected;
    isolated_node.store(old_leader.getStatus().id, .release);
    cluster.net.drop_filter = dropIsolatedNode;

    const Callback = struct {
        calls: usize = 0,
        err: ?raft.Error = null,

        fn proposal(ctx: *anyopaque, result: raft.ProposalResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.calls += 1;
            if (result == .err) self.err = result.err;
        }
        fn read(ctx: *anyopaque, result: raft.ReadIndexResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.calls += 1;
            if (result == .err) self.err = result.err;
        }
    };
    var proposal = Callback{};
    var read = Callback{};
    try old_leader.propose("isolated", .{ .ctx = &proposal, .function = Callback.proposal });
    try old_leader.readIndex("isolated-read", .{ .ctx = &read, .function = Callback.read });
    _ = try old_leader.tick();
    try std.testing.expectEqual(@as(usize, 0), proposal.calls);
    try std.testing.expectEqual(@as(usize, 0), read.calls);

    for (0..50) |_| _ = try old_leader.tick();
    try std.testing.expect(!old_leader.isLeader());
    try std.testing.expectEqual(@as(usize, 1), proposal.calls);
    try std.testing.expectEqual(error.ProposalDropped, proposal.err.?);
    try std.testing.expectEqual(@as(usize, 1), read.calls);
    try std.testing.expectEqual(error.LostLeadership, read.err.?);

    isolated_node.store(0, .release);
    for (0..40) |_| try tickCluster(&cluster);
    try std.testing.expectEqual(@as(usize, 1), proposal.calls);
    try std.testing.expectEqual(@as(usize, 1), read.calls);
}

test "raftor multi-node: dropped forwarded proposals allow election recovery" {
    const Callback = struct {
        calls: usize = 0,
        err: ?raft.Error = null,

        fn proposal(ctx: *anyopaque, result: raft.ProposalResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.calls += 1;
            if (result == .err) self.err = result.err;
        }
    };

    for ([_]bool{ false, true }) |pre_vote| {
        for ([_]bool{ false, true }) |use_poll| {
            var dropped = Callback{};
            var retried = Callback{};
            var cluster = try createClusterWithCheckQuorum(true);
            defer cluster.destroy();
            isolated_node.store(0, .release);
            defer isolated_node.store(0, .release);
            for (cluster.raftors) |r| {
                r.raw_node.raft.pre_vote = pre_vote;
                r.config.proposal_timeout_ticks = 10;
            }

            const old_leader = cluster.raftors[0];
            const sender = cluster.raftors[1];
            try old_leader.campaign();
            for (0..20) |_| try tickCluster(&cluster);
            try std.testing.expect(old_leader.isLeader());
            try std.testing.expectEqual(@as(u64, 1), sender.getStatus().leader_id);

            isolated_node.store(1, .release);
            cluster.net.drop_filter = dropIsolatedNode;
            const expected_role: StateRole = if (pre_vote) .pre_candidate else .candidate;
            var rounds: usize = 0;
            while (rounds < 60 and old_leader.getStatus().role != expected_role) : (rounds += 1) {
                _ = try old_leader.tick();
            }
            try std.testing.expectEqual(expected_role, old_leader.getStatus().role);
            try std.testing.expectEqual(@as(u64, 1), sender.getStatus().leader_id);

            isolated_node.store(0, .release);
            try sender.propose("dropped", .{ .ctx = &dropped, .function = Callback.proposal });
            _ = try sender.poll();
            const inbox = cluster.net.getTransport(1).?.inbox.items;
            try std.testing.expect(inbox.len > 0);
            try std.testing.expectEqual(raft.MessageType.propose, inbox[inbox.len - 1].msg_type);
            if (use_poll) {
                try std.testing.expect(try old_leader.poll());
            } else {
                try std.testing.expect(try old_leader.tick());
            }
            try std.testing.expectEqual(expected_role, old_leader.getStatus().role);

            for (0..60) |_| try tickCluster(&cluster);
            try std.testing.expectEqual(@as(usize, 1), countLeaders(&cluster));
            try std.testing.expectEqual(@as(usize, 1), dropped.calls);
            try std.testing.expectEqual(error.Timeout, dropped.err.?);
            try sender.propose("retry", .{ .ctx = &retried, .function = Callback.proposal });
            for (0..30) |_| try tickCluster(&cluster);
            try std.testing.expectEqual(@as(usize, 1), retried.calls);
            try std.testing.expectEqual(@as(?raft.Error, null), retried.err);
            try std.testing.expectEqual(@as(usize, 1), dropped.calls);
            for (cluster.sms) |*sm| {
                var retries: usize = 0;
                for (sm.applied.items) |data| {
                    try std.testing.expect(!std.mem.eql(u8, data, "dropped"));
                    if (std.mem.eql(u8, data, "retry")) retries += 1;
                }
                try std.testing.expectEqual(@as(usize, 1), retries);
            }
        }
    }
}
