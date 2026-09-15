const std = @import("std");
const raft = @import("raftz");
const a = std.testing.allocator;

const Entry = struct { index: u64, term: u64, data: []const u8, context: []const u8 };
const Message = struct {
    kind: []const u8,
    from: u64,
    to: u64,
    term: u64,
    index: u64,
    log_term: u64,
    commit: u64,
    commit_term: u64,
    reject: bool,
    reject_hint: u64,
    entries: []Entry,
};
const State = struct { term: u64, vote: u64, commit: u64, role: []const u8, log: []Entry, durable_term: u64, durable_vote: u64, durable_commit: u64, durable_log: []Entry };

fn entries(allocator: std.mem.Allocator, es: []const raft.Entry) ![]Entry {
    const result = try allocator.alloc(Entry, es.len);
    for (es, result) |e, *out| {
        if (e.entry_type != .normal) return error.UnsupportedEntry;
        out.* = .{ .index = e.index, .term = e.term, .data = try allocator.dupe(u8, e.data), .context = try allocator.dupe(u8, e.context) };
    }
    return result;
}

fn message(allocator: std.mem.Allocator, m: raft.Message) !Message {
    switch (m.msg_type) {
        .append, .append_response, .request_vote, .request_vote_response, .heartbeat, .heartbeat_response => {},
        else => return error.UnsupportedMessage,
    }
    if (m.context.len != 0 or m.priority != 0 or m.snapshot != null or m.request_snapshot != 0) return error.UnsupportedMessageFeature;
    return .{ .kind = @tagName(m.msg_type), .from = m.from, .to = m.to, .term = m.term, .index = m.index, .log_term = m.log_term, .commit = m.commit, .commit_term = m.commit_term, .reject = m.reject, .reject_hint = m.reject_hint, .entries = try entries(allocator, m.entries) };
}

const Cluster = struct {
    stores: [3]raft.MemoryStorage = undefined,
    nodes: [3]raft.RawNode = undefined,
    held: [3]std.ArrayList(raft.Message) = .{ .empty, .empty, .empty },
    network: std.ArrayList(raft.Message) = .empty,
    trace: std.ArrayList(u8) = .empty,
    seq: usize = 0,
    isolated: u64 = 0,

    fn config(id: u64) raft.Config {
        return .{ .id = id, .election_tick = 10, .heartbeat_tick = 1, .pre_vote = false, .check_quorum = false, .batch_append = false, .priority = 0, .load_state_on_startup = true, .election_timeout_seed = 42, .max_apply_unpersisted_log_limit = 0, .disable_proposal_forwarding = true };
    }

    fn checkConf(cs: raft.ConfState) !void {
        if (!std.mem.eql(u64, cs.voters, &.{ 1, 2, 3 }) or cs.voters_outgoing.len != 0 or cs.learners.len != 0 or cs.learners_next.len != 0 or cs.auto_leave) return error.UnsupportedMembership;
    }

    fn init(self: *Cluster) !void {
        for (&self.stores, &self.nodes, 1..) |*store, *node, id| {
            store.* = raft.MemoryStorage.init();
            var cs = raft.ConfState{ .voters = try a.dupe(u64, &.{ 1, 2, 3 }) };
            defer cs.deinit(a);
            try store.setRaftState(a, .{ .conf_state = cs });
            node.* = try raft.RawNode.init(a, config(id), store.asStorage());
        }
        try self.line(.{ .kind = "header", .version = 1, .voters = .{ 1, 2, 3 }, .pre_vote = false, .check_quorum = false, .transfer = false, .read_index = false, .snapshot = false, .compaction = false, .atomic_memory_storage = true, .send_after_persist = true });
        try self.observe("init", 0, null, "");
    }

    fn deinit(self: *Cluster) void {
        for (&self.nodes, &self.stores, &self.held) |*node, *store, *held| {
            node.deinit();
            store.deinit(a);
            for (held.items) |*m| m.deinit(a);
            held.deinit(a);
        }
        for (self.network.items) |*m| m.deinit(a);
        self.network.deinit(a);
        self.trace.deinit(a);
    }

    fn line(self: *Cluster, value: anytype) !void {
        const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
        defer a.free(bytes);
        try self.trace.appendSlice(a, bytes);
        try self.trace.append(a, '\n');
    }

    fn observe(self: *Cluster, kind: []const u8, id: u64, input: ?raft.Message, value: []const u8) !void {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const alloc = arena.allocator();
        var states: [3]State = undefined;
        var pending: std.ArrayList(Message) = .empty;
        var network: std.ArrayList(Message) = .empty;
        for (&self.nodes, &self.stores, &self.held, &states) |*node, *store, *held, *s| {
            const r = &node.raft;
            if (r.pre_vote or r.check_quorum or r.lead_transferee != null or r.pending_request_snapshot != 0 or r.raft_log.firstIndex() != 1) return error.UnsupportedNodeFeature;
            var conf = try r.progress_tracker.conf.toConfState(a);
            defer conf.deinit(a);
            try checkConf(conf);
            try checkConf(store.core.raft_state.conf_state);
            const log_entries = try r.raft_log.slice(1, r.raft_log.lastIndex() + 1, std.math.maxInt(u64), .{ .empty = .{ .can_async = false } });
            defer {
                for (log_entries) |*e| e.deinit(a);
                a.free(log_entries);
            }
            const hs = store.core.raft_state.hard_state;
            s.* = .{ .term = r.term, .vote = r.vote, .commit = r.raft_log.committed, .role = @tagName(r.state), .log = try entries(alloc, log_entries), .durable_term = hs.term, .durable_vote = hs.vote, .durable_commit = hs.commit, .durable_log = try entries(alloc, store.core.entries.items) };
            for (r.messages.items) |m| try pending.append(alloc, try message(alloc, m));
            for (held.items) |m| try pending.append(alloc, try message(alloc, m));
        }
        for (self.network.items) |m| try network.append(alloc, try message(alloc, m));
        try self.line(.{ .kind = kind, .seq = self.seq, .node = id, .input = if (input) |m| try message(alloc, m) else null, .value = value, .states = states, .pending = pending.items, .network = network.items });
        self.seq += 1;
    }

    fn hold(self: *Cluster, id: u64, ms: []const raft.Message) !void {
        for (ms) |m| try self.held[id - 1].append(a, try raft.cloneMessage(a, m));
    }

    fn flush(self: *Cluster, id: u64) !void {
        const node = &self.nodes[id - 1];
        const store = &self.stores[id - 1];
        var rounds: usize = 0;
        while (node.hasReady()) {
            rounds += 1;
            if (rounds > 20) return error.ReadyLimit;
            var rd = try node.getReady();
            defer rd.deinit(a);
            if (rd.snapshot != null or rd.read_states.len != 0) return error.UnsupportedReady;
            try self.hold(id, rd.messages());
            try self.hold(id, rd.persistedMessages());
            try self.observe("ready", id, null, "");
            try store.append(a, rd.entries);
            if (rd.hs) |hs| try store.setHardState(hs);
            try self.observe("persist", id, null, "");
            var light = try node.advance(rd);
            defer light.deinit(a);
            try self.hold(id, light.messages);
            try self.observe("advance", id, null, "");
            if (light.commit_index) |commit| {
                var hs = store.core.raft_state.hard_state;
                hs.commit = commit;
                try store.setHardState(hs);
                try self.observe("persist", id, null, "");
            }
            try self.network.appendSlice(a, self.held[id - 1].items);
            self.held[id - 1].clearRetainingCapacity();
            try self.observe("release", id, null, "");
        }
    }

    fn campaign(self: *Cluster, id: u64) !void {
        try self.nodes[id - 1].campaign();
        try self.observe("campaign", id, null, "");
        try self.flush(id);
    }

    fn propose(self: *Cluster, id: u64, value: []const u8) !void {
        if (value.len == 0) return error.EmptyProposal;
        try self.nodes[id - 1].propose("", value);
        try self.observe("propose", id, null, value);
        try self.flush(id);
    }

    fn beat(self: *Cluster, id: u64) !void {
        var m = raft.Message{ .msg_type = .beat };
        try self.nodes[id - 1].raft.step(&m);
        try self.observe("beat", id, null, "");
        try self.flush(id);
    }

    fn drain(self: *Cluster) !void {
        var count: usize = 0;
        while (self.network.items.len > 0) {
            count += 1;
            if (count > 500) return error.DeliveryLimit;
            var m = self.network.orderedRemove(0);
            defer m.deinit(a);
            if (self.isolated != 0 and (m.from == self.isolated or m.to == self.isolated)) {
                try self.observe("drop", m.to, m, "");
            } else {
                try self.nodes[m.to - 1].step(try raft.cloneMessage(a, m));
                try self.observe("receive", m.to, m, "");
                try self.flush(m.to);
            }
        }
    }

    fn finish(self: *Cluster, path: []const u8, commit: u64) !void {
        for (&self.nodes) |*node| try std.testing.expectEqual(commit, node.raft.raft_log.committed);
        try self.observe("end", 0, null, "");
        try self.line(.{ .kind = "terminal", .events = self.seq });
        try self.save(path);
    }

    fn save(self: *Cluster, path: []const u8) !void {
        try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = path, .data = self.trace.items });
    }
};

test "tla trace: three voters election, distinct commands, duplicate and atomic restart" {
    var c = Cluster{};
    try c.init();
    defer c.deinit();
    errdefer c.save(".cache/tla/election.failed.ndjson") catch {};
    try c.campaign(1);
    const duplicate = try raft.cloneMessage(a, c.network.items[0]);
    try c.network.append(a, duplicate);
    try c.observe("duplicate", duplicate.to, duplicate, "");
    try c.drain();
    try c.propose(1, "alpha");
    try c.drain();
    try c.propose(1, "beta");
    try c.drain();
    c.nodes[2].deinit();
    c.nodes[2] = try raft.RawNode.init(a, Cluster.config(3), c.stores[2].asStorage());
    try c.observe("restart", 3, null, "");
    try c.finish(".cache/tla/election.ndjson", 3);
}

test "tla trace: partition, leader change and conflicting suffix repair" {
    var c = Cluster{};
    try c.init();
    defer c.deinit();
    errdefer c.save(".cache/tla/conflict.failed.ndjson") catch {};
    try c.campaign(1);
    try c.drain();
    try c.propose(1, "shared");
    try c.drain();
    c.isolated = 1;
    try c.propose(1, "orphan");
    try c.drain();
    try std.testing.expectEqual(@as(u64, 2), c.nodes[0].raft.raft_log.committed);
    try c.campaign(2);
    try c.drain();
    try c.propose(2, "replacement");
    try c.drain();
    c.isolated = 0;
    try c.beat(2);
    try c.drain();
    try c.propose(2, "healed");
    try c.drain();
    for (&c.stores) |*store| {
        try std.testing.expectEqualStrings("replacement", store.core.entries.items[3].data);
        try std.testing.expectEqualStrings("healed", store.core.entries.items[4].data);
    }
    try c.finish(".cache/tla/conflict.ndjson", 5);
}
