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
    context: []const u8,
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
    if (m.priority != 0 or m.snapshot != null or m.request_snapshot != 0) return error.UnsupportedMessageFeature;
    return .{ .kind = @tagName(m.msg_type), .from = m.from, .to = m.to, .term = m.term, .index = m.index, .log_term = m.log_term, .commit = m.commit, .commit_term = m.commit_term, .reject = m.reject, .reject_hint = m.reject_hint, .entries = try entries(allocator, m.entries), .context = try allocator.dupe(u8, m.context) };
}

const ReadResult = struct { context: []const u8, index: u64 };
const CompletedRead = struct { context: []const u8, index: u64, value: []const u8 };
const WriteResult = struct { value: []const u8, completed: usize };
const Register = struct {
    value: []u8 = &.{},

    fn deinit(self: *Register) void {
        a.free(self.value);
    }

    fn stateMachine(self: *Register) raft.StateMachine {
        return .{ .ctx = self, .vtable = &vtable };
    }

    fn applyImpl(ctx: *anyopaque, entry: raft.Entry) raft.Error!raft.ApplyResult {
        const self: *Register = @ptrCast(@alignCast(ctx));
        const value = try a.dupe(u8, entry.data);
        a.free(self.value);
        self.value = value;
        return .{};
    }

    fn takeSnapshot(_: *anyopaque, _: std.mem.Allocator, _: u64, _: u64, _: raft.ConfState) raft.Error!raft.Snapshot {
        return error.Fatal;
    }

    fn restoreSnapshot(_: *anyopaque, _: raft.SnapshotMetadata, _: raft.SnapshotReader) raft.Error!void {
        return error.Fatal;
    }

    const vtable: raft.StateMachine.VTable = .{ .apply = applyImpl, .take_snapshot = takeSnapshot, .restore_snapshot = restoreSnapshot };
};
const WriteCapture = struct {
    value: []const u8,
    node: *raft.RawNode,
    completed: usize = 0,

    fn invoke(ctx: *anyopaque, result: raft.ProposalResult) void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        switch (result) {
            .ok => self.completed += 1,
            .err => @panic("unexpected write failure"),
        }
    }
};
const PendingRead = struct { context: []const u8, index: u64, acks: []u64 };
const TrackedRead = struct { context: []const u8, index: ?u64 };
const ReadObservation = struct {
    waiting: [][]const u8,
    pending: []PendingRead,
    ready: []ReadResult,
    tracked: []TrackedRead,
    applied: u64,
    completed: []CompletedRead,
    value: []const u8,
    writes: []WriteResult,
};
const ReadCapture = struct {
    context: []const u8,
    node: *raft.RawNode,
    count: usize = 0,
    applied: u64 = 0,
    machine: *Register,
    value: []u8 = &.{},

    fn invoke(ctx: *anyopaque, result: raft.ReadIndexResult) void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        switch (result) {
            .ok => {},
            .err => @panic("unexpected read failure"),
        }
        self.count += 1;
        self.applied = self.node.raft.raft_log.applied;
        const value = a.dupe(u8, self.machine.value) catch @panic("cannot record read result");
        a.free(self.value);
        self.value = value;
    }
};

const Cluster = struct {
    stores: [3]raft.MemoryStorage = undefined,
    nodes: [3]raft.RawNode = undefined,
    held: [3]std.ArrayList(raft.Message) = .{ .empty, .empty, .empty },
    network: std.ArrayList(raft.Message) = .empty,
    trace: std.ArrayList(u8) = .empty,
    seq: usize = 0,
    isolated: u64 = 0,
    read_mode: bool = false,
    trackers: [3]raft.ProposalTracker = undefined,
    held_reads: [3]std.ArrayList(ReadResult) = .{ .empty, .empty, .empty },
    captures: [8]ReadCapture = undefined,
    capture_count: usize = 0,
    machines: [3]Register = .{ .{}, .{}, .{} },
    writes: [8]WriteCapture = undefined,
    write_count: usize = 0,

    fn config(id: u64) raft.Config {
        return .{ .id = id, .election_tick = 10, .heartbeat_tick = 1, .pre_vote = false, .check_quorum = false, .batch_append = false, .priority = 0, .load_state_on_startup = true, .election_timeout_seed = 42, .max_apply_unpersisted_log_limit = 0, .disable_proposal_forwarding = true };
    }

    fn checkConf(cs: raft.ConfState) !void {
        if (!std.mem.eql(u64, cs.voters, &.{ 1, 2, 3 }) or cs.voters_outgoing.len != 0 or cs.learners.len != 0 or cs.learners_next.len != 0 or cs.auto_leave) return error.UnsupportedMembership;
    }

    fn init(self: *Cluster) !void {
        for (&self.stores, &self.nodes, 1..) |*store, *node, id| {
            self.trackers[id - 1] = raft.ProposalTracker.init(a);
            store.* = raft.MemoryStorage.init();
            var cs = raft.ConfState{ .voters = try a.dupe(u64, &.{ 1, 2, 3 }) };
            defer cs.deinit(a);
            try store.setRaftState(a, .{ .conf_state = cs });
            node.* = try raft.RawNode.init(a, config(id), store.asStorage());
        }
        if (self.read_mode) {
            try self.line(.{ .kind = "header", .version = 3, .application = "register-v1", .voters = .{ 1, 2, 3 }, .pre_vote = false, .check_quorum = false, .transfer = false, .read_index = true, .snapshot = false, .compaction = false, .atomic_memory_storage = true, .send_after_persist = true });
        } else {
            try self.line(.{ .kind = "header", .version = 1, .voters = .{ 1, 2, 3 }, .pre_vote = false, .check_quorum = false, .transfer = false, .read_index = false, .snapshot = false, .compaction = false, .atomic_memory_storage = true, .send_after_persist = true });
        }
        try self.observe("init", 0, null, "");
    }

    fn deinit(self: *Cluster) void {
        for (&self.machines) |*machine| machine.deinit();
        for (self.captures[0..self.capture_count]) |capture| a.free(capture.value);
        for (&self.trackers, &self.held_reads) |*tracker, *held| {
            tracker.deinit();
            for (held.items) |item| a.free(item.context);
            held.deinit(a);
        }
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
        if (self.read_mode) {
            try self.trace.appendSlice(a, bytes);
            try self.trace.append(a, '\n');
            return;
        }
        var parsed = try std.json.parseFromSlice(std.json.Value, a, bytes, .{ .allocate = .alloc_always });
        defer parsed.deinit();
        if (parsed.value.object.contains("states")) {
            const object = &parsed.value.object;
            for ([_][]const u8{ "pending", "network" }) |key| {
                for (object.getPtr(key).?.array.items) |*m| {
                    if (m.object.get("context").?.string.len != 0) return error.UnsupportedMessageFeature;
                    _ = m.object.swapRemove("context");
                }
            }
            if (object.getPtr("input").?.* != .null) {
                const m = object.getPtr("input").?;
                if (m.object.get("context").?.string.len != 0) return error.UnsupportedMessageFeature;
                _ = m.object.swapRemove("context");
            }
        }
        const encoded = try std.json.Stringify.valueAlloc(a, parsed.value, .{});
        defer a.free(encoded);
        try self.trace.appendSlice(a, encoded);
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
        if (self.read_mode) {
            var reads: [3]ReadObservation = undefined;
            for (&self.nodes, &self.trackers, &self.held_reads, &reads) |*node, *tracker, *held, *out| {
                const r = &node.raft;
                if (r.read_only.option != .safe) return error.UnsupportedReadOption;
                var waiting: std.ArrayList([]const u8) = .empty;
                for (r.pending_read_index_messages.items) |m| try waiting.append(alloc, m.entries[0].data);
                var pending_reads: std.ArrayList(PendingRead) = .empty;
                for (r.read_only.queue.items) |ctx| {
                    const status = r.read_only.pending.getPtr(ctx).?;
                    var ack_ids: std.ArrayList(u64) = .empty;
                    var it = status.acks.keyIterator();
                    while (it.next()) |ack| try ack_ids.append(alloc, ack.*);
                    std.mem.sort(u64, ack_ids.items, {}, std.sort.asc(u64));
                    try pending_reads.append(alloc, .{ .context = ctx, .index = status.index, .acks = ack_ids.items });
                }
                var ready: std.ArrayList(ReadResult) = .empty;
                for (held.items) |item| try ready.append(alloc, item);
                for (r.read_states.items) |item| try ready.append(alloc, .{ .context = item.request_ctx, .index = item.index });
                var tracked: std.ArrayList(TrackedRead) = .empty;
                var completed: std.ArrayList(CompletedRead) = .empty;
                var writes: std.ArrayList(WriteResult) = .empty;
                for (self.writes[0..self.write_count]) |write| {
                    if (write.node == node) try writes.append(alloc, .{ .value = write.value, .completed = write.completed });
                }
                for (self.captures[0..self.capture_count]) |capture| {
                    if (capture.node != node) continue;
                    if (tracker.reads.get(capture.context)) |item| try tracked.append(alloc, .{ .context = capture.context, .index = item.ready_index });
                    for (0..capture.count) |_| try completed.append(alloc, .{ .context = capture.context, .index = capture.applied, .value = capture.value });
                }
                out.* = .{ .waiting = waiting.items, .pending = pending_reads.items, .ready = ready.items, .tracked = tracked.items, .applied = r.raft_log.applied, .completed = completed.items, .value = self.machines[r.id - 1].value, .writes = writes.items };
            }
            try self.line(.{ .kind = kind, .seq = self.seq, .node = id, .input = if (input) |m| try message(alloc, m) else null, .value = value, .states = states, .pending = pending.items, .network = network.items, .reads = reads });
        } else {
            try self.line(.{ .kind = kind, .seq = self.seq, .node = id, .input = if (input) |m| try message(alloc, m) else null, .value = value, .states = states, .pending = pending.items, .network = network.items });
        }
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
            if (rd.snapshot != null or (!self.read_mode and rd.read_states.len != 0)) return error.UnsupportedReady;
            for (rd.read_states) |item| {
                const ctx = try a.dupe(u8, item.request_ctx);
                errdefer a.free(ctx);
                try self.held_reads[id - 1].append(a, .{ .context = ctx, .index = item.index });
            }
            try self.hold(id, rd.messages());
            try self.hold(id, rd.persistedMessages());
            try self.observe("ready", id, null, "");
            try store.append(a, rd.entries);
            if (rd.hs) |hs| try store.setHardState(hs);
            try self.observe("persist", id, null, "");
            var light = if (self.read_mode) try node.advanceAppend(rd) else try node.advance(rd);
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
            if (self.held_reads[id - 1].items.len > 0) {
                if (node.raft.read_states.items.len != 0) return error.UnsupportedReadInterleaving;
                for (self.held_reads[id - 1].items) |item| {
                    self.trackers[id - 1].markReadReady(item.context, item.index);
                    a.free(item.context);
                }
                self.held_reads[id - 1].clearRetainingCapacity();
                try self.observe("read_ready", id, null, "");
            }
        }
    }

    fn campaign(self: *Cluster, id: u64) !void {
        try self.nodes[id - 1].campaign();
        try self.observe("campaign", id, null, "");
        try self.flush(id);
    }

    fn propose(self: *Cluster, id: u64, value: []const u8) !void {
        if (value.len == 0) return error.EmptyProposal;
        if (self.read_mode) {
            if (self.write_count == self.writes.len) return error.WriteLimit;
            for (self.writes[0..self.write_count]) |write| {
                if (std.mem.eql(u8, write.value, value)) return error.DuplicateWriteValue;
            }
            const capture = &self.writes[self.write_count];
            capture.* = .{ .value = value, .node = &self.nodes[id - 1] };
            self.write_count += 1;
            try self.trackers[id - 1].track(value, .{ .ctx = capture, .function = WriteCapture.invoke }, 0, 0);
        }
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

    fn readIndex(self: *Cluster, id: u64, ctx: []const u8) !void {
        if (!self.read_mode or self.capture_count == self.captures.len) return error.UnsupportedRead;
        for (self.captures[0..self.capture_count]) |capture| {
            if (std.mem.eql(u8, capture.context, ctx)) return error.DuplicateRead;
        }
        const capture = &self.captures[self.capture_count];
        capture.* = .{ .context = ctx, .node = &self.nodes[id - 1], .machine = &self.machines[id - 1] };
        self.capture_count += 1;
        try self.trackers[id - 1].trackRead(ctx, .{ .ctx = capture, .function = ReadCapture.invoke }, 0, 0);
        try self.nodes[id - 1].readIndex(ctx);
        try self.observe("read", id, null, ctx);
        try self.flush(id);
    }

    fn apply(self: *Cluster, id: u64, index: u64) !void {
        try std.testing.expect(index <= self.stores[id - 1].core.raft_state.hard_state.commit);
        const node = &self.nodes[id - 1];
        const machine = self.machines[id - 1].stateMachine();
        const start = node.raft.raft_log.applied;
        for (self.stores[id - 1].core.entries.items[@intCast(start)..@intCast(index)]) |entry| {
            if (entry.data.len != 0) {
                var result = try machine.apply(entry);
                defer result.deinit(a);
                node.advanceApplyTo(entry.index);
                self.trackers[id - 1].complete(entry.data, result.response orelse "");
            } else {
                node.advanceApplyTo(entry.index);
            }
        }
        const target = try std.fmt.allocPrint(a, "{}", .{index});
        defer a.free(target);
        try self.observe("apply", id, null, target);
    }

    fn completeReads(self: *Cluster, id: u64) !void {
        self.trackers[id - 1].completeReadyReads(self.nodes[id - 1].raft.raft_log.applied);
        try self.observe("complete_reads", id, null, "");
    }

    fn deliver(self: *Cluster, index: usize) !void {
        var m = self.network.orderedRemove(index);
        defer m.deinit(a);
        if (self.isolated != 0 and (m.from == self.isolated or m.to == self.isolated)) {
            try self.observe("drop", m.to, m, "");
        } else {
            try self.nodes[m.to - 1].step(try raft.cloneMessage(a, m));
            try self.observe("receive", m.to, m, "");
            try self.flush(m.to);
        }
    }

    fn deliverMatching(self: *Cluster, kind: raft.MessageType, from: u64, to: u64, ctx: []const u8) !void {
        for (self.network.items, 0..) |m, i| {
            if (m.msg_type == kind and m.from == from and m.to == to and std.mem.eql(u8, m.context, ctx)) return self.deliver(i);
        }
        return error.MissingMessage;
    }

    fn drain(self: *Cluster) !void {
        var count: usize = 0;
        while (self.network.items.len > 0) {
            count += 1;
            if (count > 500) return error.DeliveryLimit;
            try self.deliver(0);
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

test "tla read trace: current-term gate, batched confirmation and delayed apply" {
    var c = Cluster{ .read_mode = true };
    try c.init();
    defer c.deinit();
    errdefer c.save(".cache/tla/read-batch.failed.ndjson") catch {};
    try c.campaign(1);
    try c.deliverMatching(.request_vote, 1, 2, "");
    try c.deliverMatching(.request_vote_response, 2, 1, "");
    try std.testing.expectEqual(@as(u64, 0), c.nodes[0].raft.raft_log.committed);
    try c.readIndex(1, "deferred");
    try std.testing.expectEqual(@as(usize, 1), c.nodes[0].raft.pendingReadIndexCount());
    try c.drain();
    try c.readIndex(1, "batch-first");
    try c.readIndex(1, "batch-last");
    try c.deliverMatching(.heartbeat, 1, 2, "batch-last");
    for (c.network.items) |m| {
        if (m.msg_type == .heartbeat_response and std.mem.eql(u8, m.context, "batch-last")) {
            const duplicate = try raft.cloneMessage(a, m);
            try c.network.append(a, duplicate);
            try c.observe("duplicate", duplicate.to, duplicate, "");
            break;
        }
    }
    try c.deliverMatching(.heartbeat_response, 2, 1, "batch-last");
    try std.testing.expectEqual(@as(usize, 0), c.nodes[0].raft.read_only.pendingReadCount());
    try c.completeReads(1);
    for (c.captures[0..3]) |capture| try std.testing.expectEqual(@as(usize, 0), capture.count);
    try c.apply(1, 1);
    try c.completeReads(1);
    for (c.captures[0..3]) |capture| try std.testing.expectEqual(@as(usize, 1), capture.count);
    try c.drain();
    try c.propose(1, "after-read");
    try c.drain();
    try c.readIndex(1, "after-write");
    try c.drain();
    try c.completeReads(1);
    try std.testing.expectEqual(@as(usize, 0), c.captures[3].count);
    try c.apply(1, 2);
    try c.completeReads(1);
    try std.testing.expectEqual(@as(usize, 1), c.captures[3].count);
    try std.testing.expectEqualStrings("after-read", c.captures[3].value);
    try std.testing.expectEqual(@as(usize, 1), c.writes[0].completed);
    try c.readIndex(1, "after-completed-write");
    try c.drain();
    try c.completeReads(1);
    try std.testing.expectEqualStrings("after-read", c.captures[4].value);
    try c.readIndex(1, "overlap");
    try c.propose(1, "overlap-write");
    try c.drain();
    try c.apply(1, 3);
    try c.completeReads(1);
    try std.testing.expectEqualStrings("overlap-write", c.captures[5].value);
    try std.testing.expectEqual(@as(usize, 1), c.writes[1].completed);
    c.isolated = 1;
    try c.propose(1, "pending-write");
    try c.drain();
    try std.testing.expectEqual(@as(usize, 0), c.writes[2].completed);
    try c.finish(".cache/tla/read-batch.ndjson", 3);
}

test "tla read trace: isolated old leader cannot confirm a fresh read" {
    var c = Cluster{ .read_mode = true };
    try c.init();
    defer c.deinit();
    errdefer c.save(".cache/tla/read-partition.failed.ndjson") catch {};
    try c.campaign(1);
    try c.drain();
    c.isolated = 1;
    try c.campaign(2);
    try c.drain();
    try c.propose(2, "new-leader-write");
    try c.drain();
    try c.readIndex(1, "isolated");
    try c.drain();
    try c.completeReads(1);
    try std.testing.expectEqual(@as(usize, 0), c.captures[0].count);
    c.isolated = 0;
    try c.beat(2);
    try c.drain();
    try std.testing.expectEqual(@as(usize, 0), c.nodes[0].raft.read_only.pendingReadCount());
    try c.readIndex(2, "new-leader-read");
    try c.drain();
    try c.apply(2, 3);
    try c.completeReads(2);
    try std.testing.expectEqual(@as(usize, 0), c.captures[0].count);
    try std.testing.expectEqual(@as(usize, 1), c.captures[1].count);
    try std.testing.expectEqualStrings("new-leader-write", c.captures[1].value);
    try std.testing.expectEqual(@as(usize, 1), c.writes[0].completed);
    try c.finish(".cache/tla/read-partition.ndjson", 3);
}
