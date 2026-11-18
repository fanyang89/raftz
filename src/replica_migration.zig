const std = @import("std");
const Error = @import("core/error.zig").Error;
const GroupId = @import("multi_transport.zig").GroupId;
const Probe = @import("raftor.zig").ReplicaMigrationProbe;

pub const ReplicaMigrationRequest = struct {
    group_id: GroupId,
    source_node_id: u64,
    target_node_id: u64,
    target_address: []const u8,
    timeout_ticks: u64 = 600,
    stable_catch_up_ticks: u32 = 2,
};

pub const ReplicaMigrationStage = enum(u8) {
    adding_learner,
    waiting_learner,
    catching_up,
    waiting_voter,
    stabilizing_voter,
    waiting_target_leader,
    waiting_source_removal,
};

pub const ReplicaMigrationStatus = struct {
    group_id: GroupId,
    source_node_id: u64,
    target_node_id: u64,
    stage: ReplicaMigrationStage,
    elapsed_ticks: u64,
    timeout_ticks: u64,
    stable_catch_up_ticks: u32,
    required_stable_ticks: u32,
    target_matched: u64,
    leader_commit: u64,
    target_recent_active: bool,
};

pub const ReplicaMigrationResult = struct {
    group_id: GroupId,
    source_node_id: u64,
    target_node_id: u64,
    err: ?Error = null,
};

pub const ReplicaMigrationCallback = struct {
    ctx: *anyopaque,
    function: *const fn (ctx: *anyopaque, result: ReplicaMigrationResult) void,

    pub fn invoke(self: ReplicaMigrationCallback, result: ReplicaMigrationResult) void {
        self.function(self.ctx, result);
    }
};

pub const StepResult = union(enum) {
    idle,
    worked,
    completed: ?Error,
};
const Action = enum { add_learner, promote, transfer, remove };
const Decision = union(enum) { result: StepResult, action: Action };
const idle = Decision{ .result = .idle };
const conflict = Decision{ .result = .{ .completed = error.ReplicaMigrationConflict } };

pub const ReplicaMigration = struct {
    group_id: GroupId,
    source_node_id: u64,
    target_node_id: u64,
    target_address: []u8,
    timeout_ticks: u64,
    required_stable_ticks: u32,
    start_tick: u64,
    callback: ReplicaMigrationCallback,
    stage: std.atomic.Value(ReplicaMigrationStage) = std.atomic.Value(ReplicaMigrationStage).init(.adding_learner),
    stable_ticks: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    target_matched: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    leader_commit: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    target_recent_active: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    cancel_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    source_removal_submitted: bool = false,
    allocator: std.mem.Allocator,

    pub fn create(
        allocator: std.mem.Allocator,
        request: ReplicaMigrationRequest,
        start_tick: u64,
        callback: ReplicaMigrationCallback,
    ) Error!*ReplicaMigration {
        const self = try allocator.create(ReplicaMigration);
        errdefer allocator.destroy(self);
        self.* = .{
            .group_id = request.group_id,
            .source_node_id = request.source_node_id,
            .target_node_id = request.target_node_id,
            .target_address = try allocator.dupe(u8, request.target_address),
            .timeout_ticks = request.timeout_ticks,
            .required_stable_ticks = request.stable_catch_up_ticks,
            .start_tick = start_tick,
            .callback = callback,
            .allocator = allocator,
        };
        return self;
    }

    pub fn destroy(self: *ReplicaMigration) void {
        self.allocator.free(self.target_address);
        self.allocator.destroy(self);
    }

    pub fn status(self: *const ReplicaMigration, now: u64) ReplicaMigrationStatus {
        return .{
            .group_id = self.group_id,
            .source_node_id = self.source_node_id,
            .target_node_id = self.target_node_id,
            .stage = self.stage.load(.acquire),
            .elapsed_ticks = now -| self.start_tick,
            .timeout_ticks = self.timeout_ticks,
            .stable_catch_up_ticks = self.stable_ticks.load(.acquire),
            .required_stable_ticks = self.required_stable_ticks,
            .target_matched = self.target_matched.load(.acquire),
            .leader_commit = self.leader_commit.load(.acquire),
            .target_recent_active = self.target_recent_active.load(.acquire),
        };
    }

    fn advance(self: *ReplicaMigration, stage: ReplicaMigrationStage) Decision {
        self.stage.store(stage, .release);
        self.stable_ticks.store(0, .release);
        return .{ .result = .worked };
    }

    pub fn observe(self: *ReplicaMigration, probe: Probe, tick: bool, local_id: u64, forwarding: bool) Decision {
        self.target_matched.store(probe.target_matched, .release);
        self.leader_commit.store(probe.commit_index, .release);
        self.target_recent_active.store(probe.target_recent_active, .release);
        if (probe.target_role != null and !probe.target_address_matches) return conflict;
        if (probe.source_role == null) return if (probe.target_role == .voter) .{ .result = .{ .completed = null } } else conflict;
        if (probe.source_role != .voter) return conflict;
        if (probe.joint_configuration) return idle;

        const stage = self.stage.load(.acquire);
        const leader = probe.local_role == .leader;
        if (!leader and stage != .waiting_target_leader and stage != .waiting_source_removal) {
            self.stable_ticks.store(0, .release);
            return idle;
        }
        switch (stage) {
            .adding_learner, .waiting_learner => {
                if (probe.target_role) |role| return self.advance(if (role == .learner) .catching_up else .stabilizing_voter);
                if (probe.pending_conf_change) return idle;
                if (stage == .adding_learner) return .{ .action = .add_learner };
                self.stage.store(.adding_learner, .release);
                return idle;
            },
            .catching_up, .waiting_voter => {
                if (probe.target_role == .voter) return self.advance(.stabilizing_voter);
                if (probe.target_role != .learner) return conflict;
                if (probe.pending_conf_change) return idle;
                if (stage == .waiting_voter) return self.advance(.catching_up);
                return if (self.caughtUp(probe, tick)) .{ .action = .promote } else idle;
            },
            .stabilizing_voter, .waiting_target_leader, .waiting_source_removal => {
                if (probe.target_role != .voter) return conflict;
                if (probe.pending_conf_change) return idle;
                switch (stage) {
                    .stabilizing_voter => {
                        if (!self.caughtUp(probe, tick)) return idle;
                        if (self.source_node_id == local_id and probe.leader_id == local_id and forwarding)
                            return .{ .action = .transfer };
                    },
                    .waiting_target_leader => if (probe.leader_id != self.target_node_id)
                        return if (leader) .{ .action = .transfer } else idle,
                    .waiting_source_removal => if (self.source_removal_submitted and !leader) return idle,
                    else => unreachable,
                }
                return if (leader or forwarding) .{ .action = .remove } else idle;
            },
        }
    }

    pub fn finishAction(self: *ReplicaMigration, action: Action, outcome: Error!void) StepResult {
        outcome catch |err| return if (err == error.ProposalDropped) .idle else .{ .completed = err };
        if (action == .remove) self.source_removal_submitted = true;
        return self.advance(switch (action) {
            .add_learner => .waiting_learner,
            .promote => .waiting_voter,
            .transfer => .waiting_target_leader,
            .remove => .waiting_source_removal,
        }).result;
    }

    fn caughtUp(
        migration: *ReplicaMigration,
        probe: Probe,
        advance_clock: bool,
    ) bool {
        if (!probe.target_recent_active or probe.target_matched < probe.commit_index) {
            migration.stable_ticks.store(0, .release);
            return false;
        }
        var stable = migration.stable_ticks.load(.acquire);
        if (advance_clock and stable < std.math.maxInt(u32)) {
            stable += 1;
            migration.stable_ticks.store(stable, .release);
        }
        return stable >= migration.required_stable_ticks;
    }
};

test "learner promotion counts ticks and retries until accepted then observed" {
    const migration = try ReplicaMigration.create(std.testing.allocator, .{
        .group_id = 1,
        .source_node_id = 2,
        .target_node_id = 3,
        .target_address = "target",
        .stable_catch_up_ticks = 2,
    }, 0, undefined);
    defer migration.destroy();
    var probe = Probe{
        .local_role = .leader,
        .leader_id = 1,
        .commit_index = 10,
        .pending_conf_change = false,
        .joint_configuration = false,
        .source_role = .voter,
        .target_role = .learner,
        .target_address_matches = true,
        .target_matched = 10,
        .target_recent_active = true,
    };
    try std.testing.expectEqual(StepResult.worked, migration.observe(probe, false, 1, true).result);
    try std.testing.expectEqual(idle, migration.observe(probe, true, 1, true));
    for (0..4) |_| try std.testing.expectEqual(idle, migration.observe(probe, false, 1, true));
    try std.testing.expectEqual(1, migration.status(1).stable_catch_up_ticks);

    const first = migration.observe(probe, true, 1, true).action;
    try std.testing.expectEqual(Action.promote, first);
    try std.testing.expectEqual(StepResult.idle, migration.finishAction(first, error.ProposalDropped));
    const retry = migration.observe(probe, false, 1, true).action;
    try std.testing.expectEqual(Action.promote, retry);
    try std.testing.expectEqual(StepResult.worked, migration.finishAction(retry, {}));
    probe.pending_conf_change = true;
    try std.testing.expectEqual(idle, migration.observe(probe, true, 1, true));
    try std.testing.expectEqual(ReplicaMigrationStage.waiting_voter, migration.status(3).stage);
    probe.pending_conf_change = false;
    probe.target_role = .voter;
    try std.testing.expectEqual(StepResult.worked, migration.observe(probe, false, 1, true).result);
    try std.testing.expectEqual(ReplicaMigrationStage.stabilizing_voter, migration.status(3).stage);
}
