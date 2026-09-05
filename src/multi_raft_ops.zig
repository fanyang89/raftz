//! Human-readable operational report for a Multi-Raft Host.
//!
//! The report is a stable, greppable key=value text snapshot intended for
//! runbooks, logs, and CLI output. Machine-readable consumers should use the
//! Prometheus or OpenTelemetry exporters instead.

const std = @import("std");

const error_model = @import("core/error.zig");
const multi_raft_mod = @import("multi_raft.zig");

const Error = error_model.Error;
const MultiRaftHost = multi_raft_mod.MultiRaftHost;
const MultiRaftGroupStatus = multi_raft_mod.MultiRaftGroupStatus;
const MultiRaftHostStatus = multi_raft_mod.MultiRaftHostStatus;
const ReplicaMigrationStatus = multi_raft_mod.ReplicaMigrationStatus;

/// Return an owned one-line-per-entity operations report.
pub fn encodeOpsReport(
    host: *const MultiRaftHost,
    allocator: std.mem.Allocator,
) Error![]u8 {
    var output: std.ArrayList(u8) = .empty;
    errdefer output.deinit(allocator);

    const host_status = host.getHostStatus();
    try appendHost(allocator, &output, host_status);

    const groups = try host.listGroupStatuses(allocator);
    defer allocator.free(groups);
    for (groups) |group| try appendGroup(allocator, &output, group);

    const migrations = try host.listReplicaMigrations(allocator);
    defer allocator.free(migrations);
    for (migrations) |migration| try appendMigration(allocator, &output, migration);

    return output.toOwnedSlice(allocator);
}

fn appendHost(allocator: std.mem.Allocator, output: *std.ArrayList(u8), host: MultiRaftHostStatus) Error!void {
    try format(allocator, output, "raftz node={d} groups={d} active={d} retryable={d} terminal={d} stopping={d}\n", .{ host.node_id, host.groups, host.active_groups, host.retryable_groups, host.terminal_groups, host.stopping_groups });
    try format(allocator, output, "host iterations={d} tick={d} poll={d} driven={d} productive={d} errors={d}\n", .{ host.host_iterations, host.tick_iterations, host.poll_iterations, host.groups_driven, host.groups_with_work, host.group_error_iterations });
    try format(allocator, output, "host queues operations={d} operation_bytes={d} wakes={d} wake_drops={d} unknown_messages={d}\n", .{ host.queued_group_operations.count, host.queued_group_operations.bytes, host.queued_group_wakes, host.wake_queue_drops, host.unknown_group_messages });
    try format(allocator, output, "transport envelopes={d} peer_events={d}\n", .{ host.envelopes_routed, host.peer_events_routed });
    try format(allocator, output, "operations completed={d} failed={d}\n", .{ host.group_operations_completed, host.group_operations_failed });
    try format(allocator, output, "snapshots attempts={d} successes={d} failures={d}\n", .{ host.snapshot_attempts, host.snapshot_successes, host.snapshot_failures });
    try format(allocator, output, "migrations active={d} recovered={d} started={d} completed={d} failed={d} timed_out={d} cancelled={d} leader_transfers={d}\n", .{ host.active_replica_migrations, host.recovered_replica_migrations, host.replica_migrations_started, host.replica_migrations_completed, host.replica_migrations_failed, host.replica_migrations_timed_out, host.replica_migrations_cancelled, host.replica_migration_leader_transfers });
    try format(allocator, output, "preparations attempts={d} successes={d} failures={d}\n", .{ host.group_preparation_attempts, host.group_preparation_successes, host.group_preparation_failures });
    try format(allocator, output, "drops messages={d} message_bytes={d} peer_events={d} retirements={d}\n", .{ host.group_message_drops, host.group_message_drop_bytes, host.group_peer_event_drops, host.local_group_retirements });
}

fn appendGroup(allocator: std.mem.Allocator, output: *std.ArrayList(u8), group: MultiRaftGroupStatus) Error!void {
    const lag = group.node.commit_index -| group.node.applied_index;
    try format(allocator, output, "group id={d} lifecycle={s} role={s} term={d} leader={d} commit={d} applied={d} lag={d}\n", .{ group.group_id, @tagName(group.lifecycle), @tagName(group.node.role), group.node.term, group.node.leader_id, group.node.commit_index, group.node.applied_index, lag });
    try format(allocator, output, "group id={d} pending messages={d} message_bytes={d} peer_events={d} proposals={d} proposal_bytes={d} reads={d} read_bytes={d}\n", .{ group.group_id, group.pending_messages, group.pending_message_bytes, group.pending_peer_events, group.node.queued_proposals, group.node.queued_proposal_bytes, group.node.queued_read_indexes, group.node.queued_read_index_bytes });
    try format(allocator, output, "group id={d} quota inbox_messages={d}/{d} inbox_bytes={d}/{d} peer_events={d}/{d}\n", .{ group.group_id, group.pending_messages, group.max_inbox_messages, group.pending_message_bytes, group.max_inbox_bytes, group.pending_peer_events, group.max_peer_events });
    try format(allocator, output, "group id={d} counters scheduled={d} productive={d} errors={d} priority={d} last_iteration={d} snapshot_attempts={d} snapshot_failures={d} dropped_messages={d} dropped_message_bytes={d} dropped_peer_events={d}\n", .{ group.group_id, group.scheduled_iterations, group.productive_iterations, group.error_iterations, group.priority_polls, group.last_host_iteration, group.snapshot_attempts, group.snapshot_failures, group.dropped_messages, group.dropped_message_bytes, group.dropped_peer_events });
    const last_error = if (group.last_error) |err| error_model.name(err) else "none";
    try format(allocator, output, "group id={d} flags auto_prepared={} locally_retired={} last_error={s}\n", .{ group.group_id, group.auto_prepared, group.locally_retired, last_error });
}

fn appendMigration(allocator: std.mem.Allocator, output: *std.ArrayList(u8), migration: ReplicaMigrationStatus) Error!void {
    try format(allocator, output, "migration id={d} stage={s} source={d} target={d} elapsed={d}/{d} stable={d}/{d} matched={d} leader_commit={d} recent={}\n", .{ migration.group_id, @tagName(migration.stage), migration.source_node_id, migration.target_node_id, migration.elapsed_ticks, migration.timeout_ticks, migration.stable_catch_up_ticks, migration.required_stable_ticks, migration.target_matched, migration.leader_commit, migration.target_recent_active });
}

fn format(
    allocator: std.mem.Allocator,
    output: *std.ArrayList(u8),
    comptime template: []const u8,
    args: anytype,
) Error!void {
    const max_line = 1024;
    var buffer: [max_line]u8 = undefined;
    const written = std.fmt.bufPrint(&buffer, template, args) catch return error.InvalidConfig;
    try output.appendSlice(allocator, written);
}
