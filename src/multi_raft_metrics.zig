const std = @import("std");

const error_model = @import("core/error.zig");
const multi_raft_mod = @import("multi_raft.zig");

const Error = error_model.Error;
const MultiRaftHost = multi_raft_mod.MultiRaftHost;
const MultiRaftHostStatus = multi_raft_mod.MultiRaftHostStatus;
const MultiRaftGroupStatus = multi_raft_mod.MultiRaftGroupStatus;

pub const MetricKind = enum {
    gauge,
    counter,
};

pub const AttributeValue = union(enum) {
    string: []const u8,
    integer: u64,
    boolean: bool,
};

pub const MetricAttribute = struct {
    key: []const u8,
    value: AttributeValue,
};

pub const MetricDescriptor = struct {
    name: []const u8,
    prometheus_name: []const u8,
    description: []const u8,
    unit: []const u8 = "1",
    kind: MetricKind,
};

pub const MetricPoint = struct {
    descriptor: MetricDescriptor,
    value: u64,
    attributes: []const MetricAttribute,
};

/// Synchronous adapter surface for an application-owned OpenTelemetry SDK.
pub const OpenTelemetryMetricSink = struct {
    ctx: *anyopaque,
    function: *const fn (ctx: *anyopaque, point: MetricPoint) Error!void,

    pub fn emit(self: OpenTelemetryMetricSink, point: MetricPoint) Error!void {
        return self.function(self.ctx, point);
    }
};

const host_groups = MetricDescriptor{
    .name = "raftz.multi_raft.groups",
    .prometheus_name = "raftz_multi_raft_groups",
    .description = "Number of Raft groups by lifecycle state.",
    .kind = .gauge,
};
const host_queue_items = MetricDescriptor{
    .name = "raftz.multi_raft.queue.items",
    .prometheus_name = "raftz_multi_raft_queue_items",
    .description = "Items waiting in a Multi-Raft host queue.",
    .kind = .gauge,
};
const host_queue_bytes = MetricDescriptor{
    .name = "raftz.multi_raft.queue.bytes",
    .prometheus_name = "raftz_multi_raft_queue_bytes",
    .description = "Bytes retained by a Multi-Raft host queue.",
    .unit = "By",
    .kind = .gauge,
};
const host_iterations = MetricDescriptor{
    .name = "raftz.multi_raft.host.iterations",
    .prometheus_name = "raftz_multi_raft_host_iterations_total",
    .description = "Cumulative Multi-Raft host event-loop iterations.",
    .kind = .counter,
};
const host_scheduling = MetricDescriptor{
    .name = "raftz.multi_raft.scheduling.events",
    .prometheus_name = "raftz_multi_raft_scheduling_events_total",
    .description = "Cumulative Multi-Raft scheduling events.",
    .kind = .counter,
};
const host_transport = MetricDescriptor{
    .name = "raftz.multi_raft.transport.events",
    .prometheus_name = "raftz_multi_raft_transport_events_total",
    .description = "Cumulative shared transport events.",
    .kind = .counter,
};
const host_operations = MetricDescriptor{
    .name = "raftz.multi_raft.group_operations",
    .prometheus_name = "raftz_multi_raft_group_operations_total",
    .description = "Cumulative runtime Group operation events.",
    .kind = .counter,
};
const snapshot_events = MetricDescriptor{
    .name = "raftz.multi_raft.snapshot.events",
    .prometheus_name = "raftz_multi_raft_snapshot_events_total",
    .description = "Cumulative snapshot events.",
    .kind = .counter,
};
const migration_active = MetricDescriptor{
    .name = "raftz.multi_raft.migrations",
    .prometheus_name = "raftz_multi_raft_migrations",
    .description = "Number of active or recovered replica migrations.",
    .kind = .gauge,
};
const migration_events = MetricDescriptor{
    .name = "raftz.multi_raft.migration.events",
    .prometheus_name = "raftz_multi_raft_migration_events_total",
    .description = "Cumulative replica migration events.",
    .kind = .counter,
};
const preparation_events = MetricDescriptor{
    .name = "raftz.multi_raft.preparation.events",
    .prometheus_name = "raftz_multi_raft_preparation_events_total",
    .description = "Cumulative target Group preparation events.",
    .kind = .counter,
};
const host_drop_bytes = MetricDescriptor{
    .name = "raftz.multi_raft.dropped.bytes",
    .prometheus_name = "raftz_multi_raft_dropped_bytes_total",
    .description = "Cumulative bytes dropped by Group ingress quotas.",
    .unit = "By",
    .kind = .counter,
};
const host_events = MetricDescriptor{
    .name = "raftz.multi_raft.host.events",
    .prometheus_name = "raftz_multi_raft_host_events_total",
    .description = "Cumulative miscellaneous Multi-Raft host events.",
    .kind = .counter,
};
const group_info = MetricDescriptor{
    .name = "raftz.multi_raft.group.info",
    .prometheus_name = "raftz_multi_raft_group_info",
    .description = "Static and lifecycle information for one Raft group.",
    .kind = .gauge,
};
const group_node_value = MetricDescriptor{
    .name = "raftz.multi_raft.group.node",
    .prometheus_name = "raftz_multi_raft_group_node",
    .description = "Current Raft node state value.",
    .kind = .gauge,
};
const group_pending_items = MetricDescriptor{
    .name = "raftz.multi_raft.group.pending.items",
    .prometheus_name = "raftz_multi_raft_group_pending_items",
    .description = "Items pending in one Raft group.",
    .kind = .gauge,
};
const group_limit_items = MetricDescriptor{
    .name = "raftz.multi_raft.group.limit.items",
    .prometheus_name = "raftz_multi_raft_group_limit_items",
    .description = "Effective item limit for one Raft group ingress resource.",
    .kind = .gauge,
};
const group_limit_bytes = MetricDescriptor{
    .name = "raftz.multi_raft.group.limit.bytes",
    .prometheus_name = "raftz_multi_raft_group_limit_bytes",
    .description = "Effective byte limit for one Raft group message inbox.",
    .unit = "By",
    .kind = .gauge,
};
const group_pending_bytes = MetricDescriptor{
    .name = "raftz.multi_raft.group.pending.bytes",
    .prometheus_name = "raftz_multi_raft_group_pending_bytes",
    .description = "Bytes pending in one Raft group.",
    .unit = "By",
    .kind = .gauge,
};
const group_scheduling = MetricDescriptor{
    .name = "raftz.multi_raft.group.scheduling.events",
    .prometheus_name = "raftz_multi_raft_group_scheduling_events_total",
    .description = "Cumulative scheduling events for one Raft group.",
    .kind = .counter,
};
const group_transport_events = MetricDescriptor{
    .name = "raftz.multi_raft.group.transport.events",
    .prometheus_name = "raftz_multi_raft_group_transport_events_total",
    .description = "Cumulative ingress quota drops for one Raft group.",
    .kind = .counter,
};
const group_drop_bytes = MetricDescriptor{
    .name = "raftz.multi_raft.group.dropped.bytes",
    .prometheus_name = "raftz_multi_raft_group_dropped_bytes_total",
    .description = "Cumulative message bytes dropped by one Group ingress quota.",
    .unit = "By",
    .kind = .counter,
};
const group_last_iteration = MetricDescriptor{
    .name = "raftz.multi_raft.group.last_host_iteration",
    .prometheus_name = "raftz_multi_raft_group_last_host_iteration",
    .description = "Last host iteration that scheduled one Raft group.",
    .kind = .gauge,
};

/// Emit cumulative counters and gauges using OpenTelemetry-style descriptors.
pub fn exportOpenTelemetry(
    host: *const MultiRaftHost,
    allocator: std.mem.Allocator,
    sink: OpenTelemetryMetricSink,
) Error!void {
    const host_status = host.getHostStatus();
    const groups = try host.listGroupStatuses(allocator);
    defer allocator.free(groups);
    try emitAll(host_status, groups, sink);
}

/// Return an owned Prometheus 0.0.4 text exposition snapshot.
pub fn encodePrometheus(
    host: *const MultiRaftHost,
    allocator: std.mem.Allocator,
) Error![]u8 {
    var output: std.ArrayList(u8) = .empty;
    errdefer output.deinit(allocator);
    var context = PrometheusContext{
        .allocator = allocator,
        .output = &output,
        .descriptors = std.StringHashMap(void).init(allocator),
    };
    defer context.descriptors.deinit();
    try exportOpenTelemetry(host, allocator, context.sink());
    return output.toOwnedSlice(allocator);
}

fn emitAll(
    host: MultiRaftHostStatus,
    groups: []const MultiRaftGroupStatus,
    sink: OpenTelemetryMetricSink,
) Error!void {
    try emitHost(sink, host_groups, host.node_id, host.groups, &.{stringAttribute("state", "total")});
    try emitHost(sink, host_groups, host.node_id, host.active_groups, &.{stringAttribute("state", "active")});
    try emitHost(sink, host_groups, host.node_id, host.retryable_groups, &.{stringAttribute("state", "retryable_error")});
    try emitHost(sink, host_groups, host.node_id, host.terminal_groups, &.{stringAttribute("state", "terminal")});
    try emitHost(sink, host_groups, host.node_id, host.stopping_groups, &.{stringAttribute("state", "stopping")});

    try emitHost(sink, host_queue_items, host.node_id, host.queued_group_operations.count, &.{stringAttribute("queue", "group_operations")});
    try emitHost(sink, host_queue_items, host.node_id, host.queued_group_wakes, &.{stringAttribute("queue", "group_wakes")});
    try emitHost(sink, host_queue_bytes, host.node_id, host.queued_group_operations.bytes, &.{stringAttribute("queue", "group_operations")});

    try emitHost(sink, host_iterations, host.node_id, host.host_iterations, &.{stringAttribute("kind", "all")});
    try emitHost(sink, host_iterations, host.node_id, host.tick_iterations, &.{stringAttribute("kind", "tick")});
    try emitHost(sink, host_iterations, host.node_id, host.poll_iterations, &.{stringAttribute("kind", "poll")});

    try emitHost(sink, host_scheduling, host.node_id, host.groups_driven, &.{stringAttribute("event", "group_driven")});
    try emitHost(sink, host_scheduling, host.node_id, host.priority_polls, &.{stringAttribute("event", "priority_poll")});
    try emitHost(sink, host_scheduling, host.node_id, host.groups_with_work, &.{stringAttribute("event", "productive_group")});
    try emitHost(sink, host_scheduling, host.node_id, host.group_error_iterations, &.{stringAttribute("event", "group_error")});

    try emitHost(sink, host_transport, host.node_id, host.envelopes_routed, &.{stringAttribute("event", "envelope_routed")});
    try emitHost(sink, host_transport, host.node_id, host.peer_events_routed, &.{stringAttribute("event", "peer_event_routed")});
    try emitHost(sink, host_transport, host.node_id, host.wake_queue_drops, &.{stringAttribute("event", "wake_dropped")});

    try emitHost(sink, host_operations, host.node_id, host.group_operations_completed, &.{stringAttribute("event", "completed")});
    try emitHost(sink, host_operations, host.node_id, host.group_operations_failed, &.{stringAttribute("event", "failed")});

    try emitHost(sink, snapshot_events, host.node_id, host.snapshot_attempts, &.{stringAttribute("event", "attempt")});
    try emitHost(sink, snapshot_events, host.node_id, host.snapshot_successes, &.{stringAttribute("event", "success")});
    try emitHost(sink, snapshot_events, host.node_id, host.snapshot_failures, &.{stringAttribute("event", "failure")});

    try emitHost(sink, migration_active, host.node_id, host.active_replica_migrations, &.{stringAttribute("state", "active")});
    try emitHost(sink, migration_active, host.node_id, host.recovered_replica_migrations, &.{stringAttribute("state", "recovered")});
    try emitHost(sink, migration_events, host.node_id, host.replica_migrations_started, &.{stringAttribute("event", "started")});
    try emitHost(sink, migration_events, host.node_id, host.replica_migrations_completed, &.{stringAttribute("event", "completed")});
    try emitHost(sink, migration_events, host.node_id, host.replica_migrations_failed, &.{stringAttribute("event", "failed")});
    try emitHost(sink, migration_events, host.node_id, host.replica_migrations_timed_out, &.{stringAttribute("event", "timed_out")});
    try emitHost(sink, migration_events, host.node_id, host.replica_migrations_cancelled, &.{stringAttribute("event", "cancelled")});
    try emitHost(sink, migration_events, host.node_id, host.replica_migration_leader_transfers, &.{stringAttribute("event", "leader_transfer")});

    try emitHost(sink, preparation_events, host.node_id, host.group_preparation_attempts, &.{stringAttribute("event", "attempt")});
    try emitHost(sink, preparation_events, host.node_id, host.group_preparation_successes, &.{stringAttribute("event", "success")});
    try emitHost(sink, preparation_events, host.node_id, host.group_preparation_failures, &.{stringAttribute("event", "failure")});

    try emitHost(sink, host_events, host.node_id, host.unknown_group_messages, &.{stringAttribute("event", "unknown_group_message")});
    try emitHost(sink, host_events, host.node_id, host.local_group_retirements, &.{stringAttribute("event", "local_group_retirement")});
    try emitHost(sink, host_events, host.node_id, host.group_message_drops, &.{stringAttribute("event", "group_message_dropped")});
    try emitHost(sink, host_events, host.node_id, host.group_peer_event_drops, &.{stringAttribute("event", "group_peer_event_dropped")});
    try emitHost(sink, host_drop_bytes, host.node_id, host.group_message_drop_bytes, &.{stringAttribute("kind", "group_message")});

    for (groups) |group| try emitGroup(group, host.node_id, sink);
}

fn emitGroup(group: MultiRaftGroupStatus, node_id: u64, sink: OpenTelemetryMetricSink) Error!void {
    const lifecycle = @tagName(group.lifecycle);
    const role = @tagName(group.node.role);
    try emitGroupPoint(sink, group_info, node_id, group.group_id, 1, &.{
        stringAttribute("lifecycle", lifecycle),
        stringAttribute("role", role),
        booleanAttribute("locally_retired", group.locally_retired),
        booleanAttribute("auto_prepared", group.auto_prepared),
    });
    try emitGroupPoint(sink, group_node_value, node_id, group.group_id, group.node.term, &.{stringAttribute("kind", "term")});
    try emitGroupPoint(sink, group_node_value, node_id, group.group_id, group.node.leader_id, &.{stringAttribute("kind", "leader_id")});
    try emitGroupPoint(sink, group_node_value, node_id, group.group_id, group.node.commit_index, &.{stringAttribute("kind", "commit_index")});
    try emitGroupPoint(sink, group_node_value, node_id, group.group_id, group.node.applied_index, &.{stringAttribute("kind", "applied_index")});
    try emitGroupPoint(sink, group_node_value, node_id, group.group_id, group.node.incarnation, &.{stringAttribute("kind", "incarnation")});

    try emitGroupPoint(sink, group_pending_items, node_id, group.group_id, group.pending_messages, &.{stringAttribute("kind", "messages")});
    try emitGroupPoint(sink, group_pending_items, node_id, group.group_id, group.pending_peer_events, &.{stringAttribute("kind", "peer_events")});
    try emitGroupPoint(sink, group_pending_items, node_id, group.group_id, group.node.pending_proposals, &.{stringAttribute("kind", "pending_proposals")});
    try emitGroupPoint(sink, group_pending_items, node_id, group.group_id, group.node.queued_proposals, &.{stringAttribute("kind", "queued_proposals")});
    try emitGroupPoint(sink, group_pending_items, node_id, group.group_id, group.node.queued_read_indexes, &.{stringAttribute("kind", "queued_read_indexes")});
    try emitGroupPoint(sink, group_limit_items, node_id, group.group_id, group.max_inbox_messages, &.{stringAttribute("kind", "messages")});
    try emitGroupPoint(sink, group_limit_items, node_id, group.group_id, group.max_peer_events, &.{stringAttribute("kind", "peer_events")});
    try emitGroupPoint(sink, group_limit_bytes, node_id, group.group_id, group.max_inbox_bytes, &.{stringAttribute("kind", "messages")});
    try emitGroupPoint(sink, group_pending_bytes, node_id, group.group_id, group.pending_message_bytes, &.{stringAttribute("kind", "messages")});
    try emitGroupPoint(sink, group_pending_bytes, node_id, group.group_id, group.node.queued_proposal_bytes, &.{stringAttribute("kind", "proposals")});
    try emitGroupPoint(sink, group_pending_bytes, node_id, group.group_id, group.node.queued_read_index_bytes, &.{stringAttribute("kind", "read_indexes")});

    try emitGroupPoint(sink, group_scheduling, node_id, group.group_id, group.scheduled_iterations, &.{stringAttribute("event", "scheduled")});
    try emitGroupPoint(sink, group_scheduling, node_id, group.group_id, group.productive_iterations, &.{stringAttribute("event", "productive")});
    try emitGroupPoint(sink, group_scheduling, node_id, group.group_id, group.error_iterations, &.{stringAttribute("event", "error")});
    try emitGroupPoint(sink, group_scheduling, node_id, group.group_id, group.priority_polls, &.{stringAttribute("event", "priority_poll")});
    try emitGroupPoint(sink, group_last_iteration, node_id, group.group_id, group.last_host_iteration, &.{});
    try emitGroupPoint(sink, group_transport_events, node_id, group.group_id, group.dropped_messages, &.{stringAttribute("event", "message_dropped")});
    try emitGroupPoint(sink, group_transport_events, node_id, group.group_id, group.dropped_peer_events, &.{stringAttribute("event", "peer_event_dropped")});
    try emitGroupPoint(sink, group_drop_bytes, node_id, group.group_id, group.dropped_message_bytes, &.{stringAttribute("kind", "message")});

    try emitGroupPoint(sink, snapshot_events, node_id, group.group_id, group.snapshot_attempts, &.{stringAttribute("event", "attempt")});
    try emitGroupPoint(sink, snapshot_events, node_id, group.group_id, group.snapshot_successes, &.{stringAttribute("event", "success")});
    try emitGroupPoint(sink, snapshot_events, node_id, group.group_id, group.snapshot_failures, &.{stringAttribute("event", "failure")});
}

fn emitHost(
    sink: OpenTelemetryMetricSink,
    descriptor: MetricDescriptor,
    node_id: u64,
    value: anytype,
    extra: []const MetricAttribute,
) Error!void {
    var attributes: [8]MetricAttribute = undefined;
    attributes[0] = integerAttribute("node_id", node_id);
    @memcpy(attributes[1 .. 1 + extra.len], extra);
    try sink.emit(.{
        .descriptor = descriptor,
        .value = @intCast(value),
        .attributes = attributes[0 .. 1 + extra.len],
    });
}

fn emitGroupPoint(
    sink: OpenTelemetryMetricSink,
    descriptor: MetricDescriptor,
    node_id: u64,
    group_id: u64,
    value: anytype,
    extra: []const MetricAttribute,
) Error!void {
    var attributes: [8]MetricAttribute = undefined;
    attributes[0] = integerAttribute("node_id", node_id);
    attributes[1] = integerAttribute("group_id", group_id);
    @memcpy(attributes[2 .. 2 + extra.len], extra);
    try sink.emit(.{
        .descriptor = descriptor,
        .value = @intCast(value),
        .attributes = attributes[0 .. 2 + extra.len],
    });
}

fn stringAttribute(key: []const u8, value: []const u8) MetricAttribute {
    return .{ .key = key, .value = .{ .string = value } };
}

fn integerAttribute(key: []const u8, value: u64) MetricAttribute {
    return .{ .key = key, .value = .{ .integer = value } };
}

fn booleanAttribute(key: []const u8, value: bool) MetricAttribute {
    return .{ .key = key, .value = .{ .boolean = value } };
}

const PrometheusContext = struct {
    allocator: std.mem.Allocator,
    output: *std.ArrayList(u8),
    descriptors: std.StringHashMap(void),

    fn sink(self: *PrometheusContext) OpenTelemetryMetricSink {
        return .{ .ctx = self, .function = emit };
    }

    fn emit(ctx: *anyopaque, point: MetricPoint) Error!void {
        const self: *PrometheusContext = @ptrCast(@alignCast(ctx));
        if (!self.descriptors.contains(point.descriptor.prometheus_name)) {
            try self.descriptors.put(point.descriptor.prometheus_name, {});
            try self.output.appendSlice(self.allocator, "# HELP ");
            try self.output.appendSlice(self.allocator, point.descriptor.prometheus_name);
            try self.output.append(self.allocator, ' ');
            try appendHelp(self.allocator, self.output, point.descriptor.description);
            try self.output.append(self.allocator, '\n');
            try self.output.appendSlice(self.allocator, "# TYPE ");
            try self.output.appendSlice(self.allocator, point.descriptor.prometheus_name);
            try self.output.append(self.allocator, ' ');
            try self.output.appendSlice(self.allocator, @tagName(point.descriptor.kind));
            try self.output.append(self.allocator, '\n');
        }
        try self.output.appendSlice(self.allocator, point.descriptor.prometheus_name);
        if (point.attributes.len != 0) {
            try self.output.append(self.allocator, '{');
            for (point.attributes, 0..) |attribute, index| {
                if (index != 0) try self.output.append(self.allocator, ',');
                try self.output.appendSlice(self.allocator, attribute.key);
                try self.output.appendSlice(self.allocator, "=\"");
                try appendAttributeValue(self.allocator, self.output, attribute.value);
                try self.output.append(self.allocator, '"');
            }
            try self.output.append(self.allocator, '}');
        }
        try self.output.append(self.allocator, ' ');
        try appendUnsigned(self.allocator, self.output, point.value);
        try self.output.append(self.allocator, '\n');
    }
};

fn appendHelp(allocator: std.mem.Allocator, output: *std.ArrayList(u8), value: []const u8) Error!void {
    for (value) |byte| switch (byte) {
        '\\' => try output.appendSlice(allocator, "\\\\"),
        '\n' => try output.appendSlice(allocator, "\\n"),
        else => try output.append(allocator, byte),
    };
}

fn appendAttributeValue(
    allocator: std.mem.Allocator,
    output: *std.ArrayList(u8),
    value: AttributeValue,
) Error!void {
    switch (value) {
        .string => |string| for (string) |byte| switch (byte) {
            '\\' => try output.appendSlice(allocator, "\\\\"),
            '"' => try output.appendSlice(allocator, "\\\""),
            '\n' => try output.appendSlice(allocator, "\\n"),
            else => try output.append(allocator, byte),
        },
        .integer => |integer| try appendUnsigned(allocator, output, integer),
        .boolean => |boolean| try output.appendSlice(allocator, if (boolean) "true" else "false"),
    }
}

fn appendUnsigned(allocator: std.mem.Allocator, output: *std.ArrayList(u8), value: u64) Error!void {
    var buffer: [32]u8 = undefined;
    const formatted = std.fmt.bufPrint(&buffer, "{}", .{value}) catch unreachable;
    try output.appendSlice(allocator, formatted);
}
