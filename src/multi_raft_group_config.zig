const std = @import("std");

const error_model = @import("core/error.zig");
const raw_node_mod = @import("raw_node.zig");
const raftor_config_mod = @import("raftor_config.zig");
const multi_transport_mod = @import("multi_transport.zig");

const Error = error_model.Error;
const Peer = raw_node_mod.Peer;
const RaftorConfig = raftor_config_mod.RaftorConfig;
const LegacyMembershipMigration = raftor_config_mod.LegacyMembershipMigration;
const LegacySnapshotMembership = raftor_config_mod.LegacySnapshotMembership;
const GroupId = multi_transport_mod.GroupId;

pub const MultiRaftGroupConfig = struct {
    group_id: GroupId,
    raftor: RaftorConfig,
};

pub const OwnedGroupConfig = struct {
    value: MultiRaftGroupConfig,
    owned: bool = true,

    pub fn clone(allocator: std.mem.Allocator, source: MultiRaftGroupConfig) !OwnedGroupConfig {
        var value = source;
        value.raftor.listen_addr = try cloneBytes(allocator, source.raftor.listen_addr);
        errdefer freeBytes(allocator, value.raftor.listen_addr);
        value.raftor.advertise_addr = try cloneBytes(allocator, source.raftor.advertise_addr);
        errdefer freeBytes(allocator, value.raftor.advertise_addr);
        value.raftor.initial_peers = try clonePeers(allocator, source.raftor.initial_peers);
        errdefer freePeers(allocator, value.raftor.initial_peers);
        if (source.raftor.legacy_membership_migration) |migration| {
            value.raftor.legacy_membership_migration = try cloneMigration(allocator, migration);
        }
        errdefer if (value.raftor.legacy_membership_migration) |migration| freeMigration(allocator, migration);
        value.raftor.data_dir = "";
        value.raftor.file_system = null;
        return .{ .value = value };
    }

    pub fn deinit(self: *OwnedGroupConfig, allocator: std.mem.Allocator) void {
        if (!self.owned) return;
        freeBytes(allocator, self.value.raftor.listen_addr);
        freeBytes(allocator, self.value.raftor.advertise_addr);
        freePeers(allocator, self.value.raftor.initial_peers);
        if (self.value.raftor.legacy_membership_migration) |migration| freeMigration(allocator, migration);
        self.owned = false;
    }

    pub fn transfer(self: *OwnedGroupConfig) OwnedGroupConfig {
        std.debug.assert(self.owned);
        const result = self.*;
        self.owned = false;
        return result;
    }
};

pub fn estimatedSize(config: MultiRaftGroupConfig) Error!usize {
    var total: usize = 0;
    total = try addSize(total, config.raftor.listen_addr.len);
    total = try addSize(total, config.raftor.advertise_addr.len);
    total = try addSize(total, try peersSize(config.raftor.initial_peers));
    if (config.raftor.legacy_membership_migration) |migration| {
        total = try addSize(total, try peersSize(migration.peers));
        total = try addSize(total, std.math.mul(usize, migration.retired_node_ids.len, @sizeOf(u64)) catch return error.GroupOperationBackpressure);
        if (migration.snapshot) |snapshot| {
            total = try addSize(total, try peersSize(snapshot.peers));
            total = try addSize(total, std.math.mul(usize, snapshot.retired_node_ids.len, @sizeOf(u64)) catch return error.GroupOperationBackpressure);
        }
    }
    return total;
}

fn cloneMigration(allocator: std.mem.Allocator, source: LegacyMembershipMigration) !LegacyMembershipMigration {
    const peers = try clonePeers(allocator, source.peers);
    errdefer freePeers(allocator, peers);
    const retired = try cloneIds(allocator, source.retired_node_ids);
    errdefer freeIds(allocator, retired);
    var snapshot: ?LegacySnapshotMembership = null;
    if (source.snapshot) |source_snapshot| snapshot = try cloneSnapshotMembership(allocator, source_snapshot);
    return .{
        .peers = peers,
        .retired_node_ids = retired,
        .membership_index = source.membership_index,
        .snapshot = snapshot,
    };
}

fn freeMigration(allocator: std.mem.Allocator, migration: LegacyMembershipMigration) void {
    freePeers(allocator, migration.peers);
    freeIds(allocator, migration.retired_node_ids);
    if (migration.snapshot) |snapshot| {
        freePeers(allocator, snapshot.peers);
        freeIds(allocator, snapshot.retired_node_ids);
    }
}

fn cloneSnapshotMembership(
    allocator: std.mem.Allocator,
    source: LegacySnapshotMembership,
) !LegacySnapshotMembership {
    const peers = try clonePeers(allocator, source.peers);
    errdefer freePeers(allocator, peers);
    return .{
        .peers = peers,
        .retired_node_ids = try cloneIds(allocator, source.retired_node_ids),
    };
}

fn clonePeers(allocator: std.mem.Allocator, source: []const Peer) ![]const Peer {
    if (source.len == 0) return &.{};
    const peers = try allocator.alloc(Peer, source.len);
    var initialized: usize = 0;
    errdefer {
        for (peers[0..initialized]) |peer| if (peer.context) |context| freeBytes(allocator, context);
        allocator.free(peers);
    }
    for (source) |peer| {
        peers[initialized] = .{
            .id = peer.id,
            .context = if (peer.context) |context| try cloneBytes(allocator, context) else null,
        };
        initialized += 1;
    }
    return peers;
}

fn freePeers(allocator: std.mem.Allocator, peers: []const Peer) void {
    if (peers.len == 0) return;
    for (peers) |peer| if (peer.context) |context| freeBytes(allocator, context);
    allocator.free(peers);
}

fn cloneIds(allocator: std.mem.Allocator, source: []const u64) ![]const u64 {
    if (source.len == 0) return &.{};
    return allocator.dupe(u64, source);
}

fn freeIds(allocator: std.mem.Allocator, ids: []const u64) void {
    if (ids.len != 0) allocator.free(ids);
}

fn cloneBytes(allocator: std.mem.Allocator, source: []const u8) ![]const u8 {
    if (source.len == 0) return &.{};
    return allocator.dupe(u8, source);
}

fn freeBytes(allocator: std.mem.Allocator, bytes: []const u8) void {
    if (bytes.len != 0) allocator.free(bytes);
}

fn peersSize(peers: []const Peer) Error!usize {
    var total = std.math.mul(usize, peers.len, @sizeOf(Peer)) catch return error.GroupOperationBackpressure;
    for (peers) |peer| {
        if (peer.context) |context| total = try addSize(total, context.len);
    }
    return total;
}

fn addSize(left: usize, right: usize) Error!usize {
    return std.math.add(usize, left, right) catch error.GroupOperationBackpressure;
}

// KCOV_EXCL_START
test "owned group config deep clones nested migration slices" {
    const allocator = std.testing.allocator;
    const peers = [_]Peer{.{ .id = 1, .context = "one" }};
    const retired = [_]u64{2};
    var owned = try OwnedGroupConfig.clone(allocator, .{
        .group_id = 9,
        .raftor = .{
            .listen_addr = "listen",
            .advertise_addr = "advertise",
            .initial_peers = &peers,
            .legacy_membership_migration = .{
                .peers = &peers,
                .retired_node_ids = &retired,
                .membership_index = 4,
                .snapshot = .{ .peers = &peers, .retired_node_ids = &retired },
            },
        },
    });
    defer owned.deinit(allocator);
    try std.testing.expectEqualStrings("one", owned.value.raftor.initial_peers[0].context.?);
    try std.testing.expect(try estimatedSize(owned.value) > 0);
}
// KCOV_EXCL_STOP
