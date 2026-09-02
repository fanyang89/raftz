//! Ready processing pipeline: drives RawNode → Ready → persist → apply → advance.
//!
//! The default path processes one Ready synchronously. The opt-in Async Ready
//! path stages storage-visible Ready writes with `advanceAppendAsync`, groups
//! their durability barrier, and acknowledges the group with `onPersistReady`.
//! Both paths preserve the same persistence/apply ordering:
//!
//!   1. Validate entries (optional CRC32C checksum)
//!   2. Persist an incoming snapshot as the new storage baseline
//!   3. Persist unstable entries after that snapshot
//!   4. Persist HardState (if changed) and sync storage
//!   5. Restore the durable snapshot into the StateMachine
//!   6. Send persistence-dependent outbound messages via transport
//!   7. Apply committed entries and complete read-index states
//!   8. Advance RawNode + process light ready (more committed entries + messages)

const std = @import("std");

const error_model = @import("core/error.zig");
const types = @import("core/types.zig");
const util = @import("core/util.zig");
const storage_mod = @import("storage.zig");
const raw_node_mod = @import("raw_node.zig");
const state_machine_mod = @import("state_machine.zig");
const transport_mod = @import("transport.zig");
const proposal_tracker_mod = @import("proposal_tracker.zig");
const state_role_mod = @import("core/state_role.zig");
const cluster_membership_mod = @import("cluster_membership.zig");

const Error = error_model.Error;
const Entry = types.Entry;
const EntryType = types.EntryType;
const Message = types.Message;
const ConfChangeV2 = types.ConfChangeV2;
const Snapshot = types.Snapshot;
const SnapshotMetadata = types.SnapshotMetadata;
const ReadState = @import("read_only.zig").ReadState;

const RawNode = raw_node_mod.RawNode;
const Ready = raw_node_mod.Ready;
const LightReady = raw_node_mod.LightReady;
const WritableStorage = storage_mod.WritableStorage;
const StateMachine = state_machine_mod.StateMachine;
const Transport = transport_mod.Transport;
const ProposalTracker = proposal_tracker_mod.ProposalTracker;
const StateRole = state_role_mod.StateRole;
const ClusterMembership = cluster_membership_mod.ClusterMembership;

const log = @import("grpc_lite").log;

pub const ReadyPhase = enum {
    validate,
    persist_snapshot,
    persist_entries,
    persist_hard_state,
    sync,
    restore_snapshot,
    send_messages,
    apply_committed,
    complete_reads,
    advance,
    persist_advanced_hard_state,
    sync_advanced_hard_state,
    send_advanced_messages,
    apply_advanced_committed,
    advance_apply,
    advance_append_async,
    awaiting_flush,
};

const PendingReady = struct {
    ready: Ready,
    light_ready: LightReady = .{},
    snapshot_membership: ?ClusterMembership = null,
    phase: ReadyPhase = .validate,

    fn deinit(self: *PendingReady, allocator: std.mem.Allocator) void {
        if (self.snapshot_membership) |*membership| membership.deinit(allocator);
        self.light_ready.deinit(allocator);
        self.ready.deinit(allocator);
    }
};

pub const ReadyProcessor = struct {
    raw_node: *RawNode,
    storage: WritableStorage,
    state_machine: StateMachine,
    transport: Transport,
    proposal_tracker: *ProposalTracker,
    node_id: u64,
    applied_index: u64,
    cluster_membership: ?ClusterMembership,
    membership_index: u64,
    prev_role: StateRole,
    prev_leader: u64,
    prev_term: u64,
    fatal_error: ?Error,
    fatal_after_ready: ?Error,
    pending: ?PendingReady,
    async_staged: std.ArrayList(PendingReady),
    async_ready: bool,
    async_ready_max_inflight: usize,
    async_flush_required: bool,
    checksum_enabled: bool,
    durable_state_machine: bool,
    allocator: std.mem.Allocator,

    pub fn init(
        allocator: std.mem.Allocator,
        raw_node: *RawNode,
        storage: WritableStorage,
        state_machine: StateMachine,
        transport: Transport,
        proposal_tracker: *ProposalTracker,
        node_id: u64,
        checksum_enabled: bool,
        initial_applied_index: u64,
        initial_membership: ?ClusterMembership,
        initial_membership_index: u64,
        async_ready: bool,
        async_ready_max_inflight: usize,
    ) ReadyProcessor {
        const ss = raw_node.raftConst().softState();
        return .{
            .raw_node = raw_node,
            .storage = storage,
            .state_machine = state_machine,
            .transport = transport,
            .proposal_tracker = proposal_tracker,
            .node_id = node_id,
            .applied_index = initial_applied_index,
            .cluster_membership = initial_membership,
            .membership_index = initial_membership_index,
            .prev_role = ss.role,
            .prev_leader = ss.leader_id,
            .prev_term = raw_node.raftConst().term,
            .fatal_error = null,
            .fatal_after_ready = null,
            .pending = null,
            .async_staged = .empty,
            .async_ready = async_ready,
            .async_ready_max_inflight = async_ready_max_inflight,
            .async_flush_required = false,
            .checksum_enabled = checksum_enabled,
            .durable_state_machine = state_machine.supportsDurableApplied(),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ReadyProcessor) void {
        if (self.pending) |*pending| pending.deinit(self.allocator);
        self.pending = null;
        for (self.async_staged.items) |*pending| pending.deinit(self.allocator);
        self.async_staged.deinit(self.allocator);
        if (self.cluster_membership) |*membership| membership.deinit(self.allocator);
        self.cluster_membership = null;
    }

    pub fn isLeader(self: ReadyProcessor) bool {
        return self.prev_role == .leader;
    }

    pub fn getLeaderId(self: ReadyProcessor) u64 {
        return self.prev_leader;
    }

    pub fn getAppliedIndex(self: ReadyProcessor) u64 {
        return self.applied_index;
    }

    pub fn getClusterMembership(self: *const ReadyProcessor) ?*const ClusterMembership {
        return if (self.cluster_membership) |*membership| membership else null;
    }

    pub fn getMembershipIndex(self: ReadyProcessor) u64 {
        return self.membership_index;
    }

    pub fn hydrateTransport(self: *ReadyProcessor) Error!void {
        const membership = self.cluster_membership orelse return;
        try self.reconcileTransport(null, membership);
    }

    pub fn phase(self: ReadyProcessor) ?ReadyPhase {
        if (self.pending) |pending| return pending.phase;
        if (self.async_staged.items.len != 0) return .awaiting_flush;
        return null;
    }

    pub fn hasStagedReady(self: ReadyProcessor) bool {
        return self.async_staged.items.len != 0;
    }

    pub fn canAcceptInput(self: ReadyProcessor) bool {
        return self.pending == null and !self.async_flush_required;
    }

    pub fn terminalError(self: ReadyProcessor) ?Error {
        return self.fatal_error;
    }

    /// Process Ready work without forcing an async group-commit barrier.
    pub fn process(self: *ReadyProcessor) Error!bool {
        if (self.fatal_error) |e| return e;
        if (self.async_ready) return self.stageAsyncReadies();
        if (!try self.processStepSync()) return false;
        while (self.pending != null) _ = try self.processStepSync();
        return true;
    }

    /// Flush every staged async Ready and process work unlocked by persistence.
    pub fn flush(self: *ReadyProcessor) Error!bool {
        if (self.fatal_error) |e| return e;
        if (!self.async_ready) return false;

        var had_work = false;
        while (true) {
            if (try self.stageAsyncReadies()) had_work = true;
            if (self.async_staged.items.len == 0) return had_work;
            try self.flushAsyncReadies();
            had_work = true;
        }
    }

    /// Advance one phase of the current Ready cycle.
    pub fn processStep(self: *ReadyProcessor) Error!bool {
        if (!self.async_ready) return self.processStepSync();
        if (self.fatal_error) |e| return e;
        if (self.pending != null) return self.processStepAsync();
        if (self.async_flush_required) {
            try self.flushAsyncReadies();
            return true;
        }
        if (self.raw_node.*.hasReady()) {
            self.pending = .{ .ready = try self.raw_node.*.getReady() };
            self.checkLeadershipChange(self.pending.?.ready);
            return true;
        }
        if (self.async_staged.items.len != 0) {
            try self.flushAsyncReadies();
            return true;
        }
        return false;
    }

    fn processStepSync(self: *ReadyProcessor) Error!bool {
        if (self.fatal_error) |e| return e;
        if (self.pending == null) {
            if (!self.raw_node.*.hasReady()) return false;
            self.pending = .{ .ready = try self.raw_node.*.getReady() };
            self.checkLeadershipChange(self.pending.?.ready);
            return true;
        }

        const pending = &self.pending.?;
        switch (pending.phase) {
            .validate => {
                if (self.checksum_enabled) try self.validateEntries(pending.ready.entries);
                pending.phase = .persist_snapshot;
            },
            .persist_snapshot => {
                if (pending.ready.snapshot) |snapshot| {
                    if (snapshot.metadata.index > 0) {
                        pending.snapshot_membership = try self.persistSnapshot(snapshot);
                    }
                }
                pending.phase = .persist_entries;
            },
            .persist_entries => {
                if (pending.ready.entries.len > 0) {
                    try self.storage.append(self.allocator, pending.ready.entries);
                }
                pending.phase = .persist_hard_state;
            },
            .persist_hard_state => {
                if (pending.ready.hs) |hs| {
                    try self.storage.setHardState(hs);
                }
                pending.phase = .sync;
            },
            .sync => {
                const has_snapshot = readyHasSnapshot(pending.ready);
                const durable_commit = self.durable_state_machine and pending.ready.hs != null;
                if (pending.ready.must_sync or has_snapshot or durable_commit) try self.storage.sync();
                if (pending.snapshot_membership) |membership| {
                    pending.snapshot_membership = null;
                    self.installMembership(membership, pending.ready.snapshot.?.metadata.index);
                }
                pending.phase = .restore_snapshot;
            },
            .restore_snapshot => {
                if (pending.ready.snapshot) |snapshot| {
                    if (snapshot.metadata.index > 0) try self.restoreSnapshot(snapshot);
                }
                pending.phase = .send_messages;
            },
            .send_messages => {
                self.sendMessages(pending.ready.light.messages);
                pending.phase = .apply_committed;
            },
            .apply_committed => {
                if (pending.ready.light.committed_entries.len > 0) {
                    try self.applyCommittedEntries(pending.ready.light.committed_entries);
                }
                pending.phase = .complete_reads;
            },
            .complete_reads => {
                self.completeReads(pending.ready.read_states);
                pending.phase = .advance;
            },
            .advance => {
                pending.light_ready = self.raw_node.*.advance(pending.ready) catch |err| {
                    self.fatal_error = err;
                    return err;
                };
                pending.phase = .persist_advanced_hard_state;
            },
            .persist_advanced_hard_state => {
                if (pending.light_ready.commit_index != null) {
                    try self.storage.setHardState(self.raw_node.*.raftConst().hardState());
                }
                pending.phase = .sync_advanced_hard_state;
            },
            .sync_advanced_hard_state => {
                if (pending.light_ready.commit_index != null) try self.storage.sync();
                pending.phase = .send_advanced_messages;
            },
            .send_advanced_messages => {
                self.sendMessages(pending.light_ready.messages);
                pending.phase = .apply_advanced_committed;
            },
            .apply_advanced_committed => {
                if (pending.light_ready.committed_entries.len > 0) {
                    try self.applyCommittedEntries(pending.light_ready.committed_entries);
                }
                pending.phase = .advance_apply;
            },
            .advance_apply => {
                self.raw_node.*.advanceApply();
                pending.deinit(self.allocator);
                self.pending = null;
                self.fatal_error = self.fatal_after_ready;
                self.fatal_after_ready = null;
            },
            .advance_append_async, .awaiting_flush => unreachable,
        }
        return true;
    }

    fn processStepAsync(self: *ReadyProcessor) Error!bool {
        const pending = &self.pending.?;
        switch (pending.phase) {
            .validate => {
                if (self.checksum_enabled) try self.validateEntries(pending.ready.entries);
                pending.phase = .persist_snapshot;
            },
            .persist_snapshot => {
                if (pending.ready.snapshot) |snapshot| {
                    if (snapshot.metadata.index > 0) {
                        pending.snapshot_membership = try self.persistSnapshot(snapshot);
                    }
                }
                pending.phase = .persist_entries;
            },
            .persist_entries => {
                if (pending.ready.entries.len > 0) {
                    try self.storage.append(self.allocator, pending.ready.entries);
                }
                pending.phase = .persist_hard_state;
            },
            .persist_hard_state => {
                if (pending.ready.hs) |hs| try self.storage.setHardState(hs);
                pending.phase = .advance_append_async;
            },
            .advance_append_async => {
                try self.async_staged.ensureUnusedCapacity(self.allocator, 1);
                const force_flush = readyHasSnapshot(pending.ready) or
                    readyHasCommittedConfChange(pending.ready) or
                    self.async_staged.items.len + 1 >= self.async_ready_max_inflight;
                self.raw_node.*.advanceAppendAsync(pending.ready) catch |err| {
                    self.fatal_error = err;
                    return err;
                };

                var staged = pending.*;
                self.pending = null;
                self.async_staged.appendAssumeCapacity(staged);
                staged = undefined;
                const ready = self.async_staged.items[self.async_staged.items.len - 1].ready;
                self.sendMessages(ready.messages());
                self.async_flush_required = self.async_flush_required or force_flush;
            },
            else => unreachable,
        }
        return true;
    }

    fn stageAsyncReadies(self: *ReadyProcessor) Error!bool {
        var had_work = false;
        if (self.async_flush_required) {
            try self.flushAsyncReadies();
            had_work = true;
        }

        while (self.pending != null or self.raw_node.*.hasReady()) {
            if (self.pending == null) {
                self.pending = .{ .ready = try self.raw_node.*.getReady() };
                self.checkLeadershipChange(self.pending.?.ready);
                had_work = true;
            }
            while (self.pending != null) _ = try self.processStepAsync();
            if (self.async_flush_required) {
                try self.flushAsyncReadies();
                had_work = true;
            }
        }
        return had_work;
    }

    fn flushAsyncReadies(self: *ReadyProcessor) Error!void {
        if (self.async_staged.items.len == 0) {
            self.async_flush_required = false;
            return;
        }

        var must_sync = false;
        for (self.async_staged.items) |pending| {
            must_sync = must_sync or pending.ready.must_sync or readyHasSnapshot(pending.ready) or
                (self.durable_state_machine and pending.ready.hs != null);
        }
        if (must_sync) self.storage.sync() catch |err| {
            self.async_flush_required = true;
            return err;
        };

        const highest_number = self.async_staged.items[self.async_staged.items.len - 1].ready.number;
        self.raw_node.*.onPersistReady(highest_number);
        self.async_flush_required = false;

        while (self.async_staged.items.len != 0) {
            const pending = &self.async_staged.items[0];
            if (pending.snapshot_membership) |membership| {
                pending.snapshot_membership = null;
                self.installMembership(membership, pending.ready.snapshot.?.metadata.index);
            }
            if (pending.ready.snapshot) |snapshot| {
                if (snapshot.metadata.index > 0) try self.restoreSnapshot(snapshot);
            }
            self.sendMessages(pending.ready.persistedMessages());
            if (pending.ready.light.committed_entries.len > 0) {
                try self.applyCommittedEntries(pending.ready.light.committed_entries);
            }
            self.completeReads(pending.ready.read_states);
            self.raw_node.*.advanceApplyTo(self.applied_index);

            pending.deinit(self.allocator);
            _ = self.async_staged.orderedRemove(0);
            if (self.fatal_after_ready) |err| {
                self.fatal_after_ready = null;
                self.fatal_error = err;
                return err;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    fn readyHasSnapshot(ready: Ready) bool {
        return if (ready.snapshot) |snapshot| snapshot.metadata.index > 0 else false;
    }

    fn readyHasCommittedConfChange(ready: Ready) bool {
        for (ready.light.committed_entries) |entry| switch (entry.entry_type) {
            .conf_change, .conf_change_v2 => return true,
            .normal => {},
        };
        return false;
    }

    fn completeReads(self: *ReadyProcessor, read_states: []const ReadState) void {
        for (read_states) |read_state| {
            self.proposal_tracker.markReadReady(read_state.request_ctx, read_state.index);
        }
        self.proposal_tracker.completeReadyReads(self.applied_index);
    }

    fn sendMessages(self: *ReadyProcessor, messages: []const Message) void {
        for (messages) |*message| {
            self.transport.send(message[0..1]) catch |err| {
                log.warn(@src(), "failed to send Raft message to {}: {s}", .{ message.to, @errorName(err) });
                self.raw_node.*.reportUnreachable(message.to) catch {};
                if (message.msg_type == .snapshot) {
                    self.raw_node.*.reportSnapshot(message.to, .failure) catch {};
                }
            };
        }
    }

    fn checkLeadershipChange(self: *ReadyProcessor, rd: Ready) void {
        var lost_leadership_callbacks: ?proposal_tracker_mod.DetachedCallbacks = null;
        const current_term = self.raw_node.*.raftConst().term;
        if (rd.ss) |ss| {
            if (ss.role != self.prev_role or ss.leader_id != self.prev_leader) {
                const was_leader = self.prev_role == .leader;
                self.prev_role = ss.role;
                self.prev_leader = ss.leader_id;
                if (was_leader and ss.role != .leader) {
                    lost_leadership_callbacks = self.proposal_tracker.detachAll();
                }
                self.state_machine.onLeadershipChange(ss.role == .leader, current_term, ss.leader_id);
            }
        }
        self.prev_term = current_term;
        if (lost_leadership_callbacks) |*callbacks| {
            callbacks.invoke(error.ProposalDropped, error.LostLeadership);
        }
    }

    fn validateEntries(self: *ReadyProcessor, entries: []const Entry) Error!void {
        for (entries) |entry| {
            if (util.isChecksumExemptEntry(entry)) continue;
            const expected = entry.checksum;
            if (expected == 0) continue;
            const actual = util.computeEntryChecksum(entry);
            if (actual != expected) {
                if (entry.context.len > 0) {
                    self.proposal_tracker.fail(entry.context, error.ChecksumMismatch);
                }
                self.fatal_error = error.ChecksumMismatch;
                return error.ChecksumMismatch;
            }
        }
    }

    fn restoreSnapshot(self: *ReadyProcessor, snap: Snapshot) Error!void {
        log.info(@src(), "applying snapshot at index {} term {}", .{ snap.metadata.index, snap.metadata.term });

        var reader = state_machine_mod.BufferSnapshotReader.init(snap.data);
        try self.state_machine.restoreSnapshot(snap.metadata, reader.reader());
        self.applied_index = snap.metadata.index;
        self.proposal_tracker.completeReadyReads(self.applied_index);
    }

    fn persistSnapshot(self: *ReadyProcessor, snap: Snapshot) Error!?ClusterMembership {
        if (snap.membership.len == 0 and self.cluster_membership != null) {
            return error.MissingClusterMembership;
        }
        if (snap.membership.len == 0) {
            try self.storage.applySnapshot(self.allocator, snap);
            return null;
        }

        try self.storage.applySnapshot(self.allocator, snap);
        var membership = cluster_membership_mod.decode(self.allocator, snap.membership) catch |err| {
            // KCOV_EXCL_START
            const mapped: Error = switch (err) {
                error.OutOfMemory => error.OutOfMemory,
                else => error.InvalidClusterMembership,
            };
            self.fatal_error = mapped;
            return mapped;
            // KCOV_EXCL_STOP
        };
        errdefer membership.deinit(self.allocator); // KCOV_EXCL_LINE
        membership.validate(snap.metadata.conf_state) catch {
            // KCOV_EXCL_START
            self.fatal_error = error.InvalidClusterMembership;
            return error.InvalidClusterMembership;
            // KCOV_EXCL_STOP
        };
        return membership;
    }

    fn applyCommittedEntries(self: *ReadyProcessor, entries: []Entry) Error!void {
        for (entries) |entry| {
            self.applyEntry(entry) catch |e| {
                if (entry.context.len > 0) self.proposal_tracker.fail(entry.context, e);
                self.fatal_error = e;
                return e;
            };
            self.applied_index = entry.index;
            self.proposal_tracker.completeReadyReads(self.applied_index);
        }
    }

    fn applyEntry(self: *ReadyProcessor, entry: Entry) Error!void {
        if (self.checksum_enabled) try self.validateEntries(&.{entry});

        switch (entry.entry_type) {
            .normal => {
                var result = try self.state_machine.apply(entry);
                defer result.deinit(self.allocator);
                self.applied_index = entry.index;
                if (result.response) |resp| {
                    if (entry.context.len > 0) {
                        self.proposal_tracker.complete(entry.context, resp);
                    }
                } else {
                    if (entry.context.len > 0) {
                        self.proposal_tracker.complete(entry.context, "");
                    }
                }
            },
            .conf_change, .conf_change_v2 => {
                if (self.cluster_membership != null and entry.index <= self.membership_index) return;

                var cc = util.decodeConfChangeV2(self.allocator, entry.data) catch return error.ConfChangeParseError;
                defer cc.deinit(self.allocator);

                if (self.cluster_membership) |membership| {
                    var previous = try self.storage.initialState(self.allocator);
                    defer previous.deinit(self.allocator);

                    var applied_cs = try self.raw_node.*.applyConfChange(cc);
                    defer applied_cs.deinit(self.allocator);
                    var candidate = try cluster_membership_mod.deriveClusterMembership(
                        self.allocator,
                        membership,
                        previous.conf_state,
                        applied_cs,
                        cc,
                    );
                    errdefer candidate.deinit(self.allocator);
                    try self.storage.setMembershipState(self.allocator, applied_cs, candidate, entry.index);
                    try self.storage.sync();
                    self.installMembership(candidate, entry.index);
                    return;
                }

                var applied_cs = try self.raw_node.*.applyConfChange(cc);
                defer applied_cs.deinit(self.allocator);
                try self.storage.setConfState(self.allocator, applied_cs);
                try self.storage.sync();
                for (cc.changes) |change| switch (change.change_type) {
                    .add_node, .add_learner_node => _ = self.transport.addPeer(change.node_id, cc.context) catch |err| {
                        log.warn(@src(), "failed to add transport peer {}: {s}", .{ change.node_id, @errorName(err) });
                        self.fatal_after_ready = err;
                        continue;
                    },
                    .remove_node => self.transport.removePeer(change.node_id) catch |err| {
                        log.warn(@src(), "failed to remove transport peer {}: {s}", .{ change.node_id, @errorName(err) });
                        self.fatal_after_ready = err;
                    },
                    .update_node => {},
                };
            },
        }
    }

    fn installMembership(self: *ReadyProcessor, membership: ClusterMembership, membership_index: u64) void {
        const previous = self.cluster_membership;
        self.cluster_membership = membership;
        self.membership_index = membership_index;
        self.reconcileTransport(previous, membership) catch |err| {
            log.warn(@src(), "failed to reconcile transport membership: {s}", .{@errorName(err)});
            self.fatal_after_ready = err;
        };
        if (previous) |value| {
            var owned = value;
            owned.deinit(self.allocator);
        }
    }

    fn reconcileTransport(
        self: *ReadyProcessor,
        previous: ?ClusterMembership,
        current: ClusterMembership,
    ) Error!void {
        const old_peers = if (previous) |membership| membership.peers else &.{};
        const new_peers = current.peers;
        var old_index: usize = 0;
        var new_index: usize = 0;
        while (old_index < old_peers.len or new_index < new_peers.len) {
            if (new_index == new_peers.len or
                (old_index < old_peers.len and old_peers[old_index].node_id < new_peers[new_index].node_id))
            {
                const old_peer = old_peers[old_index];
                old_index += 1;
                if (old_peer.node_id != self.node_id) try self.transport.removePeer(old_peer.node_id);
                continue;
            }
            if (old_index == old_peers.len or new_peers[new_index].node_id < old_peers[old_index].node_id) {
                const new_peer = new_peers[new_index];
                new_index += 1;
                if (new_peer.node_id != self.node_id) _ = try self.transport.addPeer(new_peer.node_id, new_peer.address);
                continue;
            }

            const old_peer = old_peers[old_index];
            const new_peer = new_peers[new_index];
            old_index += 1;
            new_index += 1;
            if (old_peer.node_id == self.node_id or std.mem.eql(u8, old_peer.address, new_peer.address)) continue;
            try self.transport.removePeer(old_peer.node_id);
            _ = try self.transport.addPeer(new_peer.node_id, new_peer.address);
        }
    }
};
