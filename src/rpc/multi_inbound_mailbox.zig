//! Bounded thread-safe inbound envelope queue for grpc MultiTransport.

const std = @import("std");
const Error = @import("../core/error.zig").Error;
const Envelope = @import("../multi_transport.zig").Envelope;

fn lock(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

pub const MultiInboundMailbox = struct {
    pub const Limits = struct {
        max_messages: usize,
        max_bytes: usize,

        pub fn validate(self: Limits) !void {
            if (self.max_messages == 0 or self.max_bytes == 0) return error.InvalidConfig;
        }
    };

    const Item = struct {
        envelope: Envelope,
        encoded_size: usize,
    };

    mutex: std.atomic.Mutex = .unlocked,
    inbox: std.ArrayList(Item) = .empty,
    head: usize = 0,
    encoded_bytes: usize = 0,
    limits: Limits,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, limits: Limits) !MultiInboundMailbox {
        try limits.validate();
        var self = MultiInboundMailbox{ .allocator = allocator, .limits = limits };
        try self.inbox.ensureTotalCapacity(allocator, limits.max_messages);
        return self;
    }

    pub fn deinit(self: *MultiInboundMailbox) void {
        self.clear();
        self.inbox.deinit(self.allocator);
        self.* = undefined;
    }

    /// Ownership transfers only on success.
    pub fn push(self: *MultiInboundMailbox, envelope: Envelope, encoded_size: usize) Error!void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        const pending = self.inbox.items.len - self.head;
        if (pending >= self.limits.max_messages or
            encoded_size > self.limits.max_bytes -| self.encoded_bytes)
        {
            return error.TransportBackpressure;
        }
        if (self.head > 0 and self.inbox.items.len == self.inbox.capacity) self.compact();
        self.inbox.appendAssumeCapacity(.{ .envelope = envelope, .encoded_size = encoded_size });
        self.encoded_bytes += encoded_size;
    }

    pub fn canAccept(self: *MultiInboundMailbox, encoded_size: usize) bool {
        lock(&self.mutex);
        defer self.mutex.unlock();
        return self.inbox.items.len - self.head < self.limits.max_messages and
            encoded_size <= self.limits.max_bytes -| self.encoded_bytes;
    }

    pub fn pop(self: *MultiInboundMailbox) ?Envelope {
        lock(&self.mutex);
        defer self.mutex.unlock();
        if (self.head == self.inbox.items.len) return null;
        const item = self.inbox.items[self.head];
        self.head += 1;
        self.encoded_bytes -= item.encoded_size;
        if (self.head == self.inbox.items.len) {
            self.inbox.clearRetainingCapacity();
            self.head = 0;
        }
        return item.envelope;
    }

    pub fn clear(self: *MultiInboundMailbox) void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        for (self.inbox.items[self.head..]) |*item| item.envelope.deinit(self.allocator);
        self.inbox.clearRetainingCapacity();
        self.head = 0;
        self.encoded_bytes = 0;
    }

    fn compact(self: *MultiInboundMailbox) void {
        const pending = self.inbox.items.len - self.head;
        std.mem.copyForwards(Item, self.inbox.items[0..pending], self.inbox.items[self.head..]);
        self.inbox.shrinkRetainingCapacity(pending);
        self.head = 0;
    }
};

// KCOV_EXCL_START
test "multi inbound mailbox owns and accounts envelopes" {
    const allocator = std.testing.allocator;
    var mailbox = try MultiInboundMailbox.init(allocator, .{ .max_messages = 1, .max_bytes = 8 });
    defer mailbox.deinit();
    try mailbox.push(.{ .group_id = 7, .message = .{ .msg_type = .heartbeat } }, 8);
    try std.testing.expectError(
        error.TransportBackpressure,
        mailbox.push(.{ .group_id = 8, .message = .{ .msg_type = .heartbeat } }, 1),
    );
    var envelope = mailbox.pop().?;
    defer envelope.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 7), envelope.group_id);
}
// KCOV_EXCL_STOP
