const std = @import("std");
const MultiPeerEvent = @import("../multi_transport.zig").PeerEvent;

pub const MultiPeerEventQueue = struct {
    const Item = struct {
        event: MultiPeerEvent,
        generation: u64,
    };

    mutex: std.atomic.Mutex = .unlocked,
    items: std.ArrayList(Item) = .empty,
    head: usize = 0,
    max_events: usize,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, max_events: usize) !MultiPeerEventQueue {
        if (max_events == 0) return error.InvalidConfig;
        var self = MultiPeerEventQueue{ .allocator = allocator, .max_events = max_events };
        try self.items.ensureTotalCapacity(allocator, max_events);
        return self;
    }

    pub fn deinit(self: *MultiPeerEventQueue) void {
        self.items.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn push(self: *MultiPeerEventQueue, event: MultiPeerEvent, generation: u64) void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        for (self.items.items[self.head..]) |item| {
            if (item.event.group_id == event.group_id and
                item.event.peer_id == event.peer_id and
                item.event.kind == event.kind and
                item.generation == generation)
            {
                return;
            }
        }
        if (self.items.items.len - self.head == self.max_events) self.head += 1;
        if (self.head > 0 and self.items.items.len == self.items.capacity) self.compact();
        self.items.appendAssumeCapacity(.{ .event = event, .generation = generation });
    }

    pub fn pop(self: *MultiPeerEventQueue) ?MultiPeerEvent {
        lock(&self.mutex);
        defer self.mutex.unlock();
        if (self.head == self.items.items.len) return null;
        const event = self.items.items[self.head].event;
        self.head += 1;
        if (self.head == self.items.items.len) {
            self.items.clearRetainingCapacity();
            self.head = 0;
        }
        return event;
    }

    pub fn clear(self: *MultiPeerEventQueue) void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        self.items.clearRetainingCapacity();
        self.head = 0;
    }

    fn compact(self: *MultiPeerEventQueue) void {
        const pending = self.items.items.len - self.head;
        std.mem.copyForwards(Item, self.items.items[0..pending], self.items.items[self.head..]);
        self.items.shrinkRetainingCapacity(pending);
        self.head = 0;
    }
};

fn lock(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

// KCOV_EXCL_START
test "multi peer event queue deduplicates per group" {
    var queue = try MultiPeerEventQueue.init(std.testing.allocator, 3);
    defer queue.deinit();
    queue.push(.{ .group_id = 1, .peer_id = 2, .kind = .@"unreachable" }, 1);
    queue.push(.{ .group_id = 1, .peer_id = 2, .kind = .@"unreachable" }, 1);
    queue.push(.{ .group_id = 2, .peer_id = 2, .kind = .@"unreachable" }, 1);
    try std.testing.expectEqual(@as(u64, 1), queue.pop().?.group_id);
    try std.testing.expectEqual(@as(u64, 2), queue.pop().?.group_id);
    try std.testing.expect(queue.pop() == null);
}
// KCOV_EXCL_STOP
