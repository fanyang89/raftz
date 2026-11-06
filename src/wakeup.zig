const std = @import("std");
const linux = std.os.linux;
const Error = @import("core/error.zig").Error;

pub const Wakeup = struct {
    pending: std.atomic.Value(u32) = .init(0),

    pub fn notify(self: *Wakeup) void {
        if (self.pending.swap(1, .release) == 0) {
            _ = linux.futex_3arg(&self.pending.raw, .{ .cmd = .WAKE, .private = true }, 1);
        }
    }

    pub fn reset(self: *Wakeup) void {
        _ = self.pending.swap(0, .acquire);
    }

    pub fn wait(self: *Wakeup, timeout_ns: u64) Error!void {
        const timeout = linux.timespec{
            .sec = @intCast(timeout_ns / std.time.ns_per_s),
            .nsec = @intCast(timeout_ns % std.time.ns_per_s),
        };
        const rc = linux.futex_4arg(&self.pending.raw, .{ .cmd = .WAIT, .private = true }, 0, &timeout);
        switch (linux.errno(rc)) {
            .SUCCESS, .AGAIN, .INTR, .TIMEDOUT => {},
            else => return error.EventLoopWaitFailed,
        }
    }

    pub fn now() Error!u64 {
        var time: linux.timespec = undefined;
        if (linux.errno(linux.clock_gettime(.MONOTONIC, &time)) != .SUCCESS) return error.EventLoopClockFailed;
        return @as(u64, @intCast(time.sec)) *| std.time.ns_per_s +| @as(u64, @intCast(time.nsec));
    }
};

test "notification between reset and wait is retained and coalesced" {
    var wakeup: Wakeup = .{};
    wakeup.reset();
    wakeup.notify();
    wakeup.notify();
    const start = try Wakeup.now();
    try wakeup.wait(5 * std.time.ns_per_s);
    try std.testing.expect(try Wakeup.now() - start < std.time.ns_per_s);
    wakeup.reset();
    try std.testing.expectEqual(@as(u32, 0), wakeup.pending.load(.acquire));
}
