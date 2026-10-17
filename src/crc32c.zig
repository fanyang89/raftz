const std = @import("std");

const c = @import("crc32c_bindings");

pub fn extend(crc: u32, bytes: []const u8) u32 {
    return c.crc32c_extend(crc, bytes.ptr, bytes.len);
}

pub fn value(bytes: []const u8) u32 {
    return c.crc32c_value(bytes.ptr, bytes.len);
}

// KCOV_EXCL_START
test "standard CRC32C vector" {
    try std.testing.expectEqual(@as(u32, 0xe3069283), value("123456789"));
    try std.testing.expectEqual(value("123456789"), extend(value("1234"), "56789"));
}
// KCOV_EXCL_STOP
