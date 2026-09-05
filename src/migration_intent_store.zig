const std = @import("std");

const error_model = @import("core/error.zig");
const fs_mod = @import("fs.zig");

const crc32c = @import("crc32c");

const Error = error_model.Error;
const Fs = fs_mod.Fs;

const magic = "RMIG";
const version: u32 = 1;
const suffix = ".intent";
const fixed_size = magic.len + @sizeOf(u32) + 4 * @sizeOf(u64) + 2 * @sizeOf(u32);
const checksum_size = @sizeOf(u32);

pub const Intent = struct {
    group_id: u64,
    source_node_id: u64,
    target_node_id: u64,
    target_address: []u8,
    timeout_ticks: u64,
    stable_catch_up_ticks: u32,

    pub fn deinit(self: *Intent, allocator: std.mem.Allocator) void {
        if (self.target_address.len != 0) allocator.free(self.target_address);
        self.* = undefined;
    }
};

pub const IntentView = struct {
    group_id: u64,
    source_node_id: u64,
    target_node_id: u64,
    target_address: []const u8,
    timeout_ticks: u64,
    stable_catch_up_ticks: u32,
};

pub const Store = struct {
    allocator: std.mem.Allocator,
    fs: Fs,
    dir: [:0]u8,
    max_address_bytes: usize,

    pub fn init(
        allocator: std.mem.Allocator,
        data_dir: []const u8,
        file_system: ?Fs,
        max_address_bytes: usize,
    ) Error!Store {
        const fs = file_system orelse fs_mod.realFileSystem();
        const dir = try std.fmt.allocPrintSentinel(allocator, "{s}/migrations", .{data_dir}, 0);
        errdefer allocator.free(dir);
        const created = fs.makeDir(dir) catch |err| return mapFsError(err);
        if (created) {
            const root = try allocator.dupeZ(u8, data_dir);
            defer allocator.free(root);
            fs.syncDir(root) catch |err| return mapFsError(err);
        }
        return .{
            .allocator = allocator,
            .fs = fs,
            .dir = dir,
            .max_address_bytes = max_address_bytes,
        };
    }

    pub fn deinit(self: *Store) void {
        self.allocator.free(self.dir);
        self.* = undefined;
    }

    pub fn put(self: *Store, intent: IntentView) Error!void {
        if (intent.target_address.len > self.max_address_bytes) return error.GroupOperationBackpressure;
        const encoded = try encode(self.allocator, intent);
        defer self.allocator.free(encoded);
        const final_path = try self.intentPath(intent.group_id, suffix);
        defer self.allocator.free(final_path);
        const temporary_path = try self.intentPath(intent.group_id, ".intent.tmp");
        defer self.allocator.free(temporary_path);

        const handle = self.fs.open(temporary_path, .write_truncate) catch |err| return mapFsError(err);
        var open = true;
        defer if (open) self.fs.close(handle) catch {};
        self.fs.pwriteAll(handle, encoded, 0) catch |err| return mapFsError(err);
        self.fs.truncate(handle, @intCast(encoded.len)) catch |err| return mapFsError(err);
        self.fs.syncFile(handle) catch |err| return mapFsError(err);
        self.fs.close(handle) catch |err| {
            open = false;
            return mapFsError(err);
        };
        open = false;
        self.fs.rename(temporary_path, final_path) catch |err| return mapFsError(err);
        self.fs.syncDir(self.dir) catch |err| return mapFsError(err);
    }

    pub fn remove(self: *Store, group_id: u64) Error!void {
        const file_path = try self.intentPath(group_id, suffix);
        defer self.allocator.free(file_path);
        self.fs.unlink(file_path) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return mapFsError(err),
        };
        self.fs.syncDir(self.dir) catch |err| return mapFsError(err);
    }

    pub fn loadAll(self: *Store, max_intents: usize) Error![]Intent {
        var listing = self.fs.listDir(self.allocator, self.dir) catch |err| return mapFsError(err);
        defer listing.deinit();
        var intents: std.ArrayList(Intent) = .empty;
        errdefer {
            for (intents.items) |*intent| intent.deinit(self.allocator);
            intents.deinit(self.allocator);
        }
        for (listing.entries.items) |entry| {
            if (entry.kind != .file or !std.mem.endsWith(u8, entry.name, suffix)) continue;
            if (intents.items.len >= max_intents) return error.GroupOperationBackpressure;
            const id_bytes = entry.name[0 .. entry.name.len - suffix.len];
            const filename_group_id = std.fmt.parseInt(u64, id_bytes, 10) catch return error.MigrationIntentCorrupt;
            if (filename_group_id == 0) return error.MigrationIntentCorrupt;
            const file_path = try self.intentPath(filename_group_id, suffix);
            defer self.allocator.free(file_path);
            var intent = try self.loadOne(file_path);
            errdefer intent.deinit(self.allocator);
            if (intent.group_id != filename_group_id) return error.MigrationIntentCorrupt;
            try intents.append(self.allocator, intent);
        }
        std.mem.sort(Intent, intents.items, {}, struct {
            fn lessThan(_: void, lhs: Intent, rhs: Intent) bool {
                return lhs.group_id < rhs.group_id;
            }
        }.lessThan);
        return intents.toOwnedSlice(self.allocator);
    }

    fn loadOne(self: *Store, file_path: [:0]const u8) Error!Intent {
        const handle = self.fs.open(file_path, .read_only) catch |err| return mapFsError(err);
        defer self.fs.close(handle) catch {};
        const size_u64 = self.fs.fileSize(handle) catch |err| return mapFsError(err);
        const max_size = fixed_size + self.max_address_bytes + checksum_size;
        if (size_u64 < fixed_size + checksum_size or size_u64 > max_size) return error.MigrationIntentCorrupt;
        const size = std.math.cast(usize, size_u64) orelse return error.MigrationIntentCorrupt;
        const bytes = try self.allocator.alloc(u8, size);
        defer self.allocator.free(bytes);
        const read_len = self.fs.preadAll(handle, bytes, 0) catch |err| return mapFsError(err);
        if (read_len != bytes.len) return error.MigrationIntentCorrupt;
        return decode(self.allocator, bytes, self.max_address_bytes);
    }

    fn intentPath(self: *Store, group_id: u64, extension: []const u8) Error![:0]u8 {
        return std.fmt.allocPrintSentinel(self.allocator, "{s}/{}{s}", .{ self.dir, group_id, extension }, 0);
    }
};

fn encode(allocator: std.mem.Allocator, intent: IntentView) Error![]u8 {
    if (intent.group_id == 0 or intent.source_node_id == 0 or intent.target_node_id == 0 or
        intent.timeout_ticks == 0 or intent.stable_catch_up_ticks == 0 or intent.target_address.len == 0)
    {
        return error.MigrationIntentCorrupt;
    }
    const address_len = std.math.cast(u32, intent.target_address.len) orelse return error.MigrationIntentCorrupt;
    const total = std.math.add(usize, fixed_size + checksum_size, intent.target_address.len) catch
        return error.MigrationIntentCorrupt;
    const bytes = try allocator.alloc(u8, total);
    var offset: usize = 0;
    @memcpy(bytes[offset .. offset + magic.len], magic);
    offset += magic.len;
    writeInt(u32, bytes, &offset, version);
    writeInt(u64, bytes, &offset, intent.group_id);
    writeInt(u64, bytes, &offset, intent.source_node_id);
    writeInt(u64, bytes, &offset, intent.target_node_id);
    writeInt(u64, bytes, &offset, intent.timeout_ticks);
    writeInt(u32, bytes, &offset, intent.stable_catch_up_ticks);
    writeInt(u32, bytes, &offset, address_len);
    @memcpy(bytes[offset .. offset + intent.target_address.len], intent.target_address);
    offset += intent.target_address.len;
    writeInt(u32, bytes, &offset, crc32c.value(bytes[0..offset]));
    std.debug.assert(offset == bytes.len);
    return bytes;
}

fn decode(
    allocator: std.mem.Allocator,
    bytes: []const u8,
    max_address_bytes: usize,
) Error!Intent {
    if (bytes.len < fixed_size + checksum_size) return error.MigrationIntentCorrupt;
    var offset: usize = 0;
    if (!std.mem.eql(u8, bytes[offset .. offset + magic.len], magic)) return error.MigrationIntentCorrupt;
    offset += magic.len;
    const stored_version = readInt(u32, bytes, &offset) catch return error.MigrationIntentCorrupt;
    if (stored_version != version) return error.UnsupportedVersion;
    const payload = bytes[0 .. bytes.len - checksum_size];
    var checksum_offset = payload.len;
    const expected_checksum = readInt(u32, bytes, &checksum_offset) catch return error.MigrationIntentCorrupt;
    if (crc32c.value(payload) != expected_checksum) return error.MigrationIntentCorrupt;
    const group_id = readInt(u64, bytes, &offset) catch return error.MigrationIntentCorrupt;
    const source_node_id = readInt(u64, bytes, &offset) catch return error.MigrationIntentCorrupt;
    const target_node_id = readInt(u64, bytes, &offset) catch return error.MigrationIntentCorrupt;
    const timeout_ticks = readInt(u64, bytes, &offset) catch return error.MigrationIntentCorrupt;
    const stable_catch_up_ticks = readInt(u32, bytes, &offset) catch return error.MigrationIntentCorrupt;
    const address_len = readInt(u32, bytes, &offset) catch return error.MigrationIntentCorrupt;
    if (group_id == 0 or source_node_id == 0 or target_node_id == 0 or timeout_ticks == 0 or
        stable_catch_up_ticks == 0 or address_len == 0 or address_len > max_address_bytes)
    {
        return error.MigrationIntentCorrupt;
    }
    if (offset + address_len != payload.len) return error.MigrationIntentCorrupt;
    return .{
        .group_id = group_id,
        .source_node_id = source_node_id,
        .target_node_id = target_node_id,
        .target_address = try allocator.dupe(u8, bytes[offset .. offset + address_len]),
        .timeout_ticks = timeout_ticks,
        .stable_catch_up_ticks = stable_catch_up_ticks,
    };
}

fn writeInt(comptime T: type, bytes: []u8, offset: *usize, value: T) void {
    const size = @sizeOf(T);
    std.mem.writeInt(T, bytes[offset.*..][0..size], value, .little);
    offset.* += size;
}

fn readInt(comptime T: type, bytes: []const u8, offset: *usize) !T {
    const size = @sizeOf(T);
    if (offset.* > bytes.len or bytes.len - offset.* < size) return error.EndOfStream;
    const value = std.mem.readInt(T, bytes[offset.*..][0..size], .little);
    offset.* += size;
    return value;
}

fn mapFsError(err: fs_mod.Error) Error {
    return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        else => error.MigrationIntentIo,
    };
}
