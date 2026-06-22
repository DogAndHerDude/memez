const std = @import("std");
const testing = std.testing;
const hasher = @import("hasher.zig");

pub const NO_EXPIRY: u64 = 0;

pub const StoreError = error{
    InitializationFailed,
    TableFull,
    TableNotInitialized,
    KeyNotFound,
    OutOfMemory,
};

pub const TypeTag = enum {
    none,
    integer,
    string,
    list,
    vector,
};

pub const NodeState = enum {
    empty,
    occupied,
    deleted,
};

// TODO: Better memory alignment
pub const StoreNode = struct {
    key: []const u8,
    value: *const anyopaque,
    // Number of bytes addressable through `value` for variable-sized payloads
    // (e.g. `.string`). Unused (0) for fixed-size tags like `.integer`.
    value_len: usize = 0,
    ttl: u64 = 0,
    expires_at: u64 = 0,
    psl: u64 = 0,
    tag: TypeTag = .none,
    state: NodeState = .empty,
};

pub const SetOptions = struct {
    ttl: u64 = 0, // 0 = no expiry, ms
    nx: bool = false, // only set if not exists
    xx: bool = false, // only set if exists
    get: bool = false, // return old value
    // Override the computed `now + ttl` expiry with an absolute timestamp.
    // Used by migration paths to preserve the source node's original expiry
    // without a post-set write race against probe expiry sweeps.
    expires_at: ?u64 = null,
};

pub const Store = struct {
    min_capacity: usize, // starting capacity based on min_size

    table: []StoreNode,
    occupied: usize = 0,
    capacity: usize,

    gpa: std.mem.Allocator,

    io: std.Io,
    mu: std.Io.Mutex = .init,

    // TODO: change to min_size: usize to initialize an empty store, grow as needed
    // TODO: read from disk and initialize with the size of the data on disk
    pub fn init(allocator: std.mem.Allocator, io: std.Io, min_size: usize) !Store {
        const max_init_items = min_size / @sizeOf(StoreNode);
        const buf = try allocator.alloc(StoreNode, max_init_items);
        // Zero the buffer so every slot starts with state == .empty (the
        // zero-valued enum tag) and psl == 0. Without this the robin-hood
        // probe in set() reads uninitialized memory.
        @memset(std.mem.sliceAsBytes(buf), 0);

        return Store{
            .min_capacity = max_init_items,
            .table = buf,
            .capacity = max_init_items,
            .io = io,
            .gpa = allocator,
        };
    }

    pub fn deinit(self: *Store) void {
        self.gpa.free(self.table);
    }

    pub fn get(self: *Store, key: []const u8) StoreError!*StoreNode {
        self.mu.lockUncancelable(self.io);
        defer self.mu.unlock(self.io);

        const hash = try hasher.hashKey(key);
        const idx = hash % self.table.len;
        const now: u64 = @intCast(std.Io.Clock.now(.real, self.io).toNanoseconds());

        const node = &self.table[idx];

        if (node.state != .occupied) {
            return StoreError.KeyNotFound;
        }

        if (std.mem.eql(u8, key, node.key)) {
            std.log.debug("GET now: {d} expires_at: {d}", .{ now, node.expires_at });
            if (node.expires_at != NO_EXPIRY and node.expires_at <= now) {
                self.removeUnsafe(node.key) catch {};

                return StoreError.KeyNotFound;
            }

            return node;
        }

        var n_i_psl = idx + 1;

        if (n_i_psl >= self.table.len) {
            return StoreError.KeyNotFound;
        }

        while (n_i_psl < self.table.len) {
            const n_node = &self.table[n_i_psl];

            if (n_node.psl == 0) {
                return StoreError.KeyNotFound;
            }

            if (std.mem.eql(u8, key, n_node.key)) {
                if (n_node.expires_at != NO_EXPIRY and n_node.expires_at <= now) {
                    self.removeUnsafe(n_node.key) catch {};

                    return StoreError.KeyNotFound;
                }

                return n_node;
            }

            n_i_psl += 1;
        }

        return StoreError.KeyNotFound;
    }

    pub fn set(self: *Store, key: []const u8, value: *const anyopaque, value_len: usize, tag: TypeTag, opts: SetOptions) StoreError!*StoreNode {
        self.mu.lockUncancelable(self.io);
        defer self.mu.unlock(self.io);

        const now: u64 = @intCast(std.Io.Clock.now(.real, self.io).toNanoseconds());
        const expires_at = opts.expires_at orelse if (opts.ttl == 0) NO_EXPIRY else now + opts.ttl;
        std.log.debug("SET: expires_at: {d}, now: {d}, ttl_ns: {d}", .{ expires_at, now, opts.ttl });
        var new_node = StoreNode{
            .state = .occupied,
            .key = key,
            .value = value,
            .value_len = value_len,
            .tag = tag,
            .ttl = opts.ttl,
            .expires_at = expires_at,
            .psl = 0,
        };

        const hash = try hasher.hashKey(key);
        const start_idx = hash % self.table.len;
        var idx = start_idx;

        while (true) {
            const current_node = &self.table[idx];

            // Insertion target: a slot that holds no live entry.
            if (current_node.state != .occupied) {
                if (opts.xx) return StoreError.KeyNotFound;
                self.table[idx] = new_node;
                self.occupied += 1;
                return &self.table[idx];
            }

            // Existing live entry with the same key — replace in place.
            if (std.mem.eql(u8, current_node.key, new_node.key)) {
                if (opts.nx) return &self.table[idx];
                const preserved_psl = current_node.psl;
                self.table[idx] = new_node;
                self.table[idx].psl = preserved_psl;
                return &self.table[idx];
            }

            // Robin-hood: if the new entry has probed further than the
            // resident, swap and continue probing with the displaced entry.
            if (new_node.psl > current_node.psl) {
                const temp_node = current_node.*;
                current_node.* = new_node;
                new_node = temp_node;
            }

            new_node.psl += 1;
            idx = (idx + 1) % self.table.len;

            // Full wrap back to the original hash bucket means no free slot was found.
            if (idx == start_idx) return StoreError.TableFull;
        }
    }

    pub fn remove(self: *Store, key: []const u8) !void {
        self.mu.lockUncancelable(self.io);
        defer self.mu.unlock(self.io);
        try self.removeUnsafe(key);
    }

    // Caller must hold self.mu. Use from within other Store methods that
    // already lock the table (e.g. get() removing an expired entry).
    pub fn removeUnsafe(self: *Store, key: []const u8) !void {
        const hash = try hasher.hashKey(key);
        const start_idx = hash % self.table.len;

        // Probe for the matching key. Wraps around, skips tombstones, and
        // bails on the robin-hood psl invariant: hitting an occupied entry
        // whose psl is shorter than our probe distance means our key would
        // have been swapped in at that slot on insert, so it's not here.
        var probe_idx = start_idx;
        var probe_psl: usize = 0;
        while (probe_psl < self.table.len) : ({
            probe_idx = (probe_idx + 1) % self.table.len;
            probe_psl += 1;
        }) {
            const cur = &self.table[probe_idx];
            if (cur.state == .empty) return;
            if (cur.state == .occupied) {
                if (std.mem.eql(u8, cur.key, key)) break;
                if (cur.psl < probe_psl) return;
            }
            // .deleted: tombstone — keep probing.
        }
        if (probe_psl >= self.table.len) return;

        // Back-shift deletion: walk the hole forward, sliding any displaced
        // entry (occupied, psl > 0) into it and decrementing its psl. The
        // trailing slot is then marked empty — no tombstone left behind.
        var hole_idx = probe_idx;
        while (true) {
            const next_idx = (hole_idx + 1) % self.table.len;
            const next = &self.table[next_idx];

            if (next.state != .occupied or next.psl == 0) break;

            self.table[hole_idx] = next.*;
            self.table[hole_idx].psl -= 1;
            hole_idx = next_idx;
        }

        resetNode(&self.table[hole_idx]);
        if (self.occupied > 0) self.occupied -= 1;
    }
};

pub fn resetNode(node: *StoreNode) void {
    node.key = "";
    node.psl = 0;
    node.expires_at = 0;
    node.ttl = 0;
    node.state = .empty;
    node.tag = .none;
    node.value_len = 0;
}

test "expired node returns as KeyNotFound" {
    const size = 2 * 1024;
    var store = try Store.init(testing.allocator, testing.io, size);
    defer store.deinit();
    const key: []const u8 = "foo";
    const val: []const u8 = "bar";
    const ttl_ms: u64 = std.time.ms_per_s;

    _ = try store.set(key, val.ptr, val.len, .string, .{
        .ttl = ttl_ms,
    });

    const node = try store.get(key);
    const res_bytes_ptr: [*]const u8 = @ptrCast(node.value);
    const val_bytes = res_bytes_ptr[0..node.value_len];
    try std.testing.expectEqual(key, node.key);
    try std.testing.expectEqual(val, val_bytes);
    try testing.io.sleep(.{ .nanoseconds = std.time.ns_per_s }, .real);
    try std.testing.expectError(StoreError.KeyNotFound, store.get(key));
}
