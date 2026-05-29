const std = @import("std");
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
    ttl: u64 = 0,
    expires: u64 = 0,
    psl: u64 = 0,
    tag: TypeTag = .none,
    state: NodeState = .empty,
};

const SetOptions = struct {
    ttl: u64 = 0, // 0 = no expiry
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
            if (node.expires != NO_EXPIRY and node.expires <= now) {
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
                if (n_node.expires != NO_EXPIRY and n_node.expires <= now) {
                    self.removeUnsafe(n_node.key) catch {};

                    return StoreError.KeyNotFound;
                }

                return n_node;
            }

            n_i_psl += 1;
        }

        return StoreError.KeyNotFound;
    }

    pub fn set(self: *Store, key: []const u8, value: *const anyopaque, tag: TypeTag, opts: SetOptions) StoreError!*StoreNode {
        self.mu.lockUncancelable(self.io);
        defer self.mu.unlock(self.io);

        const now: u64 = @intCast(std.Io.Clock.now(.real, self.io).toNanoseconds());
        const expires_value = opts.expires_at orelse now + opts.ttl;
        var new_node = StoreNode{
            .state = .occupied,
            .key = key,
            .value = value,
            .tag = tag,
            .ttl = opts.ttl,
            .expires = expires_value,
            .psl = 0,
        };

        const hash = try hasher.hashKey(key);
        const start_idx = hash % self.table.len;
        var idx = start_idx;

        while (true) {
            const current_node = &self.table[idx];

            if (opts.nx and (current_node.state == .empty or current_node.state == .deleted)) {
                self.table[idx] = new_node;
                self.occupied += 1;

                return &self.table[idx];
            }

            if (opts.xx and current_node.state == .occupied and std.mem.eql(u8, new_node.key, current_node.key)) {
                self.table[idx] = new_node;

                return &self.table[idx];
            }

            // Shift them around, shake 'em up
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
    node.expires = 0;
    node.ttl = 0;
    node.state = .empty;
    node.tag = .none;
}
