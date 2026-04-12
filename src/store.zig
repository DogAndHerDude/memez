const std = @import("std");
const hasher = @import("hasher.zig");

pub const NO_EXPIRY: u64 = 0;

pub const StoreError = error{
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
    expires: u64 = 0,
    psl: u64 = 0,
    tag: TypeTag = .none,
    state: NodeState = .empty,
};

const ActiveTable = enum {
    primary,
    secondary,
};

const SetOptions = struct {
    ttl: u64 = 0, // 0 = no expiry
    nx: bool = false, // only set if not exists
    xx: bool = false, // only set if exists
    get: bool = false, // return old value
};

pub const Store = struct {
    const UPSIZE_THRESHOLD: f16 = 0.75;
    const DOWNSIZE_THRESHOLD: f16 = 0.50;

    const UPSIZE_FACTOR: usize = 2;
    const DOWNSIZE_FACTOR: usize = 2;

    min_size: usize,

    table: []StoreNode,
    occupied: usize = 0,
    capacity: usize,
    migrated: usize = 0,
    deleted: usize = 0,

    gpa: std.mem.Allocator,

    mu: std.Thread.Mutex = std.Thread.Mutex{},

    // Callbacks for the probe
    on_remove: ?*const fn (*anyopaque, *StoreNode) void = null,
    on_remove_ctx: ?*anyopaque = null,

    // TODO: change to min_size: usize to initialize an empty store, grow as needed
    // TODO: read from disk and initialize with the size of the data on disk
    pub fn init(allocator: std.mem.Allocator, size: usize) !Store {
        const max_items = size / @sizeOf(StoreNode);
        const buf = try allocator.alloc(StoreNode, max_items);

        return Store{
            .min_size = max_items,
            .table = buf,
            .capacity = max_items,
            .gpa = allocator,
        };
    }

    pub fn deinit(self: *Store) void {
        self.gpa.free(self.table);
    }

    // TODO: Have private functions to get from primary & get from secondary
    //       if both return and both tables are not undefined, determine which needs to be migrated over to current active table
    //       otherwise return value
    pub fn get(self: *Store, key: []const u8) StoreError!*StoreNode {
        const hash = try hasher.hashKey(key);
        const idx = hash % self.table.len;
        const now: u64 = @intCast(std.time.timestamp());

        const node = self.table[idx];

        if (node.state != .occupied) {
            return StoreError.KeyNotFound;
        }

        if (std.mem.eql(u8, key, node.key)) {
            if (node.expires <= now) {
                self.removeAndBroadcast(node.key, node);

                return StoreError.KeyNotFound;
            }

            return &node;
        }

        var n_i_psl = idx + 1;

        if (n_i_psl >= self.table.len) {
            return StoreError.KeyNotFound;
        }

        while (n_i_psl < self.table.len) {
            const n_node = self.table[idx + 1];

            if (n_node.psl == 0) {
                return StoreError.KeyNotFound;
            }

            if (std.mem.eql(u8, key, n_node.key)) {
                if (node.expires <= now) {
                    self.removeAndBroadcast(node.key, node);

                    return StoreError.KeyNotFound;
                }

                return &n_node;
            }

            n_i_psl += 1;
        }

        return StoreError.KeyNotFound;
    }

    pub fn set(self: *Store, key: []const u8, value: *const anyopaque, tag: TypeTag, opts: SetOptions) StoreError!*StoreNode {
        const i_now = std.time.timestamp();
        const now: u64 = @intCast(i_now);
        const expires_at = now + opts.ttl;
        var new_node = StoreNode{
            .state = .occupied,
            .key = key,
            .value = value,
            .tag = tag,
            .expires = expires_at, // TODO: check remaining options for setting
            .psl = 0,
        };

        const hash = try hasher.hashKey(key);
        var idx = hash % self.table.len;

        while (true) {
            const current_node = &self.table[idx];

            if (current_node.state == .empty or current_node.state == .deleted) {
                self.table[idx] = new_node;
                self.occupied += 1;

                if (current_node.state == .deleted and self.deleted > 0) self.deleted -= 1;

                return &new_node;
            }

            // Overwrite existing matching value
            if (current_node.state == .occupied and std.mem.eql(new_node.key, current_node.key)) {
                self.table[idx] = new_node;

                return &new_node;
            }

            // Shift them around, shake 'em up
            if (new_node.psl > current_node.psl) {
                const temp_node = current_node.*;
                current_node.* = new_node;
                new_node = temp_node;
            }

            new_node.psl += 1;
            idx = (idx + 1) % self.table.len;

            if (idx == 0) return StoreError.TableFull;
        }

        // I'm sure something is not handled here potentially, but shouldn't happen really unless I loop forever more
    }

    pub fn remove(self: *Store, key: []const u8) !void {
        self.mu.lock();
        defer self.mu.unlock();

        const hash = try hasher.hashKey(key);
        const idx = hash % self.table.len;

        var node = self.table[idx];

        if (node.state != .occupied) {
            return;
        }

        node.state = .deleted;
        if (self.occupied > 0) self.occupied -= 1;
        self.deleted += 1;

        // TODO: if p_deleted + p_migrated == p_capacity we can then free and realloc the list

        var n_i_psl = idx + 1;

        while (n_i_psl <= self.table.len) {
            if (self.table[n_i_psl].psl > 0) {
                const p_node = &self.table[n_i_psl];
                var temp_node = p_node.*;
                temp_node.psl -= 1;
                self.table[n_i_psl - 1] = temp_node;

                if (n_i_psl + 1 < self.table.len and self.table[n_i_psl + 1].psl == 0) {
                    self.resetNode(p_node);
                }

                n_i_psl += 1;

                continue;
            }

            return;
        }

        return;
    }

    pub fn onRemove(ctx: *anyopaque, node: *StoreNode) void {
        const self: *Store = @ptrCast(@alignCast(ctx));

        self.remove(node.key) catch |err| {
            std.debug.print("STORE: onRemove error: {}\n", .{err});
        };
    }

    fn removeAndBroadcast(self: *Store, key: []const u8, node: *StoreNode) !void {
        if (self.on_remove) |cb| {
            // Remove from the scanner probe first, since it's a double linked list, then remove it here
            try cb(self.on_remove_ctx.?, node);
        }

        // The lookup is O(1) either way, didn't wanna reimplement the same remove function just by passing a node
        // Plus you need to iterrate through the current hash table to change the psl either way
        try self.remove(key);
    }

    fn resetNode(_: *Store, node: *StoreNode) void {
        node.key = "";
        node.value = &null;
        node.psl = 0;
        node.expires = 0;
        node.state = .empty;
        node.tag = .none;
    }
};
