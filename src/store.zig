const std = @import("std");
const hasher = @import("hasher.zig");

pub const NO_EXPIRY: u64 = 0;

pub const TypeTag = enum {
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
    value: *const anyopaque,
    expires: u62 = 0,
    state: NodeState = .empty,
    key: []const u8,
    tag: ?TypeTag,
    psl: u64 = 0,
    next: ?*StoreNode,
    prev: ?*StoreNode,
};

pub const Store = struct {
    // Primary table
    p_table: []StoreNode,
    p_size: usize = 0,
    p_capacity: usize,
    p_migrated: usize = 0,
    p_deleted: usize = 0,

    // Secondary table
    s_table: []StoreNode,
    s_size: usize = 0,
    s_capacity: usize,
    s_migrated: usize,
    s_deleted: usize,

    gpa: std.mem.Allocator,
    //raw_buffer: []u8,

    mu: std.Thread.Mutex = std.Thread.Mutex{},

    // TODO: change to min_size: usize to initialize an empty store, grow as needed
    // TODO: read from disk and initialize with the size of the data on disk
    pub fn init(allocator: std.mem.Allocator, size: usize) !Store {
        const max_items = size / @sizeOf(StoreNode);
        const buf = try allocator.alloc(StoreNode, size);

        // TODO: Change to DPA
        //const raw_buffer = try allocator.alloc(u8, size);

        //var fba = std.heap.FixedBufferAllocator.init(raw_buffer);
        //const fba_allocator = fba.allocator();

        //const list = try fba_allocator.alloc(StoreNode, max_items);

        //for (list) |*node| {
        //    node.state = .empty;
        //}

        //var gpa = std.heap.GeneralPurposeAllocator(.{});
        //const alloc = gpa.allocator();
        //const buf = try alloc.alloc(StoreNode, size);

        return Store{
            .p_table = buf,
            .p_capacity = max_items,
            .gpa = allocator,
        };
    }

    pub fn deinit(self: *Store, parent_allocator: std.mem.Allocator) void {
        parent_allocator.free(self.raw_buffer);
    }

    pub fn get(self: *Store, key: []const u8) !StoreNode {
        const hash = try hasher.hashKey(key);
        const idx = hash % self.p_table.len;

        const node = self.p_table[idx];

        if (node.state != .occupied) {
            // TODO: Return error not found
            return;
        }

        if (std.mem.eql(u8, key, node.key)) {
            // TODO: Check expiry and remove if needed
            return node;
        }

        var n_i_psl = idx + 1;

        if (n_i_psl >= self.len) {
            // Errors galore
            return;
        }

        while (n_i_psl < self.len) {
            const n_node = self.p_table[idx + 1];

            if (n_node.psl == 0) {
                // No matching hash keys, not found err
                return;
            }

            if (std.mem.eql(u8, key, n_node.key)) {
                // TODO: Check expiry and remove if needed
                return n_node;
            }

            n_i_psl += 1;
        }

        // Ain't found nothing, toss an error back
        return;
    }

    pub fn set(self: *Store, key: []const u8, value: *const anyopaque, tag: TypeTag, expires: u62) !void {
        const i_now = std.time.timestamp();
        const now = @as(u62, i_now);
        const expires_at = now + expires;
        var new_node = StoreNode{
            .state = .occupied,
            .key = key,
            .value = value,
            .tag = tag,
            .expires = expires_at,
            .psl = 0,
        };

        const hash = try hasher.hashKey(key);
        var idx = hash % self.p_table.len;

        while (true) {
            const current_node = &self.p_table[idx];

            if (current_node.state == .empty or current_node.state == .deleted) {
                self.p_table[idx] = new_node;
                self.p_size += 1;

                if (current_node.state == .deleted and self.p_deleted > 0) self.p_deleted -= 1;

                return;
            }

            // Overwrite existing matching value
            if (current_node.state == .occupied and std.mem.eql(new_node.key, current_node.key)) {
                self.p_table[idx] = new_node;
                return;
            }

            // Shift them around, shake 'em up
            if (new_node.psl > current_node.psl) {
                const temp_node = current_node.*;
                current_node.* = new_node;
                new_node = temp_node;
            }

            new_node.psl += 1;
            idx = (idx + 1) % self.p_table.len;
        }
    }

    pub fn remove(self: *Store, key: []const u8) !void {
        const hash = try hasher.hashKey(key);
        const idx = hash % self.p_table.len;

        const node = self.p_table[idx];

        if (node.state != .occupied) {
            return;
        }

        node.state = .deleted;
        if (self.p_size > 0) self.p_size -= 1;
        self.p_deleted += 1;

        // TODO: if p_deleted + p_migrated == p_capacity we can then free and realloc the list

        var n_i_psl = idx + 1;

        while (n_i_psl <= self.p_table.len) {
            if (self.p_table[n_i_psl].psl > 0) {
                const p_node = &self.p_table[n_i_psl];
                const temp_node = p_node.*;
                temp_node.psl -= 1;
                self.p_table[n_i_psl - 1].* = temp_node;

                if (n_i_psl + 1 < self.p_table.len and self.p_table[n_i_psl + 1].psl == 0) {
                    p_node.key = "";
                    p_node.value = null;
                    p_node.psl = 0;
                    p_node.expires = 0;
                    p_node.state = .empty;
                    p_node.tag = null;
                    // *next & *prev are supposed to be cleaned up in the cleaner
                    // so if that ain't done, tough shit, your LL state is fucked
                }

                n_i_psl += 1;

                continue;
            }

            return;
        }

        return;
    }

    pub fn resize() !void {
        // TODO: Resize to twice the size with a new list
        //       if count of list1 is 0 then
        return;
    }
};
