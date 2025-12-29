const std = @import("std");
const hasher = @import("hasher.zig");

pub const NO_EXPIRY: u64 = 0;

pub const TypeTag = enum {
    integer,
    string,
    bool,
};

pub const NodeState = enum(u8) {
    EMPTY = 0,
    OCCUPIED = 1,
    DELETED = 2,
};

// TODO: Better memory alignment
pub const StoreNode = struct {
    value: *const anyopaque,
    expires: u62,
    state: NodeState,
    key: []const u8,
    tag: ?TypeTag,
    psl: u64,
    next: ?*StoreNode,
    prev: ?*StoreNode,
};

pub const Store = struct {
    // TODO: assign not the max size and init a second list when the first list grows to 75% of its size
    //       create list2 double the size of list1 but within size limits
    //       empty list1 and fill list2 with nodes from list1
    //       make list2 into list1
    list: []StoreNode,
    count: u64 = 0,
    fba: std.heap.FixedBufferAllocator,
    raw_buffer: []u8,

    // TODO: change to min_size: usize to initialize an empty store, grow as needed
    // TODO: read from disk and initialize with the size of the data on disk
    pub fn init(allocator: std.mem.Allocator, size: usize) !Store {
        const raw_buffer = try allocator.alloc(u8, size);

        var fba = std.heap.FixedBufferAllocator.init(raw_buffer);
        const fba_allocator = fba.allocator();

        const max_items = size / @sizeOf(StoreNode);

        const list = try fba_allocator.alloc(StoreNode, max_items);

        for (list) |*node| {
            node.state = NodeState.EMPTY;
        }

        return Store{
            .list = list,
            .fba = fba,
            .raw_buffer = raw_buffer,
        };
    }

    pub fn deinit(self: *Store, parent_allocator: std.mem.Allocator) void {
        parent_allocator.free(self.raw_buffer);
    }

    pub fn get(self: *Store, key: []const u8) !StoreNode {
        const hash = try hasher.hashKey(key);
        const idx = hash % self.list.len;

        const node = self.list[idx];

        if (node.state != NodeState.OCCUPIED) {
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
            const n_node = self.list[idx + 1];

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

    pub fn put(self: *Store, key: []const u8, value: *const anyopaque, tag: TypeTag, expires: u62) !void {
        const i_now = std.time.timestamp();
        const now = @as(u62, i_now);
        const expires_at = now + expires;
        var new_node = StoreNode{
            .state = NodeState.OCCUPIED,
            .key = key,
            .value = value,
            .tag = tag,
            .expires = expires_at,
            .psl = 0,
        };

        const hash = try hasher.hashKey(key);
        var idx = hash % self.list.len;

        const current_node = &self.list[idx];

        while (true) {
            if (current_node.state == NodeState.EMPTY or current_node.state == NodeState.DELETED) {
                self.list[idx] = new_node;
                self.count += 1;
                return;
            }

            if (new_node.psl > current_node.psl) {
                const temp_node = current_node.*;
                current_node.* = new_node;
                new_node = temp_node;
                self.count += 1;
            }

            new_node.psl += 1;
            idx = (idx + 1) % self.list.len;

            if (idx >= self.list.len) {
                // Ain't no more space, get lost, buddy
                // Probably return some error something wack
                return;
            }
        }
    }

    pub fn remove(self: *Store, key: []const u8) !void {
        const hash = try hasher.hashKey(key);
        const idx = hash % self.list.len;

        const node = self.list[idx];

        if (node.state != NodeState.OCCUPIED) {
            return;
        }

        node.state = NodeState.DELETED;
        self.count -= 1;

        var n_i_psl = idx + 1;

        while (n_i_psl < self.list.len) {
            if (self.list[n_i_psl].psl > 0) {
                const p_node = &self.list[n_i_psl];
                const temp_node = p_node.*;
                temp_node.psl -= 1;
                self.list[n_i_psl - 1].* = temp_node;
                // Resets, however, not sure if this works the way I imagined
                // does p_node get modified too or p_node gets brought into the stack as a new value?
                p_node.key = "";
                p_node.value = null;
                p_node.psl = 0;
                p_node.expires = 0;
                p_node.state = NodeState.EMPTY;
                p_node.tag = null;
                // *next & *prev are supposed to be cleaned up in the cleaner
                // so if that ain't done, tough shit, your LL state is fucked

                n_i_psl += 1;
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
