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
    STORED = 1,
};

// TODO: Better memory alignment
pub const StoreNode = struct {
    value: *const anyopaque,
    expires: u62,
    state: NodeState,
    key: []const u8,
    tag: TypeTag,
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
    secondaryList: ?[]StoreNode,
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

    pub fn put(self: *Store, key: []const u8, value: *const anyopaque, tag: TypeTag, expires: u62) !void {
        var new_node = StoreNode{
            .state = NodeState.STORED,
            .key = key,
            .value = value,
            .tag = tag,
            .expires = expires,
            .psl = 0,
        };

        const hash = try hasher.hashKey(key);
        var idx = hash % self.list.len;

        const current_node = &self.list[idx];

        while (true) {
            if (current_node.state == NodeState.EMPTY) {
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
        // TODO: The actual impl, lol
        self.count -= 1;
        return;
    }

    pub fn resize() !void {
        // TODO: Resize to twice the size with a new list
        //       if count of list1 is 0 then
        return;
    }
};
