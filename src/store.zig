const std = @import("std");
const hasher = @import("hasher.zig");

const NO_EXPIRY: u64 = 0;

const TypeTag = union(enum) {
    boolean: bool, // 1 byte
    integer: u64, // 8 bytes
    string: []const u8, // 16 bytes (ptr + len)
};

const StoreNode = struct {
    key: []const u8,
    value: *anyopaque,
    tag: TypeTag,
    expires: u64,
    psl: u64,
};

const Store = struct {
    list: []StoreNode,
    capacity: usize,
    fba: std.heap.FixedBufferAllocator,
    raw_buffer: []u8,

    pub fn init(allocator: std.Mem.Allocator, size: usize) !Store {
        const raw_buffer = try allocator.alloc(u8, size);

        var fba = std.heap.FixedBufferAllocator.init(raw_buffer);
        const fba_allocator = fba.allocator();

        const max_items = size / @sizeOf(StoreNode);

        const list = try fba_allocator.alloc(StoreNode, max_items);

        return Store{
            .list = list,
            .capacity = max_items,
            .fba = fba,
            .raw_buffer = raw_buffer,
        };
    }

    pub fn deinit(self: *Store, parent_allocator: std.Mem.Allocator) !void {
        parent_allocator.free(self.raw_buffer);
    }

    pub fn put(self: *Store, key: []const u8, value: *anyopaque, tag: TypeTag, expires: u64) !void {
        const new_node = StoreNode{
            .key = key,
            .value = value,
            .tag = tag,
            .expires = expires,
            .psl = 0, // TODO: update the psl logic
        }

        const hash = try hasher.hashKey(key);
        const idx = hash % self.capacity;
        const current_node = &self.list[idx];

        while (true) {}
    }
};
