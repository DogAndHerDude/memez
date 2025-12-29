const std = @import("std");
const memez = @import("memez");
const s = @import("store.zig");

pub fn main() !void {
    const user_size_mb: usize = 512;
    const size_bytes = user_size_mb * 1024 * 1024;

    const store = try s.Store.init(std.heap.page_allocator, size_bytes);
    defer store.deinit(&store, std.heap.page_allocator);

    const key = "my.key";
    const value = "my value";

    store.put(&store, key, value, store.TypeTag.string, store.NO_EXPIRY);
    //try memez.bufferedPrint(hash);
}
