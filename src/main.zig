const std = @import("std");
const memez = @import("memez");
const s = @import("store.zig");

pub fn main() !void {
    const user_size_mb: usize = 512;
    const size_bytes = user_size_mb * 1024 * 1024;

    var store = try s.Store.init(std.heap.page_allocator, size_bytes);
    defer store.deinit(std.heap.page_allocator);

    const key = "my.key";
    const value = "my value";

    // TODO: Take result and create a double linked list with tail being the quikest to expire items
    //       Scan and stop when expiry is > now
    //       Do it in its own thread
    try store.put(key, value, s.TypeTag.string, s.NO_EXPIRY);

    // Pass store instance to a TCP server
    // Multiplex that bad boy
}
