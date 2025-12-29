const std = @import("std");
const memez = @import("memez");
const h = @import("hasher.zig");
const s = @import("store.zig");

pub fn main() !void {
    //const val = "eat.shit.idiot.lol";
    //const hash = h.hash_key(val);
    const user_size_mb: usize = 512;
    const size_bytes = user_size_mb * 1024 * 1024;

    const store = try s.init(std.heap.page_allocator, size_bytes);
    defer store.deinit(&store, std.heap.page_allocator);
    //try memez.bufferedPrint(hash);
}
