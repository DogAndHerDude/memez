const std = @import("std");
const memez = @import("memez");
const s = @import("store.zig");
const p = @import("probe.zig");

pub fn main() !void {
    // TODO: read config.toml file
    // read min_mem_size
    // read max_mem_size
    const user_size_mb: usize = 512;
    const size_bytes = user_size_mb * 1024 * 1024;

    var allocator = std.heap.GeneralPurposeAllocator(.{});

    var probe = try p.CacheProbe.init();
    var store = try s.Store.init(allocator.allocator(), size_bytes);

    store.on_remove = probe.onRemove;
    store.on_remove_ctx = &probe;

    probe.on_remove = store.onRemove;
    probe.on_remove_ctx = &store;

    // use the cleaner on a seperate thread
    // var cleaner = try c.CacheCleaner.init(&store);

    defer store.deinit(allocator);

    // TODO: spawn std.Thread.spawn
    //       run cleanup every second, if more than 25% of items cleaned up run again
    //       deal with locks and throw the whole project away because the main thread cannot access the data...

    // Pass store instance to a TCP server
    // Multiplex that bad boy
}
