const std = @import("std");
const xev = @import("xev");
const memez = @import("memez");
const manager = @import("manager.zig");

pub fn main() !void {
    // TODO: read config.toml file
    // read min_mem_size
    // read max_mem_size
    const user_size_mb: usize = 512;
    const size_bytes = user_size_mb * 1024 * 1024;

    const allocator = std.heap.GeneralPurposeAllocator(.{});
    const mngr = try manager.Manager.init(allocator, size_bytes);

    defer mngr.deinit();

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    const w = try xev.Timer.init();
    defer w.deinit();

    // 5s timer
    var c: xev.Completion = undefined;
    w.run(&loop, &c, 5000, void, null, &timerCallback);

    try loop.run(.until_done);
}

fn timerCallback(
    userdata: ?*void,
    loop: *xev.Loop,
    c: *xev.Completion,
    result: xev.Timer.RunError!void,
) xev.CallbackAction {
    _ = userdata;
    _ = loop;
    _ = c;
    _ = result catch unreachable;
    return .disarm;
}
