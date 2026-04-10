const std = @import("std");
const xev = @import("xev");
const manager = @import("manager.zig");

const ev_vars = struct {
    manager: *manager.Manager,
};

pub fn main() !void {
    // TODO: read config.toml file
    // read min_mem_size
    // read max_mem_size
    const user_size_mb: usize = 512;
    const size_bytes = user_size_mb * 1024 * 1024;

    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    const allocator = gpa.allocator();
    var mngr = try manager.Manager.init(allocator, size_bytes);

    defer mngr.deinit();
    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    const w = try xev.Timer.init();
    defer w.deinit();

    var c: xev.Completion = undefined;
    const v = ev_vars{
        .manager = &mngr,
    };
    w.run(&loop, &c, 100, ev_vars, @constCast(&v), &timerCallback);

    try loop.run(.until_done);
}

fn timerCallback(
    userdata: ?*ev_vars,
    loop: *xev.Loop,
    c: *xev.Completion,
    result: xev.Timer.RunError!void,
) xev.CallbackAction {
    _ = userdata.?;
    _ = loop;
    _ = c;
    _ = result catch unreachable;
    return .disarm;
}
