const std = @import("std");
const xev = @import("xev");
const manager = @import("manager.zig");
const server = @import("server.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const user_size_mb: usize = 512;
    const size_bytes = user_size_mb * 1024 * 1024;

    var mngr = try manager.Manager.init(gpa, init.io, size_bytes);
    defer mngr.deinit();

    var tpool = xev.ThreadPool.init(.{});
    defer tpool.deinit();
    defer tpool.shutdown();

    var loop = try xev.Loop.init(.{ .thread_pool = &tpool });
    defer loop.deinit();

    const w = try xev.Timer.init();
    defer w.deinit();

    var c: xev.Completion = undefined;
    w.run(&loop, &c, 100, manager.Manager, &mngr, timerCallback);

    var srv = try server.Server.init(gpa, &loop, "127.0.0.1", 5882);
    srv.start();

    try loop.run(.until_done);
}

fn timerCallback(
    userdata: ?*manager.Manager,
    loop: *xev.Loop,
    c: *xev.Completion,
    result: xev.Timer.RunError!void,
) xev.CallbackAction {
    _ = loop;
    _ = c;
    _ = result catch unreachable;

    const mngr = userdata orelse return .disarm;

    _ = mngr;

    return .disarm;
}
