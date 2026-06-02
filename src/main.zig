const std = @import("std");
const xev = @import("xev");
const manager = @import("manager.zig");
const server = @import("server.zig");
const config = @import("config.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;

    var cfg = config.load(gpa, init.io, "config.toml");
    defer cfg.deinit();
    try config.validate(cfg.value);

    const min_size_bytes = cfg.value.min_size_mb * 1024 * 1024;
    const max_size_bytes = cfg.value.max_size_mb * 1024 * 1024;

    var mngr = try manager.Manager.init(gpa, init.io, min_size_bytes, max_size_bytes);
    defer mngr.deinit();
    try mngr.start();

    var tpool = xev.ThreadPool.init(.{});
    defer tpool.deinit();
    defer tpool.shutdown();

    var loop = try xev.Loop.init(.{ .thread_pool = &tpool });
    defer loop.deinit();

    const w = try xev.Timer.init();
    defer w.deinit();

    var c: xev.Completion = undefined;
    w.run(&loop, &c, 100, manager.Manager, &mngr, timerCallback);

    var srv = try server.Server.init(gpa, &loop, &mngr, cfg.value.host, cfg.value.port);
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
