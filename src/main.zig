const std = @import("std");
const xev = @import("xev");
const manager = @import("manager.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const user_size_mb: usize = 512;
    const size_bytes = user_size_mb * 1024 * 1024;

    var mngr = try manager.Manager.init(gpa, init.io, size_bytes);
    defer mngr.deinit();

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    const w = try xev.Timer.init();
    defer w.deinit();

    var c: xev.Completion = undefined;

    w.run(&loop, &c, 100, manager.Manager, &mngr, timerCallback) catch |err| {
        std.debug.print("Failed to start timer: {}\n", .{err});
        return;
    };

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
