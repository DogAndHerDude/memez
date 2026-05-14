const std = @import("std");
const xev = @import("xev");
const m = @import("manager.zig");

fn onTick(
    userdata: ?*m.Manager,
    loop: *xev.Loop,
    c: *xev.Completion,
    result: xev.Timer.RunError!void,
) xev.CallbackAction {
    _ = loop;
    _ = c;
    _ = result catch unreachable;

    if (userdata) |manager| {
        // TODO:
        // - take 25
        // - check if needs migration
        // - migrate if needed
        // - if took less than whatever amount of time do it again
        // - otherwise just stop and wait for next tick
    }

    return .disarm;
}

fn migrationLoop(manager: m.Manager) !void {
    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var timer = try xev.Timer.init();
    defer timer.deinit();

    var c: xev.Completion = undefined;
    timer.run(&loop, &c, m.Manager, onTick);

    try loop.run(.until_done);
}

fn spawn(manager: m.Manager) !void {
    const migrator_trhead = try std.Thread.spawn(.{}, migrationLoop, .{manager});
    defer migrator_trhead.join();
}
