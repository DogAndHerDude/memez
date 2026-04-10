const std = @import("std");
const xev = @import("xev");
const p = @import("probe.zig");

fn onTick(
    userdata: ?*p.CacheProbe,
    loop: *xev.Loop,
    c: *xev.Completion,
    result: xev.Timer.RunError!void,
) xev.CallbackAction {
    _ = loop;
    _ = c;
    _ = result catch unreachable;

    if (userdata) |probe| {
        probe.scan() catch |err| {
            std.debug.print("SCANNER: scan error: {}\n", .{err});
        };
    }

    return .disarm;
}

fn scanLoop(probe: *p.CacheProbe) !void {
    // in your spawn or init
    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var timer = try xev.Timer.init();
    defer timer.deinit();

    var c: xev.Completion = undefined;
    timer.run(&loop, &c, 100, p.CacheProbe, probe, onTick);

    try loop.run(.until_done);
    // TODO: Prefer Event Loop
    //       Implement it myself or just use available ones?
    while (true) {
        // TODO: Check how many items have been scanned
        //       if x then scan again
        std.Thread.sleep(std.time.ns_per_s / 10);
    }
}

pub fn spawn(probe: *p.CacheProbe) !void {
    const probe_thread = try std.Thread.spawn(.{}, scanLoop, .{probe});
    defer probe_thread.join();
}
