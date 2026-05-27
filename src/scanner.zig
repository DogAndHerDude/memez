const std = @import("std");
const xev = @import("xev");
const p = @import("probe.zig");
const s = @import("store.zig");

const MAX_RUNS_PER_TICK: usize = 20;

fn onTick(
    userdata: ?*p.s.Store,
    loop: *xev.Loop,
    c: *xev.Completion,
    result: xev.Timer.RunError!void,
) xev.CallbackAction {
    _ = loop;
    _ = c;
    _ = result catch unreachable;

    if (userdata) |store| {
        var checked: usize = 0;
        var cursor: usize = 0; // This could cause issues

        store.mu.lock();
        defer store.mu.unlock();

        while (checked < std.math.min(MAX_RUNS_PER_TICK, store.capacity)) : (checked += 1) {
            cursor = (cursor + 1) % store.capacity; // Sequantial, replace with random access

            const now: u64 = @intCast(std.time.timestamp());
            const node = &store.table[cursor];

            if (node.state == .occupied) {
                if (node.expires > now) {
                    continue;
                }

                node.state = .deleted;
            }
        }
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
}

pub fn spawn(active_store_ptr: *s.Store) !void {
    const probe_thread = try std.Thread.spawn(.{}, scanLoop, .{active_store_ptr});
    defer probe_thread.join();
}
