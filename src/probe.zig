const std = @import("std");
const xev = @import("xev");
const s = @import("store.zig");

const MAX_RUNS_PER_TICK: usize = 25;

fn onTick(
    userdata: ?*s.Store,
    loop: *xev.Loop,
    c: *xev.Completion,
    result: xev.Timer.RunError!void,
) xev.CallbackAction {
    _ = loop;
    _ = c;
    _ = result catch unreachable;

    if (userdata) |store| {
        // NOTE: technically it can pick same node twice or more
        //       should use a seed route instead
        const r_now: u64 = @intCast(std.time.timestamp());
        var checked: usize = 0;
        var prng = std.Random.DefaultPrng.init(r_now);
        const rand = prng.random();

        store.mu.lock(store.io);
        defer store.mu.unlock(store.io);

        while (checked < std.math.min(MAX_RUNS_PER_TICK, store.capacity)) : (checked += 1) {
            const idx = rand.intRangeLessThan(usize, 0, store.capacity);
            const now: u64 = @intCast(std.time.timestamp());
            const node = &store.table[idx];

            if (node.state == .occupied) {
                if (node.expires > now) {
                    continue;
                }

                node.state = .deleted;
                if (store.occupied > 0) {
                    store.occupied -= 1;
                }
            }
        }
    }

    return .disarm;
}

fn scanLoop(probe: *s.Store) !void {
    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var timer = try xev.Timer.init();
    defer timer.deinit();

    var c: xev.Completion = undefined;
    timer.run(&loop, &c, 100, s.Store, probe, onTick);

    try loop.run(.until_done);
}

pub fn spawn(active_store_ptr: *s.Store) !void {
    const probe_thread = try std.Thread.spawn(.{}, scanLoop, .{active_store_ptr});
    defer probe_thread.join();
}
