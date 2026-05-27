const std = @import("std");
const xev = @import("xev");
const p = @import("probe.zig");
const s = @import("store.zig");

// A large prime number for massive spatial scattering
const PRIME_STEP: usize = 131071;
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
        const r_now: u64 = @intCast(std.time.timestamp());
        var checked: usize = 0;
        var prng = std.Random.DefaultPrng.init(r_now);
        const rand = prng.random();

        store.mu.lock();
        defer store.mu.unlock();

        while (checked < std.math.min(MAX_RUNS_PER_TICK, store.capacity)) : (checked += 1) {
            const idx = rand.intRangeLessThan(usize, 0, store.capacity);
            const now: u64 = @intCast(std.time.timestamp());
            const node = &store.table[idx];

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
