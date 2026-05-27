const std = @import("std");
const xev = @import("xev");
const m = @import("manager.zig");

const MAX_RUNS_PER_TICK: usize = 25;

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
        const r_now: u64 = @intCast(std.time.timestamp());
        var checked: usize = 0;
        var prng = std.Random.DefaultPrng.init(r_now);
        const rand = prng.random();
        const inactive_store = manager.inactive_store;
        const active_store = manager.active_store;

        if (inactive_store) |i_store| {
            if (active_store) |a_store| {
                i_store.mu.lock();
                a_store.mu.lock();
                defer i_store.mu.unlock();
                defer a_store.mu.unlock();

                while (checked < std.math.min(MAX_RUNS_PER_TICK, i_store.capacity)) : (checked += 1) {
                    const idx = rand.intRangeLessThan(usize, 0, i_store.capacity);
                    const node = &i_store.table[idx];

                    if (node.state != .occupied) {
                        continue;
                    }

                    a_store.set(node.key, node.value, node.tag, .{ .ttl = node.ttl });
                }

                // TODO: deinit inactive store, default to null
            }
        }
    }

    return .disarm;
}

fn migrationLoop(manager: m.Manager) !void {
    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var timer = try xev.Timer.init();
    defer timer.deinit();

    var c: xev.Completion = undefined;
    timer.run(&loop, &c, m.Manager, manager, onTick);

    try loop.run(.until_done);
}

fn spawn(manager: m.Manager) !void {
    const migrator_trhead = try std.Thread.spawn(.{}, migrationLoop, .{manager});
    defer migrator_trhead.join();
}
