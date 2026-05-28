const std = @import("std");
const xev = @import("xev");
const m = @import("manager.zig");

// HUGE NOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOTE:
// Swap to a linear sweep and track items sweeped
// in a persistant context.
// Otherwise when it hits high enough migration count
// we'll miss those nodes letting the loop dangle,
// trying hard, never finishing... There is a joke in there somewhere.
// Linear will take a while but guarantees that it will be freed. At some point.

fn onTick(
    userdata: ?*m.Manager,
    loop: *xev.Loop,
    c: *xev.Completion,
    result: xev.Timer.RunError!void,
) xev.CallbackAction {
    _ = result catch unreachable;

    if (userdata) |manager| {
        const i_store = manager.inactive_store orelse .disarm;
        const a_store = manager.active_store orelse .disarm;

        i_store.mu.lock();
        a_store.mu.lock();
        defer i_store.mu.unlock();
        defer a_store.mu.unlock();

        if (i_store.occupied == 0) {
            manager.freeInactiveStoreVOLATILE();
            return .disarm;
        }

        var prng = std.Random.DefaultPrng.init(@intCast(std.time.nanoTimestamp()));
        const rand = prng.random();

        var checked: usize = 0;
        var migrated: usize = 0;
        const max_samples: usize = 20;

        while (checked < max_samples) : (checked += 1) {
            const idx = rand.intRangeLessThan(usize, 0, i_store.table.len);
            const node = &i_store.table[idx];

            if (node.state != .occupied) {
                continue;
            }

            a_store.set(node.key, node.value, node.tag, .{ .ttl = node.ttl });

            node.state = .deleted;
            i_store.occupied -= 1;
            migrated += 1;

            if (i_store.occupied == 0) break;
        }

        if (i_store.occupied == 0) {
            manager.freeInactiveStoreVOLATILE();
            return .disarm;
        }

        const threshold = max_samples / 4;
        if (migrated > threshold) {
            c.op.timer.timer.reset(loop, c, 0, m.Manager, manager) catch {
                return .rearm;
            };

            return .disarm;
        }
    }

    return .rearm;
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
