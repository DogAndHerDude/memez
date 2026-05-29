const std = @import("std");
const xev = @import("xev");
const m = @import("manager.zig");
const m_store = @import("store.zig");

// HUGE NOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOTE:
// Swap to a linear sweep and track items sweeped
// in a persistant context.
// Otherwise when it hits high enough migration count
// we'll miss those nodes letting the loop dangle,
// trying hard, never finishing... There is a joke in there somewhere.
// Linear will take a while but guarantees that it will be freed. At some point.

const MigratorContext = struct {
    io: std.Io,
    manager: *m.Manager,
    shutdown: *xev.Async,
    prefer_linear_scan: bool = false,
    // Resume point for the linear sweep once we've flipped out of random sampling.
    // Survives across ticks so we don't lose progress between timer fires.
    linear_cursor: usize = 0,
    // Tracks which inactive Store the cursor refers to. When a fresh rehash
    // installs a different Store under inactive_store, onTick resets the
    // scan state instead of indexing into a new table with a stale cursor.
    last_i_store: ?*m_store.Store = null,
    stop: bool = false,

    // xev plumbing populated by migrationLoop; used by onTick to reset the
    // timer to fire ASAP when migration work is being productive. The pointed-to
    // objects live on migrationLoop's stack, which outlives every callback
    // invocation since the thread doesn't exit until loop.run returns.
    loop: *xev.Loop = undefined,
    timer: xev.Timer = undefined,
    c_timer: *xev.Completion = undefined,
    c_cancel: *xev.Completion = undefined,
};

// Occupancy ratio at or below which random sampling stops finding nodes quickly
// enough to drain the inactive table; flip into a linear sweep at that point so
// migration is guaranteed to terminate.
const LINEAR_THRESHOLD_NUM: usize = 1;
const LINEAR_THRESHOLD_DEN: usize = 4;
const MAX_SAMPLES_PER_TICK: usize = 20;

pub const Migrator = struct {
    thread: std.Thread,
    shutdown: *xev.Async,
    ctx: *MigratorContext,
    gpa: std.mem.Allocator,

    pub fn stop(self: *Migrator) void {
        self.shutdown.notify() catch |err| {
            std.log.err("MIGRATOR: notify shutdown failed: {}", .{err});
        };
        self.thread.join();
        self.shutdown.deinit();
        self.gpa.destroy(self.shutdown);
        self.gpa.destroy(self.ctx);
    }
};

fn onTick(
    userdata: ?*MigratorContext,
    loop: *xev.Loop,
    c: *xev.Completion,
    result: xev.Timer.RunError!void,
) xev.CallbackAction {
    _ = loop;
    _ = c;
    _ = result catch unreachable;

    const ctx = userdata orelse return .disarm;
    if (ctx.stop) return .disarm;

    const manager = ctx.manager;
    const io = ctx.io;

    // Capture the store pointers under manager.mu so we don't race with rehash.
    // Heap boxing makes the captured *Store stable for the rest of this tick:
    // the migrator is the only writer of inactive_store (via free), and Manager.deinit
    // joins this thread before destroying stores.
    manager.mu.lockUncancelable(manager.io);
    const i_store_opt = manager.inactive_store;
    const a_store_opt = manager.active_store;
    manager.mu.unlock(manager.io);

    const i_store = i_store_opt orelse return .disarm;
    const a_store = a_store_opt orelse return .disarm;

    // Detect a fresh inactive store (different *Store than last tick) and reset
    // the scan state. Pointer-compare is good enough in practice; an allocator
    // could in theory hand back the same address for a different Store, but
    // that requires the previous one to have been fully freed first — which
    // means an entire migrate-then-free cycle completed, and the worst case
    // is that we keep a stale cursor for one tick.
    if (ctx.last_i_store != i_store) {
        ctx.prefer_linear_scan = false;
        ctx.linear_cursor = 0;
        ctx.last_i_store = i_store;
    }

    var inactive_empty = false;
    var migrated: usize = 0;

    {
        i_store.mu.lockUncancelable(i_store.io);
        defer i_store.mu.unlock(i_store.io);

        if (i_store.occupied == 0) {
            inactive_empty = true;
        } else {
            // Flip into linear sweep once occupancy is low enough that random
            // sampling stops being productive. Sticky: occupancy in the inactive
            // store is monotonically non-increasing, so we never need to flip back.
            if (!ctx.prefer_linear_scan and
                i_store.occupied * LINEAR_THRESHOLD_DEN <= i_store.capacity * LINEAR_THRESHOLD_NUM)
            {
                ctx.prefer_linear_scan = true;
            }

            if (ctx.prefer_linear_scan) {
                var scanned: usize = 0;
                while (scanned < MAX_SAMPLES_PER_TICK and ctx.linear_cursor < i_store.table.len) : ({
                    scanned += 1;
                    ctx.linear_cursor += 1;
                }) {
                    const node = &i_store.table[ctx.linear_cursor];
                    if (node.state != .occupied) continue;

                    _ = a_store.set(node.key, node.value, node.tag, .{
                        .ttl = node.ttl,
                        .expires_at = node.expires,
                    }) catch {
                        // Don't advance the cursor on failure — retry this slot next tick.
                        return .rearm;
                    };

                    node.state = .deleted;
                    i_store.occupied -= 1;
                    migrated += 1;

                    if (i_store.occupied == 0) break;
                }

                // Wrap the cursor if we walked off the end. With inactive being
                // write-only-by-the-migrator this shouldn't leave any nodes behind,
                // but resetting keeps the next tick well-defined.
                if (ctx.linear_cursor >= i_store.table.len) ctx.linear_cursor = 0;
            } else {
                var prng = std.Random.DefaultPrng.init(@intCast(std.Io.Clock.now(.real, io).toNanoseconds()));
                const rand = prng.random();

                var checked: usize = 0;
                while (checked < MAX_SAMPLES_PER_TICK) : (checked += 1) {
                    const idx = rand.intRangeLessThan(usize, 0, i_store.table.len);
                    const node = &i_store.table[idx];

                    if (node.state != .occupied) continue;

                    _ = a_store.set(node.key, node.value, node.tag, .{
                        .ttl = node.ttl,
                        .expires_at = node.expires,
                    }) catch {
                        return .rearm;
                    };

                    node.state = .deleted;
                    i_store.occupied -= 1;
                    migrated += 1;

                    if (i_store.occupied == 0) break;
                }
            }

            inactive_empty = (i_store.occupied == 0);
        }
    }

    // Release i_store.mu before freeing the store, otherwise the deferred unlock
    // above would touch freed memory.
    if (inactive_empty) {
        manager.freeInactiveStoreVOLATILE();
        return .disarm;
    }

    // Productive tick: fire again immediately instead of waiting the full
    // interval. Same 25% threshold as the random→linear flip — if more than
    // a quarter of samples were migrated, there's plenty more to drain.
    if (migrated * LINEAR_THRESHOLD_DEN > MAX_SAMPLES_PER_TICK * LINEAR_THRESHOLD_NUM) {
        ctx.timer.reset(ctx.loop, ctx.c_timer, ctx.c_cancel, 0, MigratorContext, ctx, onTick);
        return .disarm;
    }

    return .rearm;
}

fn onShutdown(
    userdata: ?*MigratorContext,
    loop: *xev.Loop,
    c: *xev.Completion,
    result: xev.Async.WaitError!void,
) xev.CallbackAction {
    _ = loop;
    _ = c;
    _ = result catch {};

    const ctx = userdata orelse return .disarm;
    ctx.stop = true;
    return .disarm;
}

fn migrationLoop(ctx: *MigratorContext) !void {
    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var timer = try xev.Timer.init();
    defer timer.deinit();

    // xev requires c_timer / c_cancel to be initialized to .{} (not undefined)
    // because Timer.reset inspects them to decide whether a cancel is needed.
    var c_timer: xev.Completion = .{};
    var c_cancel: xev.Completion = .{};
    var c_async: xev.Completion = undefined;

    ctx.loop = &loop;
    ctx.timer = timer;
    ctx.c_timer = &c_timer;
    ctx.c_cancel = &c_cancel;

    timer.run(&loop, &c_timer, 100, MigratorContext, ctx, onTick);
    ctx.shutdown.wait(&loop, &c_async, MigratorContext, ctx, onShutdown);

    try loop.run(.until_done);
}

pub fn spawn(allocator: std.mem.Allocator, manager: *m.Manager, io: std.Io) !Migrator {
    const shutdown = try allocator.create(xev.Async);
    errdefer allocator.destroy(shutdown);
    shutdown.* = try xev.Async.init();
    errdefer shutdown.deinit();

    const ctx = try allocator.create(MigratorContext);
    errdefer allocator.destroy(ctx);
    ctx.* = .{
        .io = io,
        .manager = manager,
        .shutdown = shutdown,
    };

    const thread = try std.Thread.spawn(.{}, migrationLoop, .{ctx});

    return .{
        .thread = thread,
        .shutdown = shutdown,
        .ctx = ctx,
        .gpa = allocator,
    };
}
