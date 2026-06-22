const std = @import("std");
const xev = @import("xev");
const s = @import("store.zig");

const MAX_RUNS_PER_TICK: usize = 25;

const ProbeContext = struct {
    io: std.Io,
    store: *s.Store,
    shutdown: *xev.Async,
    stop: bool = false,
};

pub const Probe = struct {
    thread: std.Thread,
    shutdown: *xev.Async,
    ctx: *ProbeContext,
    gpa: std.mem.Allocator,

    pub fn stop(self: *Probe) void {
        self.shutdown.notify() catch |err| {
            std.log.err("PROBE: notify shutdown failed: {}", .{err});
        };
        self.thread.join();
        self.shutdown.deinit();
        self.gpa.destroy(self.shutdown);
        self.gpa.destroy(self.ctx);
    }
};

fn onTick(
    userdata: ?*ProbeContext,
    loop: *xev.Loop,
    c: *xev.Completion,
    result: xev.Timer.RunError!void,
) xev.CallbackAction {
    _ = loop;
    _ = c;
    _ = result catch unreachable;

    const ctx = userdata orelse return .disarm;
    if (ctx.stop) return .disarm;

    const io = ctx.io;
    const store = ctx.store;
    // NOTE: technically it can pick same node twice or more
    //       should use a seed route instead
    const r_now: u64 = @intCast(std.Io.Clock.now(.real, io).toNanoseconds());
    var checked: usize = 0;
    var prng = std.Random.DefaultPrng.init(r_now);
    const rand = prng.random();

    store.mu.lockUncancelable(store.io);
    defer store.mu.unlock(store.io);

    while (checked < @min(MAX_RUNS_PER_TICK, store.capacity)) : (checked += 1) {
        const idx = rand.intRangeLessThan(usize, 0, store.capacity);
        const now: u64 = @intCast(std.Io.Clock.now(.real, io).toNanoseconds());
        const node = &store.table[idx];

        if (node.state == .occupied) {
            if (node.expires_at > now) {
                continue;
            }

            node.state = .deleted;
            if (store.occupied > 0) {
                store.occupied -= 1;
            }
        }
    }

    return .disarm;
}

fn onShutdown(
    userdata: ?*ProbeContext,
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

fn scanLoop(ctx: *ProbeContext) !void {
    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var timer = try xev.Timer.init();
    defer timer.deinit();

    var c_timer: xev.Completion = undefined;
    var c_async: xev.Completion = undefined;

    timer.run(&loop, &c_timer, 100, ProbeContext, ctx, onTick);
    ctx.shutdown.wait(&loop, &c_async, ProbeContext, ctx, onShutdown);

    try loop.run(.until_done);
}

pub fn spawn(allocator: std.mem.Allocator, store: *s.Store, io: std.Io) !Probe {
    const shutdown = try allocator.create(xev.Async);
    errdefer allocator.destroy(shutdown);
    shutdown.* = try xev.Async.init();
    errdefer shutdown.deinit();

    const ctx = try allocator.create(ProbeContext);
    errdefer allocator.destroy(ctx);
    ctx.* = .{
        .io = io,
        .store = store,
        .shutdown = shutdown,
    };

    const thread = try std.Thread.spawn(.{}, scanLoop, .{ctx});

    return .{
        .thread = thread,
        .shutdown = shutdown,
        .ctx = ctx,
        .gpa = allocator,
    };
}
