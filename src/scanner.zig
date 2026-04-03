const std = @import("std");
const p = @import("probe.zig");

fn scanLoop(probe: *p.CacheProbe) !void {
    // TODO: Prefer Event Loop
    //       Implement it myself or just use available ones?
    while (true) {
        // TODO: Check how many items have been scanned
        //       if x then scan again
        try probe.scan();
        std.time.sleep(std.time.ns_per_s / 10);
    }
}

pub fn spawn(probe: *p.CacheProbe) !void {
    const probe_thread = try std.Thread.spawn(.{}, scanLoop, .{&probe});
    defer probe_thread.join();
}
