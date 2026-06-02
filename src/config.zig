const std = @import("std");
const toml = @import("toml");

pub const Config = struct {
    host: []const u8 = "127.0.0.1",
    port: u16 = 5882,
    min_size_mb: usize = 512,
    // Maximum size the hash table is allowed to grow to, in megabytes. 0
    // means unlimited. Must be either 0 or >= min_size_mb.
    max_size_mb: usize = 0,
};

pub const ValidateError = error{
    MaxSmallerThanMin,
};

pub fn validate(cfg: Config) ValidateError!void {
    if (cfg.max_size_mb != 0 and cfg.max_size_mb < cfg.min_size_mb) {
        std.log.err(
            "CONFIG: max_size_mb ({d}) must be 0 or >= min_size_mb ({d})",
            .{ cfg.max_size_mb, cfg.min_size_mb },
        );
        return ValidateError.MaxSmallerThanMin;
    }
}

pub const Loaded = struct {
    value: Config,
    parsed: ?toml.Parsed(Config),

    pub fn deinit(self: *Loaded) void {
        if (self.parsed) |p| p.deinit();
        self.parsed = null;
    }
};

pub fn load(gpa: std.mem.Allocator, io: std.Io, path: []const u8) Loaded {
    var parser = toml.Parser(Config).init(gpa);
    defer parser.deinit();

    const parsed = parser.parseFile(io, path) catch |err| {
        switch (err) {
            error.FileNotFound => std.log.info("CONFIG: {s} not found; using defaults", .{path}),
            else => std.log.warn("CONFIG: failed to load {s}: {} — using defaults", .{ path, err }),
        }
        return .{ .value = .{}, .parsed = null };
    };

    return .{ .value = parsed.value, .parsed = parsed };
}
