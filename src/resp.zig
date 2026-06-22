const std = @import("std");

pub const Command = union(enum) {
    set: Set,
    get: Get,

    pub const Set = struct {
        key: []const u8,
        value: []const u8,
        ex_seconds: ?u64 = null,
        px_milliseconds: ?u64 = null,
    };

    pub const Get = struct {
        key: []const u8,
    };
};

pub const ParseError = error{
    Incomplete,
    Protocol,
    Unsupported,
};

pub const Parsed = struct {
    command: Command,
    consumed: usize,
};

pub fn parse(buf: []const u8) ParseError!Parsed {
    if (buf.len == 0) return ParseError.Incomplete;
    if (buf[0] != '*') return ParseError.Protocol;

    const header = try readLine(buf, 1);
    const count = std.fmt.parseInt(usize, header.line, 10) catch return ParseError.Protocol;
    if (count < 1) return ParseError.Protocol;

    var pos = header.next;

    const cmd = try readBulk(buf, pos);
    pos = cmd.next;

    if (asciiEqlIgnoreCase(cmd.bytes, "SET")) {
        if (count < 3) return ParseError.Protocol;

        const key = try readBulk(buf, pos);
        pos = key.next;
        const value = try readBulk(buf, pos);
        pos = value.next;

        var ex: ?u64 = null;
        var px: ?u64 = null;

        while (pos < buf.len) {
            const opt = try readBulk(buf, pos);
            pos = opt.next;

            if (asciiEqlIgnoreCase(opt.bytes, "EX")) {
                const val = try readBulk(buf, pos);
                pos = val.next;
                ex = std.fmt.parseInt(u64, val.bytes, 10) catch return ParseError.Protocol;
            } else if (asciiEqlIgnoreCase(opt.bytes, "PX")) {
                const val = try readBulk(buf, pos);
                pos = val.next;
                px = std.fmt.parseInt(u64, val.bytes, 10) catch return ParseError.Protocol;
            } else {
                return ParseError.Protocol;
            }
        }

        return .{
            .command = .{ .set = .{ .key = key.bytes, .value = value.bytes, .ex_seconds = ex, .px_milliseconds = px } },
            .consumed = pos,
        };
    }

    if (asciiEqlIgnoreCase(cmd.bytes, "GET")) {
        if (count != 2) return ParseError.Protocol;

        const key = try readBulk(buf, pos);
        pos = key.next;

        return .{
            .command = .{ .get = .{ .key = key.bytes } },
            .consumed = pos,
        };
    }

    return ParseError.Unsupported;
}

const Line = struct { line: []const u8, next: usize };

fn readLine(buf: []const u8, start: usize) ParseError!Line {
    if (start >= buf.len) return ParseError.Incomplete;
    var i = start;
    while (i + 1 < buf.len) : (i += 1) {
        if (buf[i] == '\r' and buf[i + 1] == '\n') {
            return .{ .line = buf[start..i], .next = i + 2 };
        }
    }
    return ParseError.Incomplete;
}

const Bulk = struct { bytes: []const u8, next: usize };

fn readBulk(buf: []const u8, start: usize) ParseError!Bulk {
    if (start >= buf.len) return ParseError.Incomplete;
    if (buf[start] != '$') return ParseError.Protocol;

    const header = try readLine(buf, start + 1);
    const len = std.fmt.parseInt(usize, header.line, 10) catch return ParseError.Protocol;

    const body_end = header.next + len;
    if (body_end + 2 > buf.len) return ParseError.Incomplete;
    if (buf[body_end] != '\r' or buf[body_end + 1] != '\n') return ParseError.Protocol;

    return .{ .bytes = buf[header.next..body_end], .next = body_end + 2 };
}

fn asciiEqlIgnoreCase(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |x, y| {
        if (std.ascii.toLower(x) != std.ascii.toLower(y)) return false;
    }
    return true;
}

test "parse SET without EX" {
    const wire = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    const r = try parse(wire);
    try std.testing.expectEqual(@as(usize, wire.len), r.consumed);
    try std.testing.expectEqualStrings("foo", r.command.set.key);
    try std.testing.expectEqualStrings("bar", r.command.set.value);
    try std.testing.expect(r.command.set.ex_seconds == null);
}

test "parse SET with EX" {
    const wire = "*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nEX\r\n$2\r\n60\r\n";
    const r = try parse(wire);
    try std.testing.expectEqual(@as(usize, wire.len), r.consumed);
    try std.testing.expectEqual(@as(?u64, 60), r.command.set.ex_seconds);
}

test "parse incomplete returns Incomplete" {
    try std.testing.expectError(ParseError.Incomplete, parse("*3\r\n$3\r\nSE"));
}

test "parse rejects unknown command" {
    try std.testing.expectError(ParseError.Unsupported, parse("*1\r\n$4\r\nPING\r\n"));
}

test "parse GET" {
    const wire = "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
    const r = try parse(wire);
    try std.testing.expectEqual(@as(usize, wire.len), r.consumed);
    try std.testing.expectEqualStrings("foo", r.command.get.key);
}

test "parse GET wrong arity" {
    try std.testing.expectError(ParseError.Protocol, parse("*1\r\n$3\r\nGET\r\n"));
}
