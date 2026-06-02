const std = @import("std");
const posix = std.posix;
const xev = @import("xev");

const READ_BUF_SIZE: usize = 4096;

pub const Server = struct {
    gpa: std.mem.Allocator,
    loop: *xev.Loop,
    listener: xev.TCP,
    bound: std.Io.net.IpAddress,
    accept_c: xev.Completion,

    pub fn init(gpa: std.mem.Allocator, loop: *xev.Loop, host: []const u8, port: u16) !Server {
        const addr = try std.Io.net.IpAddress.parse(host, port);
        const listener = try xev.TCP.init(addr);
        try listener.bind(addr);
        try listener.listen(128);

        return .{
            .gpa = gpa,
            .loop = loop,
            .listener = listener,
            .bound = addr,
            .accept_c = undefined,
        };
    }

    pub fn start(self: *Server) void {
        std.log.info("SERVER: listening on {f}", .{self.bound});
        self.listener.accept(self.loop, &self.accept_c, Server, self, onAccept);
    }

    fn onAccept(
        ud: ?*Server,
        loop: *xev.Loop,
        _: *xev.Completion,
        r: xev.AcceptError!xev.TCP,
    ) xev.CallbackAction {
        const self = ud orelse return .disarm;

        const client = r catch |err| {
            std.log.err("SERVER: accept failed: {}", .{err});
            return .rearm;
        };

        const conn = Connection.create(self.gpa, loop, client) catch |err| {
            std.log.err("SERVER: connection alloc failed: {}", .{err});
            client.close(loop, &self.accept_c, void, null, dropClose);
            return .rearm;
        };
        if (peerAddress(client.fd)) |peer| {
            std.log.info("SERVER: connection accepted fd={d} peer={f}", .{ client.fd, peer });
        } else {
            std.log.info("SERVER: connection accepted fd={d}", .{client.fd});
        }
        conn.beginRead();

        return .rearm;
    }

    fn dropClose(
        _: ?*void,
        _: *xev.Loop,
        _: *xev.Completion,
        _: xev.TCP,
        _: xev.CloseError!void,
    ) xev.CallbackAction {
        return .disarm;
    }
};

const Connection = struct {
    gpa: std.mem.Allocator,
    loop: *xev.Loop,
    socket: xev.TCP,
    fd: posix.socket_t,
    c: xev.Completion,
    buf: [READ_BUF_SIZE]u8,

    fn create(gpa: std.mem.Allocator, loop: *xev.Loop, socket: xev.TCP) !*Connection {
        const conn = try gpa.create(Connection);
        conn.* = .{
            .gpa = gpa,
            .loop = loop,
            .socket = socket,
            .fd = socket.fd,
            .c = undefined,
            .buf = undefined,
        };
        return conn;
    }

    fn beginRead(self: *Connection) void {
        self.socket.read(self.loop, &self.c, .{ .slice = &self.buf }, Connection, self, onRead);
    }

    fn onRead(
        ud: ?*Connection,
        _: *xev.Loop,
        _: *xev.Completion,
        _: xev.TCP,
        _: xev.ReadBuffer,
        r: xev.ReadError!usize,
    ) xev.CallbackAction {
        const self = ud orelse return .disarm;

        const n = r catch |err| {
            switch (err) {
                error.EOF => {},
                else => std.log.err("SERVER: read failed: {}", .{err}),
            }
            self.beginClose();
            return .disarm;
        };

        self.socket.write(self.loop, &self.c, .{ .slice = self.buf[0..n] }, Connection, self, onWrite);
        return .disarm;
    }

    fn onWrite(
        ud: ?*Connection,
        _: *xev.Loop,
        _: *xev.Completion,
        _: xev.TCP,
        _: xev.WriteBuffer,
        r: xev.WriteError!usize,
    ) xev.CallbackAction {
        const self = ud orelse return .disarm;

        _ = r catch |err| {
            std.log.err("SERVER: write failed: {}", .{err});
            self.beginClose();
            return .disarm;
        };

        self.beginRead();
        return .disarm;
    }

    fn beginClose(self: *Connection) void {
        self.socket.close(self.loop, &self.c, Connection, self, onClose);
    }

    fn onClose(
        ud: ?*Connection,
        _: *xev.Loop,
        _: *xev.Completion,
        _: xev.TCP,
        r: xev.CloseError!void,
    ) xev.CallbackAction {
        const self = ud orelse return .disarm;
        _ = r catch |err| std.log.err("SERVER: close failed: {}", .{err});
        std.log.info("SERVER: connection closed fd={d}", .{self.fd});
        self.gpa.destroy(self);
        return .disarm;
    }
};

fn peerAddress(fd: posix.socket_t) ?std.Io.net.IpAddress {
    var storage: posix.sockaddr.storage = undefined;
    var len: posix.socklen_t = @sizeOf(@TypeOf(storage));
    posix.getpeername(fd, @ptrCast(&storage), &len) catch return null;
    const sa: *const posix.sockaddr = @ptrCast(&storage);
    switch (sa.family) {
        posix.AF.INET => {
            const in: *const posix.sockaddr.in = @ptrCast(@alignCast(sa));
            return .{ .ip4 = .{
                .bytes = @bitCast(in.addr),
                .port = std.mem.bigToNative(u16, in.port),
            } };
        },
        posix.AF.INET6 => {
            const in6: *const posix.sockaddr.in6 = @ptrCast(@alignCast(sa));
            return .{ .ip6 = .{
                .bytes = in6.addr,
                .port = std.mem.bigToNative(u16, in6.port),
                .flow = in6.flowinfo,
                .interface = .{ .index = in6.scope_id },
            } };
        },
        else => return null,
    }
}
