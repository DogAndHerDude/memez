const std = @import("std");
const posix = std.posix;
const xev = @import("xev");
const manager = @import("manager.zig");
const m_store = @import("store.zig");
const resp = @import("resp.zig");

const IN_BUF_SIZE: usize = 4096;
// Sized to comfortably hold any bulk-string GET response whose value came in
// through a SET on a same-sized IN_BUF: header `$<len>\r\n` plus value plus
// trailing CRLF stays under this with room to spare.
const OUT_BUF_SIZE: usize = IN_BUF_SIZE + 32;

pub const Server = struct {
    gpa: std.mem.Allocator,
    loop: *xev.Loop,
    mngr: *manager.Manager,
    listener: xev.TCP,
    bound: std.Io.net.IpAddress,
    accept_c: xev.Completion,

    pub fn init(
        gpa: std.mem.Allocator,
        loop: *xev.Loop,
        mngr: *manager.Manager,
        host: []const u8,
        port: u16,
    ) !Server {
        const addr = try std.Io.net.IpAddress.parse(host, port);
        const listener = try xev.TCP.init(addr);
        try listener.bind(addr);
        try listener.listen(128);

        return .{
            .gpa = gpa,
            .loop = loop,
            .mngr = mngr,
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

        const conn = Connection.create(self.gpa, loop, self.mngr, client) catch |err| {
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
    mngr: *manager.Manager,
    socket: xev.TCP,
    fd: posix.socket_t,
    c: xev.Completion,

    in_buf: [IN_BUF_SIZE]u8,
    in_len: usize,

    out_buf: [OUT_BUF_SIZE]u8,
    out_len: usize,
    out_sent: usize,
    close_after_write: bool,

    fn create(
        gpa: std.mem.Allocator,
        loop: *xev.Loop,
        mngr: *manager.Manager,
        socket: xev.TCP,
    ) !*Connection {
        const conn = try gpa.create(Connection);
        conn.* = .{
            .gpa = gpa,
            .loop = loop,
            .mngr = mngr,
            .socket = socket,
            .fd = socket.fd,
            .c = undefined,
            .in_buf = undefined,
            .in_len = 0,
            .out_buf = undefined,
            .out_len = 0,
            .out_sent = 0,
            .close_after_write = false,
        };
        return conn;
    }

    fn beginRead(self: *Connection) void {
        if (self.in_len >= self.in_buf.len) {
            self.queueError("ERR command too large");
            return;
        }
        self.socket.read(self.loop, &self.c, .{ .slice = self.in_buf[self.in_len..] }, Connection, self, onRead);
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

        self.in_len += n;
        self.processNext();
        return .disarm;
    }

    fn processNext(self: *Connection) void {
        const parsed = resp.parse(self.in_buf[0..self.in_len]) catch |err| switch (err) {
            error.Incomplete => {
                self.beginRead();
                return;
            },
            error.Protocol => {
                self.queueError("ERR protocol error");
                return;
            },
            error.Unsupported => {
                self.queueError("ERR unsupported command");
                return;
            },
        };

        self.execute(parsed.command) catch |err| {
            std.log.err("SERVER: execute failed: {}", .{err});
            self.queueError("ERR internal error");
            return;
        };

        const remaining = self.in_len - parsed.consumed;
        if (remaining > 0) {
            std.mem.copyForwards(u8, self.in_buf[0..remaining], self.in_buf[parsed.consumed..self.in_len]);
        }
        self.in_len = remaining;

        self.beginWrite();
    }

    fn execute(self: *Connection, cmd: resp.Command) !void {
        switch (cmd) {
            .set => |s| {
                const key = try self.gpa.dupe(u8, s.key);
                errdefer self.gpa.free(key);
                const value = try self.gpa.dupe(u8, s.value);
                errdefer self.gpa.free(value);

                const ttl_ns: u64 = if (s.ex_seconds) |sec| sec * std.time.ns_per_s else 0;
                _ = try self.mngr.set(key, value.ptr, value.len, m_store.TypeTag.string, .{ .ttl = ttl_ns });

                self.queueSimple("OK");
            },
            .get => |g| {
                const node = self.mngr.get(g.key) catch |err| switch (err) {
                    m_store.StoreError.KeyNotFound => {
                        self.queueNil();
                        return;
                    },
                    else => return err,
                };
                self.queueBulkFromNode(node);
            },
        }
    }

    fn queueNil(self: *Connection) void {
        const w = std.fmt.bufPrint(&self.out_buf, "$-1\r\n", .{}) catch unreachable;
        self.out_len = w.len;
        self.out_sent = 0;
    }

    fn queueBulkFromNode(self: *Connection, node: *m_store.StoreNode) void {
        // Strings are the only payload SET stores today; reject anything else
        // explicitly so we don't reinterpret an integer or list as bytes.
        if (node.tag != m_store.TypeTag.string) {
            self.queueError("ERR value is not a string");
            return;
        }

        const bytes_ptr: [*]const u8 = @ptrCast(node.value);
        const value_bytes = bytes_ptr[0..node.value_len];

        const header = std.fmt.bufPrint(&self.out_buf, "${d}\r\n", .{value_bytes.len}) catch {
            self.queueError("ERR value too large");
            return;
        };
        const after_header = header.len;
        const total = after_header + value_bytes.len + 2;
        if (total > self.out_buf.len) {
            self.queueError("ERR value too large");
            return;
        }
        @memcpy(self.out_buf[after_header .. after_header + value_bytes.len], value_bytes);
        self.out_buf[total - 2] = '\r';
        self.out_buf[total - 1] = '\n';
        self.out_len = total;
        self.out_sent = 0;
    }

    fn queueSimple(self: *Connection, msg: []const u8) void {
        const w = std.fmt.bufPrint(&self.out_buf, "+{s}\r\n", .{msg}) catch unreachable;
        self.out_len = w.len;
        self.out_sent = 0;
    }

    fn queueError(self: *Connection, msg: []const u8) void {
        const w = std.fmt.bufPrint(&self.out_buf, "-{s}\r\n", .{msg}) catch unreachable;
        self.out_len = w.len;
        self.out_sent = 0;
        self.close_after_write = true;
        self.beginWrite();
    }

    fn beginWrite(self: *Connection) void {
        self.socket.write(
            self.loop,
            &self.c,
            .{ .slice = self.out_buf[self.out_sent..self.out_len] },
            Connection,
            self,
            onWrite,
        );
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

        const n = r catch |err| {
            std.log.err("SERVER: write failed: {}", .{err});
            self.beginClose();
            return .disarm;
        };

        self.out_sent += n;
        if (self.out_sent < self.out_len) {
            self.beginWrite();
            return .disarm;
        }

        self.out_len = 0;
        self.out_sent = 0;

        if (self.close_after_write) {
            self.beginClose();
            return .disarm;
        }

        // Pipelining: if the buffer already holds another full command, process
        // it now; otherwise wait for more bytes.
        if (self.in_len > 0) {
            self.processNext();
        } else {
            self.beginRead();
        }
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
