const std = @import("std");
const s = @import("store.zig");

// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// !!!!!!!!!!!!!!! NOTE !!!!!!!!!!!!!!!
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// Switch to using the passed hash table for the future
// Less overhead checking for order
// I can utilize lazy expiry easier
// I can run a 100ms scan and pick x random items
// I can scan again if I deleted 25% of them
// If not, just go back to sleep
// Goodnight
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

const ProbeNode = struct {
    store_node: *s.StoreNode,
    next: ?*s.StoreNode = null,
    prev: ?*s.StoreNode = null,
};

// Not currently used until I change the allocator used for the store

pub const CacheProbe = struct {
    head: ?*ProbeNode,
    tail: ?*ProbeNode,
    size: usize = 0,

    mu: std.Thread.Mutex = std.Thread.Mutex{},

    // Callabacks for the store
    on_remove: ?*const fn (*anyopaque, *s.StoreNode) void = null,
    on_remove_ctx: ?anyopaque = null,

    gpa: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) !CacheProbe {
        return CacheProbe{
            .gpa = allocator,
        };
    }

    pub fn deinit(self: *CacheProbe) !void {
        var probe_node = self.head;

        self.mu.lock();
        defer self.mu.unlock();

        while (probe_node) |current| {
            probe_node = current.next;

            self.gpa.destroy(current);
        }
    }

    pub fn add(self: *CacheProbe, node: *s.StoreNode) !void {
        const probe_node = try self.gpa.create(ProbeNode);

        probe_node.* = .{
            .store_node = node,
        };

        if (self.head == null) {
            self.head = probe_node;
            self.tail = probe_node;
            self.size += 1;

            return;
        }

        if (node.expires < self.head.?.store_node.expires) {
            self.head.prev = probe_node;
            probe_node.next = self.head;
            probe_node.prev = null;
            self.head = probe_node;
            self.size += 1;

            return;
        }

        if (node.expires >= self.tail.?.store_node.expires) {
            probe_node.prev = self.tail;
            probe_node.next = null;
            self.tail.?.next = probe_node;
            self.tail = probe_node;
            self.size += 1;

            return;
        }

        var curr_opt = self.head;

        while (curr_opt) |curr| {
            if (curr.store_node.expires < node.expires) {
                probe_node.next = curr;
                probe_node.prev = curr.prev;

                if (curr.prev) |p| {
                    p.next = probe_node;
                }

                curr.prev = probe_node;

                self.size += 1;

                return;
            }

            curr_opt = curr.next;
        }
    }

    pub fn remove(self: *CacheProbe, node: *s.StoreNode) !void {
        var curr_opt = self.head;

        while (curr_opt) |curr| {
            if (curr.store_node == node) {
                if (curr == self.head) self.head = curr.next;
                if (curr == self.tail) self.tail = curr.prev;

                if (curr.prev) |prev| prev.next = curr.next;
                if (curr.next) |next| next.prev = curr.prev;

                if (self.size > 0) self.size -= 1;

                self.gpa.destroy(curr);
                return;
            }
            curr_opt = curr.next;
        }

        return error.NodeNotFound;
    }

    // Callbacks for external structs
    pub fn onRemove(ctx: *anyopaque, node: *s.StoreNode) !void {
        const self: *CacheProbe = @ptrCast(@alignCast(ctx));

        self.mu.lock();
        defer self.mu.unlock();

        try self.remove(node);
    }

    pub fn scan(self: *CacheProbe) !void {
        self.mu.lock();
        defer self.mu.unlock();

        var current_node = self.head;
        const now: u64 = @intCast(std.time.timestamp());

        while (current_node) |node| {
            if (node.store_node.expires > now) {
                // Everything after this is later, no point scanning further, save them CPU cycles, save the planet
                return;
            }

            const next = node.next;

            try self.remove(node.store_node);

            if (self.on_remove) |cb| {
                cb(self.on_remove_ctx.?, node);
            }

            current_node = next;
        }
    }
};
