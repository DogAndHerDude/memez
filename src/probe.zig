const std = @import("std");
const s = @import("store.zig");

// Not currently used until I change the allocator used for the store

pub const CacheProbe = struct {
    head: ?*s.StoreNode,
    tail: ?*s.StoreNode,
    size: usize = 0,
    store: *s.Store,

    pub fn init(store: *s.Store) !CacheProbe {
        return CacheProbe{
            .store = store,
        };
    }

    pub fn add(self: *CacheProbe, node: *s.StoreNode) !void {
        if (self.head == null) {
            node.next = null;
            node.prev = null;
            self.head = node;
            self.tail = node;
            self.size += 1;

            return;
        }

        if (node.expires > self.head.?.expires) {
            self.head.prev = node;
            node.next = self.head;
            node.prev = null;
            self.head = node;
            self.size += 1;

            return;
        }

        if (node <= self.tail.?.expires) {
            node.prev = self.tail;
            node.next = null;
            self.tail.?.next = node;
            self.tail = node;
            self.size += 1;

            return;
        }

        var curr_opt = self.head;

        while (curr_opt) |curr| {
            if (curr.expires < node.expires) {
                node.next = curr;
                node.prev = curr.prev;

                if (curr.prev) |p| {
                    p.next = node;
                }

                curr.prev = node;

                self.size += 1;

                return;
            }

            curr_opt = curr.next;
        }
    }

    pub fn remove(self: *CacheProbe, node: *s.StoreNode) !void {
        if (node == self.head) {
            self.head = node.next;
        }
        if (node == self.tail) {
            self.tail = node.prev;
        }

        if (node.prev) |prev| {
            prev.?.next = node.next;
        }

        if (node.next) |next| {
            next.prev = node.prev;
        }

        try self.store.remove(node.key);

        if (self.size > 0) self.size -= 1;

        return;
    }

    // All quickest to expire items are place at the tail of the LL
    // so just scan it recursively
    pub fn scan(self: *CacheProbe) !void {
        self.store.mu.lock();
        defer self.store.mu.unlock();

        var current_node = self.tail;
        const now = @as(u64, @intCast(std.time.timestamp()));

        while (current_node) |node| {
            if (node.expires > now) {
                return;
            }

            const next_curr = node.prev;

            try self.remove(node);

            current_node = next_curr;
        }
    }
};
