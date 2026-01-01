const std = @import("std");
const s = @import("store.zig");

// Not currently used until I change the allocator used for the store

pub const CacheCleaner = struct {
    head: ?*s.StoreNode,
    tail: ?*s.StoreNode,
    size: usize = 0,
    store: *s.Store,

    pub fn init(store: *s.Store) !CacheCleaner {
        return CacheCleaner{
            .store = store,
        };
    }

    pub fn add(self: *CacheCleaner, node: *s.StoreNode) !void {
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

    pub fn remove(self: *CacheCleaner, node: *s.StoreNode) !void {
        // TODO: Save node copy in case removal errors
        if (node.prev) {
            node.prev.?.next = node.next;
        }

        if (node.next) {
            node.next.?.prev = node.prev;
        }

        try self.store.remove(node.key);

        self.size -= 1;

        return;
    }

    // All quickest to expire items are place at the tail of the LL
    // so just scan it recursively
    pub fn scan(self: *CacheCleaner) !void {
        if (self.tail == null) {
            return;
        }

        return self.rScan(self.tail);
    }

    fn rScan(self: *CacheCleaner, node: s.StoreNode) !void {
        const i_now = std.time.timestamp();
        const now = @as(u64, i_now);

        if (node.expires > 0 and node.expires <= now) {
            self.remove(node);

            if (node.prev != null) {
                return self.rScan(node.prev);
            }

            return;
        }

        // Previous nodes won't be expired either, just quit this bs before it eats up your CPU cycles.
        // delicious, delicious CPU cycles...
        return;
    }
};
