const s = @import("store.zig");

const CacheCleaner = struct {
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

    pub fn remove(node: *s.StoreNode) !void {
        return;
    }

    pub fn scan(self: *CacheCleaner) !void {
        return;
    }
};
