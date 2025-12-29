const store = @import("store.zig");

const CacheCleaner = struct {
    head: ?*store.StoreNode,
    tail: ?*store.StoreNode,
    size: usize = 0,

    pub fn init() !CacheCleaner {
        return CacheCleaner{};
    }

    pub fn add(self: *CacheCleaner, node: *store.StoreNode) !void {
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
};
