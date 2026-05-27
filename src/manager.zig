const std = @import("std");
const m_store = @import("store.zig");
const probe = @import("probe.zig");

const ActiveStore = enum {
    primary,
    secondary,
};

const RehashDirection = enum {
    up,
    down,
    none,
};

pub const Manager = struct {
    const UPSIZE_THRESHOLD: f16 = 0.75;
    const DOWNSIZE_THRESHOLD: f16 = 0.50;

    const UPSIZE_FACTOR: usize = 2;
    const DOWNSIZE_FACTOR: usize = 2;

    // TODO: refactor to a tuple or active/inactive stores
    //       swap values as necessary
    //       pass that to migrator thread
    //       avoid making pointers out of them for the cache miss
    active_store: ?m_store.Store = null,
    inactive_store: ?m_store.Store = null,

    active_store_ptr: ?*m_store.Store = null,

    gpa: std.mem.Allocator,

    mu: std.Thread.Mutex = std.Thread.Mutex{},

    pub fn init(allocator: std.mem.Allocator, size: usize) !Manager {
        var manager = Manager{
            .gpa = allocator,
            .active_store = try m_store.Store.init(allocator, size),
        };

        if (manager.active_store) |*store| {
            // TODO: Update PTR on rehash
            manager.active_store_ptr = store;

            try probe.spawn(store);

            return manager;
        }

        // TODO: errors, cause we done goofed major!
        return;
    }

    pub fn deinit(self: *Manager) void {
        if (self.active_store) |*store| {
            store.deinit();
        }

        if (self.inactive_store) |*store| {
            store.deinit();
        }

        self.active_store = null;
        self.inactive_store = null;
        self.active_store_ptr = null;
    }

    pub fn get(self: *Manager, key: []const u8) !*m_store.StoreNode {
        defer self.freeInactiveTable();

        const a_table = try self.getActiveTable();

        a_table.mu.lock();
        defer a_table.mu.unlock();

        const node = a_table.get(key) catch |err| {
            if (err == m_store.StoreError.KeyNotFound) {
                const i_table = self.getInactiveTable() catch |i_err| {
                    std.debug.print("MANAGER: get error: {}\n", .{i_err});

                    return m_store.StoreError.KeyNotFound;
                };

                i_table.mu.lock();
                defer i_table.mu.unlock();

                // Better call migrate within the store and handle data there
                // This is just a POC for myself
                const node = try i_table.get(key);
                const n_node = try self.migrateUnsafe(node) catch |m_err| {
                    std.debug.print("MANAGER: migrate error: {}\n", .{m_err});

                    return m_store.StoreError.KeyNotFound;
                };

                n_node.ttl = node.ttl;
                n_node.expires = node.expires;

                m_store.resetNode(node);

                if (i_table.occupied > 0) i_table.occupied -= 1;

                return n_node;
            }
        };

        return node;
    }

    // More command types in the future, this is just a basic abstraction right now
    // - add expiry set command
    // - all that kinds of fancy stuff
    // - will probably refer to redis commands for implementation
    pub fn set(self: *Manager, key: []const u8, value: *anyopaque, tag: m_store.TypeTag, opts: m_store.SetOptions) !*m_store.StoreNode {
        var store: ?m_store.Store = undefined;
        const rehash_direction = try self.needsRehash();

        try self.rehash(rehash_direction);

        store = try self.getActiveTable();

        if (store) |a_store| {
            a_store.mu.lock();
            a_store.mu.unlock();

            const new_node = try a_store.set(key, value, tag, opts);

            self.probe.add(new_node);

            return new_node;
        }

        // TODO: error
        return;
    }

    // TODO: Remove fn

    pub fn migrateUnsafe(self: *Manager, i_table: *m_store.Store, node: *m_store.StoreNode) !*m_store.StoreNode {
        const n_node = try self.set(node.key, node.value, node.tag, .{});

        // Since I do not save initial options, and expiry would be shifted from now + original ttl
        // I just basically save all the relevant values to it
        // Perhaps zig has a better approach for this
        n_node.ttl = node.ttl;
        n_node.expires = node.expires;

        if (n_node.expires > 0) {
            self.probe.swap(node, n_node);
        }

        m_store.resetNode(node);

        if (i_table.occupied > 0) i_table.occupied -= 1;

        return n_node;
    }

    pub fn needsRehash(self: *Manager) !RehashDirection {
        if (self.active_store) |a_store| {
            const occupied: f16 = @floatCast(a_store.occupied);
            const capacity: f16 = @floatCast(a_store.capacity);
            const load = occupied / capacity;

            if (load >= UPSIZE_THRESHOLD) return .up;
            if (load <= DOWNSIZE_THRESHOLD and a_store.occupied > a_store.min_capacity) return .down;

            return .none;
        }

        // TODO: errors
        return;
    }

    pub fn rehash(self: *Manager, direction: RehashDirection) !void {
        switch (direction) {
            .up => {
                self.mu.lock();
                defer self.mu.unlock();

                var n_store: ?m_store.Store = null;

                if (self.active_store) |a_store| {
                    const n_size = @sizeOf(a_store.table) * UPSIZE_FACTOR;
                    n_store = try m_store.Store.init(self.gpa, n_size);
                } else {
                    n_store.?.deinit();

                    // TODO: errors, we done goof'd
                    return;
                }

                if (n_store and self.inactive_store == null) |new_store| {
                    self.inactive_store = self.active_store;
                    self.active_store = new_store;

                    return;
                }

                if (n_store and self.inactive_store != null) {
                    // Gotta force migrate otherwise we're screwed
                }

                // We done goof'd here too
                return;
            },
            .down => {
                return;
            },
            .none => {
                return;
            },
        }
    }

    fn freeInactiveTable(self: *Manager) void {
        var i_table = self.getInactiveTable() catch |err| {
            std.debug.print("MANAGER: freeInactiveTable error: {}\n", .{err});
        };

        if (i_table.occupied == 0) {
            i_table.deinit();
            i_table = null;
        }
    }
};
