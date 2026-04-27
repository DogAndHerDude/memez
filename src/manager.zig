const std = @import("std");
const m_store = @import("store.zig");
const probe = @import("probe.zig");
const scanner = @import("scanner.zig");

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

    p_store: ?m_store.Store = null,
    s_store: ?m_store.Store = null,

    probe: probe.CacheProbe,

    active_store: ActiveStore = .primary,

    gpa: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, size: usize) !Manager {
        // TODO: define remove callback ctx & fn
        var manager = Manager{
            .gpa = allocator,
            .p_store = try m_store.Store.init(allocator, size),
            .probe = try probe.CacheProbe.init(allocator),
        };

        if (manager.p_store) |*store| {
            store.on_remove = &probe.CacheProbe.onRemove;
            store.on_remove_ctx = &manager.probe;
        }

        manager.probe.on_remove = &m_store.Store.onRemove;
        manager.probe.on_remove_ctx = &manager.p_store;

        try scanner.spawn(&manager.probe);

        return manager;
    }

    pub fn deinit(self: *Manager) void {
        if (self.p_store) |*store| {
            store.deinit();
        }

        if (self.s_store) |*store| {
            store.deinit();
        }

        self.probe.deinit();
    }

    pub fn get(self: *Manager, key: []const u8) !*m_store.StoreNode {
        defer self.freeInactiveTable();

        const a_table = try self.getActiveTable();

        a_table.mu.lock();
        defer a_table.mu.unlock();

        const node = a_table.get(key) catch |err| {
            if (err == m_store.StoreError.KeyNotFound) {
                //TODO: Check if inactive store is allocated
                //      if true then check for key there
                //      if found, migrate it to new table
                //      then return pointer from the new table
                //      swap the address in the probe

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

    fn getActiveTable(self: *Manager) !*m_store.Store {
        switch (self.active_store) {
            .primary => {
                if (self.p_store) |*store| {
                    return store;
                }

                // TODO: Errors
                return;
            },
            .secondary => {
                if (self.s_store) |*store| {
                    return store;
                }

                // TODO: Errors
                return;
            },
        }
    }

    fn getInactiveTable(self: *Manager) !*m_store.Store {
        switch (self.active_store) {
            .primary => {
                if (self.s_store) |*store| {
                    return store;
                }

                // TODO: Errors
                return;
            },
            .secondary => {
                if (self.p_store) |*store| {
                    return store;
                }

                // TODO: Errors
                return;
            },
        }
    }

    pub fn needsRehash(self: *Manager) RehashDirection {
        const a_table = try self.getActiveTable();
        const occupied: f16 = @floatCast(a_table.occupied);
        const capacity: f16 = @floatCast(a_table.capacity);
        const load = occupied / capacity;

        if (load >= UPSIZE_THRESHOLD) return .up;
        if (load <= DOWNSIZE_THRESHOLD and a_table.occupied > a_table.min_capacity) return .down;

        return .none;
    }

    pub fn rehash(self: *Manager, direction: RehashDirection) !void {
        switch (direction) {
            .up => {
                const a_store = try self.getActiveTable();
                const n_size = @sizeOf(a_store.table) * UPSIZE_FACTOR;
                const n_store = try m_store.Store.init(self.gpa, n_size);

                if (self.active_store == .primary and self.s_store == null) {
                    self.s_store = n_store;
                } else if (self.active_store == .secondary and self.p_store == null) {
                    self.p_store = n_store;
                } else {
                    // An oopsie occured
                    // But generally I should be dealing with the existing store somehow
                    n_store.deinit();
                }

                // TODO: Set new table to active position
                //       Set old table to inactive
                //       If old inactive table exists and is not empty then migrate remaining data? Drop it? What do?
                //          - Generally I'll be running another thread for active rehash and granting 1ms of workload every 100ms to migrate or idx % 64 == 0
                //          - This will allow to actively migrate and check for any rehash needs instead of lazy which can fill up the whole table and explode it
                //       If old inactive table is empty just nuke it from orbit first and then spawn them

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
