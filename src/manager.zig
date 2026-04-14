const std = @import("std");
const m_store = @import("store.zig");
const probe = @import("probe.zig");
const scanner = @import("scanner.zig");

const ActiveStore = enum {
    primary,
    secondary,
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

    pub fn init(allocator: std.mem.Allocator, size: usize) !Manager {
        // TODO: define remove callback ctx & fn
        var manager = Manager{
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
        const a_table = try self.getActiveTable();
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

                // Better call migrate within the store and handle data there
                // This is just a POC for myself
                const node = try i_table.get(key);
                const n_node = try a_table.set(key, node.value, node.tag);

                n_node.ttl = node.ttl;
                n_node.expires = node.expires;

                return n_node;
            }
        };

        return node;
    }

    // More command types in the future, this is just a basic abstraction right now
    // - add expiry set command
    // - all that kinds of fancy stuff
    // - will probably refer to redis commands for implementation
    pub fn set(self: *Manager, key: []const u8, value: *anyopaque, tag: m_store.TypeTag, opts: m_store.SetOptions) !void {
        var store: ?m_store.Store = undefined;

        //const needs_rehash = try self.needsRehash()
        //if () {
        //    self.rehash();
        //}

        store = try self.getActiveTable();

        if (store) |existing| {
            const new_node = try existing.set(key, value, tag, opts);

            self.probe.add(new_node);

            return;
        }

        // TODO: error
        return;
    }

    fn getActiveTable(self: *Manager) !*m_store.Store {
        switch (self.active_store) {
            .primary => {
                if (self.p_store) |store| {
                    return &store;
                }

                // TODO: Errors
                return;
            },
            .secondary => {
                if (self.s_store) |store| {
                    return &store;
                }

                // TODO: Errors
                return;
            },
        }
    }

    fn getInactiveTable(self: *Manager) !*m_store.Store {
        switch (self.active_store) {
            .primary => {
                if (self.s_store) |store| {
                    return &store;
                }

                // TODO: Errors
                return;
            },
            .secondary => {
                if (self.p_store) |store| {
                    return &store;
                }

                // TODO: Errors
                return;
            },
        }
    }

    fn needsRehash() !bool {
        return false;
    }

    fn rehash() !void {}
};
