const std = @import("std");
const m_store = @import("store.zig");

const ActiveStore = enum {
    primary,
    secondary,
};

pub const Manager = struct {
    const UPSIZE_THRESHOLD: f16 = 0.75;
    const DOWNSIZE_THRESHOLD: f16 = 0.50;

    const UPSIZE_FACTOR: usize = 2;
    const DOWNSIZE_FACTOR: usize = 2;

    p_store: ?m_store.Store,
    s_store: ?m_store.Store,

    active_store: ActiveStore = .primary,

    pub fn init(allocator: std.mem.Allocator, size: usize) !Manager {
        return Manager{
            .p_store = m_store.Store.init(allocator, size),
        };
    }

    pub fn get(self: *Manager, key: []const u8) !*m_store.StoreNode {
        const a_table = try self.getActiveTable();
        const node = a_table.get(key) catch |err| {
            if (err == m_store.StoreError.KeyNotFound) {
                //TODO: Check if inactive store is allocated
                //      if true then check for key there
                //      if found, migrate it to new table
                //      then return pointer from the new table
            }
        };

        return node;
    }

    pub fn set(self: *Manager, key: []const u8, value: *const anyopaque, tag: m_store.TypeTag, expires: u64) !void {
        var store: ?m_store.Store = undefined;

        if (try self.needsRehash()) {
            self.rehash();
        }

        store = try self.getActiveTable();

        if (store) |existing| {
            try existing.set(key, value, tag, expires);
        }

        // TODO: error
        return;
    }

    fn getActiveTable(self: *Manager) !m_store.Store {
        switch (self.active_store) {
            .primary => {
                if (self.p_store) |store| {
                    return store;
                }

                // TODO: Errors
                return;
            },
            .secondary => {
                if (self.s_store) |store| {
                    return store;
                }

                return;
            },
        }
    }

    fn needsRehash(self: *Manager) !bool {
        return false;
    }

    fn rehash(self: *Manager) !void {}
};
