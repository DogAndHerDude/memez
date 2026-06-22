const std = @import("std");
const m_store = @import("store.zig");
const probe = @import("probe.zig");
const migrator = @import("migrator.zig");

const ManagerError = error{ FailedToInitialize, NoActiveStore, FailedRehashNoEmptySlot };

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

    // Stores are heap-boxed so their addresses (and their internal mutexes) stay
    // stable across rehash. Manager only owns the pointers; the Store structs
    // themselves never move once allocated.
    active_store: ?*m_store.Store = null,
    inactive_store: ?*m_store.Store = null,

    probe_worker: ?probe.Probe = null,
    migrator_worker: ?migrator.Migrator = null,

    gpa: std.mem.Allocator,

    // Upper bound on the byte size of a single store's table. 0 = unlimited.
    max_size: usize = 0,

    io: std.Io,
    mu: std.Io.Mutex = .init,

    pub fn init(allocator: std.mem.Allocator, io: std.Io, min_size: usize, max_size: usize) ManagerError!Manager {
        if (max_size != 0 and max_size < min_size) {
            std.log.err("MANAGER: max_size ({d}) < min_size ({d})", .{ max_size, min_size });
            return ManagerError.FailedToInitialize;
        }

        var manager = Manager{
            .io = io,
            .gpa = allocator,
            .max_size = max_size,
        };

        const a_store = allocator.create(m_store.Store) catch |err| {
            std.log.err("MANAGER: failed to allocate active store: {}", .{err});
            return ManagerError.FailedToInitialize;
        };
        errdefer allocator.destroy(a_store);

        a_store.* = m_store.Store.init(allocator, io, min_size) catch |err| {
            std.log.err("MANAGER: failed to init active store: {}", .{err});
            return ManagerError.FailedToInitialize;
        };
        errdefer a_store.deinit();

        manager.active_store = a_store;

        return manager;
    }

    // Spawns worker threads. Must be called after Manager.init returns, so
    // workers capture the Manager's final address rather than the temporary
    // returned from init.
    pub fn start(self: *Manager) ManagerError!void {
        const a_store = self.active_store orelse return ManagerError.NoActiveStore;

        self.probe_worker = probe.spawn(self.gpa, a_store, self.io) catch |err| {
            std.log.err("MANAGER: failed to spawn probe: {}", .{err});
            return ManagerError.FailedToInitialize;
        };
        errdefer if (self.probe_worker) |*w| {
            w.stop();
            self.probe_worker = null;
        };

        self.migrator_worker = migrator.spawn(self.gpa, self, self.io) catch |err| {
            std.log.err("MANAGER: failed to spawn migrator: {}", .{err});
            return ManagerError.FailedToInitialize;
        };
    }

    pub fn deinit(self: *Manager) void {
        // Stop workers before tearing down the stores they reference.
        if (self.probe_worker) |*w| w.stop();
        self.probe_worker = null;

        if (self.migrator_worker) |*w| w.stop();
        self.migrator_worker = null;

        if (self.active_store) |a_store| {
            a_store.deinit();
            self.gpa.destroy(a_store);
        }
        if (self.inactive_store) |i_store| {
            i_store.deinit();
            self.gpa.destroy(i_store);
        }

        self.active_store = null;
        self.inactive_store = null;
    }

    pub fn get(self: *Manager, key: []const u8) !*m_store.StoreNode {
        self.mu.lockUncancelable(self.io);
        defer self.mu.unlock(self.io);

        const a_table = self.active_store orelse return ManagerError.NoActiveStore;

        const node = a_table.get(key) catch |err| {
            if (err != m_store.StoreError.KeyNotFound) return err;

            const i_table = self.inactive_store orelse return m_store.StoreError.KeyNotFound;

            // Store.get locks the table's mutex internally; don't pre-lock here.
            const i_node = try i_table.get(key);
            const n_node = self.migrateUnsafe(i_table, i_node) catch |m_err| {
                std.debug.print("MANAGER: migrate error: {}\n", .{m_err});
                return m_store.StoreError.KeyNotFound;
            };

            return n_node;
        };

        return node;
    }

    // More command types in the future, this is just a basic abstraction right now
    // - add expiry set command
    // - all that kinds of fancy stuff
    // - will probably refer to redis commands for implementation
    pub fn set(self: *Manager, key: []const u8, value: *const anyopaque, value_len: usize, tag: m_store.TypeTag, opts: m_store.SetOptions) !*m_store.StoreNode {
        self.mu.lockUncancelable(self.io);
        defer self.mu.unlock(self.io);

        const rehash_direction = try self.needsRehash();
        try self.rehashUnsafe(rehash_direction);

        const store = self.active_store orelse return m_store.StoreError.TableNotInitialized;
        const new_node = try store.set(key, value, value_len, tag, opts);

        return new_node;
    }

    // TODO: Probably return on remove or some shit like that as an option
    pub fn remove(self: *Manager, key: []const u8) !void {
        self.mu.lockUncancelable(self.io);
        defer self.mu.unlock(self.io);
        if (self.active_store) |store| try store.remove(key);
    }

    pub fn migrateUnsafe(self: *Manager, i_table: *m_store.Store, node: *m_store.StoreNode) !*m_store.StoreNode {
        const a_store = self.active_store orelse return ManagerError.NoActiveStore;

        // expires_at preserves the source node's absolute expiry instead of
        // letting a_store.set extend it by `now + ttl`. The override is applied
        // under a_store.mu, so the probe can't observe a transient bumped value.
        const n_node = try a_store.set(node.key, node.value, node.value_len, node.tag, .{
            .ttl = node.ttl,
            .expires_at = node.expires_at,
        });

        m_store.resetNode(node);
        if (i_table.occupied > 0) i_table.occupied -= 1;

        return n_node;
    }

    pub fn needsRehash(self: *Manager) ManagerError!RehashDirection {
        const a_store = self.active_store orelse return ManagerError.NoActiveStore;

        const occupied: f16 = @floatFromInt(a_store.occupied);
        const capacity: f16 = @floatFromInt(a_store.capacity);
        const load = occupied / capacity;

        if (load >= UPSIZE_THRESHOLD) return .up;
        if (load <= DOWNSIZE_THRESHOLD and a_store.occupied > a_store.min_capacity) return .down;

        return .none;
    }

    pub fn rehash(self: *Manager, direction: RehashDirection) ManagerError!void {
        self.mu.lockUncancelable(self.io);
        defer self.mu.unlock(self.io);
        try self.rehashUnsafe(direction);
    }

    // Caller must hold self.mu.
    pub fn rehashUnsafe(self: *Manager, direction: RehashDirection) ManagerError!void {
        switch (direction) {
            .up => {
                const a_store = self.active_store orelse return ManagerError.NoActiveStore;

                // Need free inactive slot to swap into; otherwise the old store has nowhere to go.
                if (self.inactive_store != null) return ManagerError.FailedRehashNoEmptySlot;

                const n_size = a_store.table.len * @sizeOf(m_store.StoreNode) * UPSIZE_FACTOR;

                // Cap growth at max_size if configured. The set that triggered
                // this rehash still completes on the existing table; sets stop
                // succeeding only once the un-grown table genuinely fills up.
                if (self.max_size != 0 and n_size > self.max_size) {
                    std.log.warn(
                        "MANAGER: upsize to {d} bytes would exceed max_size {d}; staying at current capacity",
                        .{ n_size, self.max_size },
                    );
                    return;
                }

                const n_store = self.gpa.create(m_store.Store) catch |err| {
                    std.log.err("MANAGER: rehash up allocate failed: {}", .{err});
                    return ManagerError.FailedToInitialize;
                };
                errdefer self.gpa.destroy(n_store);
                n_store.* = m_store.Store.init(self.gpa, self.io, n_size) catch |err| {
                    std.log.err("MANAGER: rehash up init failed: {}", .{err});
                    return ManagerError.FailedToInitialize;
                };
                errdefer n_store.deinit();

                self.inactive_store = self.active_store;
                self.active_store = n_store;
                return;
            },
            .down => {
                // We do not have the free slot to scale down for now
                if (self.inactive_store != null) return;

                const a_store = self.active_store orelse return ManagerError.NoActiveStore;

                // NOTE: DUUUMB, will never scale down because I overwrite the starting configuration min_size
                // meaning min_capacity will always match capacity.
                // Leaving this heap of shit here so I could refactor it
                if (a_store.capacity <= a_store.min_capacity) return;

                const n_size = a_store.table.len * @sizeOf(m_store.StoreNode) / DOWNSIZE_FACTOR;
                const n_store = self.gpa.create(m_store.Store) catch |err| {
                    std.log.err("MANAGER: rehash down allocate failed: {}", .{err});
                    return ManagerError.FailedToInitialize;
                };
                errdefer self.gpa.destroy(n_store);
                n_store.* = m_store.Store.init(self.gpa, self.io, n_size) catch |err| {
                    std.log.err("MANAGER: rehash down init failed: {}", .{err});
                    return ManagerError.FailedToInitialize;
                };
                errdefer n_store.deinit();

                self.inactive_store = self.active_store;
                self.active_store = n_store;
                return;
            },
            .none => return,
        }
    }

    // Locks self.mu internally. Safe to call from a worker thread.
    pub fn freeInactiveStoreVOLATILE(self: *Manager) void {
        self.mu.lockUncancelable(self.io);
        defer self.mu.unlock(self.io);

        if (self.inactive_store) |i_store| {
            i_store.deinit();
            self.gpa.destroy(i_store);
            self.inactive_store = null;
        }
    }
};
