const std = @import("std");
const memez = @import("memez");
const h = @import("hasher.zig");

pub fn main() !void {
    const val = "eat.shit.idiot.lol";
    const hash = h.hash_key(val);

    try memez.bufferedPrint(hash);
}
