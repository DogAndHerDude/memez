const FNV_OFFSET: u64 = 14695981039346656037;
const FNV_PRIME: u64 = 1099511628211;

pub fn hashKey(key: []const u8) !u64 {
    var hash: u64 = FNV_OFFSET;

    for (key) |char| {
        hash ^= @as(u64, char);
        hash *%= FNV_PRIME;
    }

    return hash;
}
