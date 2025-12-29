const StoreNode = struct {
    expires: u64,
    value: u64,
};

const Store = struct {
    list: []StoreNode,

    pub fn init(size: usize) !Store {}
};
