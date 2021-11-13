use crate::rolling_hash::RollingHash;

const CHAR_OFFSET: u32 = 31;

/// Rolling hash algorithm which can be used for chunking.
///
/// Based on the rsync/bup rolling hash implementation.
pub struct RollSum {
    s1: u32,
    s2: u32,
    window: Vec<u8>,
    offset: usize,
}

impl RollSum {
    /// Create a new instance of BuzHash with the given window size.
    pub fn new(window_size: usize) -> Self {
        Self {
            s1: window_size as u32 * CHAR_OFFSET,
            s2: window_size as u32 * (window_size - 1) as u32 * CHAR_OFFSET,
            offset: 0,
            window: vec![0; window_size],
        }
    }
    fn add(&mut self, drop: u8, add: u8) {
        let drop = drop as u32;
        self.s1 = self.s1.wrapping_add(add as u32);
        self.s1 = self.s1.wrapping_sub(drop);
        self.s2 = self.s2.wrapping_add(self.s1);
        self.s2 = self
            .s2
            .wrapping_sub((self.window.len() as u32) * (drop + CHAR_OFFSET));
    }
    /// Process a single byte.
    pub fn input(&mut self, in_val: u8) {
        let out_val = self.window[self.offset];
        self.add(out_val, in_val);
        self.window[self.offset] = in_val;
        self.offset += 1;
        if self.offset >= self.window.len() {
            self.offset = 0;
        }
    }
    /// Get current hash sum.
    pub fn sum(&self) -> u32 {
        (self.s1 << 16) | (self.s2 & 0xffff)
    }
}

impl RollingHash for RollSum {
    fn init_done(&self) -> bool {
        // No init needed.
        true
    }
    fn init(&mut self, _value: u8) {
        unimplemented!("not used")
    }
    fn input(&mut self, value: u8) {
        self.input(value)
    }
    fn sum(&self) -> u32 {
        self.sum()
    }
}
