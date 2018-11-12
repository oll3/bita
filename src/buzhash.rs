use rolling_hasher::RollingHasher;

pub struct BuzHash {
    seed: u32,
    max_seed: u32,
    buf: Vec<u8>,
    index: usize,
    window: usize,
    hash_sum: u32,
}

impl BuzHash {
    pub fn new(window: usize) -> Self {
        // Generate max seed value
        let seed = 2953;
        let mut max_seed: u32 = seed;
        for _ in 0..window {
            max_seed = max_seed.wrapping_mul(max_seed);
        }
        BuzHash {
            seed: seed,
            max_seed: max_seed,
            index: 0,
            buf: vec![0; window],
            window: window,
            hash_sum: 0,
        }
    }

    // Reset hash state machine
    pub fn reset(&mut self) {
        self.hash_sum = 0;
        self.buf[0] = 0;
        self.index = 0;
    }

    // Push and process a byte
    pub fn push(&mut self, val: u8) {
        self.hash_sum = (val as u32).wrapping_add(
            (self.hash_sum)
                .wrapping_sub(self.max_seed.wrapping_mul(self.buf[self.index] as u32))
                .wrapping_mul(self.seed),
        );
        self.buf[self.index] = val;
        self.index += 1;
        if self.index >= self.window {
            self.index = 0;
        }
    }

    // Get current hash sum
    pub fn hash(&self) -> u32 {
        self.hash_sum
    }
}

impl RollingHasher for BuzHash {
    fn reset(&mut self) {
        self.reset();
    }
    fn push(&mut self, inc: u8) {
        self.push(inc);
    }
    fn hash(&self) -> u32 {
        self.hash()
    }
}
