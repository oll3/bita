pub trait RollingHash {
    fn new(window_size: usize, hash_seed: u32) -> Self;
    fn init(&mut self, value: u8) {
        self.input(value);
    }
    fn window_size(&self) -> usize;
    fn input(&mut self, value: u8);
    fn sum(&self) -> u32;
}
