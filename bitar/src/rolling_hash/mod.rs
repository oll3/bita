mod buzhash;
mod rollsum;

pub use buzhash::BuzHash;
pub use rollsum::RollSum;

/// Rolling hash.
pub trait RollingHash {
    fn new(window_size: usize) -> Self;
    fn init(&mut self, value: u8) {
        self.input(value);
    }
    fn window_size(&self) -> usize;
    fn input(&mut self, value: u8);
    fn sum(&self) -> u32;
}
