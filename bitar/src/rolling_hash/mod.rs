mod buzhash;
mod rollsum;

pub use buzhash::BuzHash;
pub use rollsum::RollSum;

/// Rolling hash.
pub trait RollingHash {
    /// Returns true if hasher has been initialized.
    fn init_done(&self) -> bool;
    fn init(&mut self, value: u8);
    fn input(&mut self, value: u8);
    fn sum(&self) -> u32;
}
