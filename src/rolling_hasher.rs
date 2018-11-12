pub trait RollingHasher {
    fn reset(&mut self);
    fn push(&mut self, inc: u8);
    fn hash(&self) -> u32;
}
