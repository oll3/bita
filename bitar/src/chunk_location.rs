use std::cmp::Ordering;

/// A chunk's size and offset in source file.
#[derive(Eq, PartialEq, Debug, Clone)]
pub struct ChunkLocation {
    pub offset: u64,
    pub size: usize,
}

impl ChunkLocation {
    pub fn new(offset: u64, size: usize) -> Self {
        Self { offset, size }
    }
    /// Get the end offset of the chunk.
    pub fn end(&self) -> u64 {
        self.offset + self.size as u64
    }
}

impl Ord for ChunkLocation {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.offset.cmp(&other.offset) {
            Ordering::Equal => self.size.cmp(&other.size),
            v => v,
        }
    }
}

impl PartialOrd for ChunkLocation {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
