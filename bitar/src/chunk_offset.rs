use std::cmp::Ordering;

#[derive(Eq, PartialEq, Debug, Clone, Copy)]
pub struct ChunkOffset {
    pub offset: u64,
    pub size: usize,
}

impl ChunkOffset {
    pub fn new(offset: u64, size: usize) -> Self {
        Self { offset, size }
    }

    pub fn end(&self) -> u64 {
        self.offset + self.size as u64
    }
}

impl Ord for ChunkOffset {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.offset.cmp(&other.offset) {
            Ordering::Equal => self.size.cmp(&other.size),
            v => v,
        }
    }
}

impl PartialOrd for ChunkOffset {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
