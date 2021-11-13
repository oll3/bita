use bytes::BytesMut;

use super::Chunker;
use crate::Chunk;

pub struct FixedSizeChunker {
    chunk_size: usize,
}

impl FixedSizeChunker {
    pub fn new(fixed_size: usize) -> Self {
        Self {
            chunk_size: fixed_size,
        }
    }
}

impl Chunker for FixedSizeChunker {
    fn next(&mut self, buf: &mut BytesMut) -> Option<Chunk> {
        if buf.len() >= self.chunk_size {
            Some(Chunk(buf.split_to(self.chunk_size).freeze()))
        } else {
            None
        }
    }
}
