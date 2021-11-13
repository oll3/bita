use bytes::BytesMut;

use super::{Chunker, FilterConfig};
use crate::{rolling_hash::RollingHash, Chunk};

pub struct RollingHashChunker<H> {
    hasher: H,
    filter_mask: u32,
    min_chunk_size: usize,
    max_chunk_size: usize,
    hash_input_limit: usize,
    // Offset in buffer.
    offset: usize,
}

impl<H> RollingHashChunker<H> {
    pub fn new(hasher: H, config: &FilterConfig) -> Self {
        // Allow for chunk size less than the rolling hash window.
        let hash_input_limit = if config.min_chunk_size >= config.window_size {
            config.min_chunk_size - config.window_size
        } else {
            0
        };
        Self {
            filter_mask: config.filter_bits.mask(),
            min_chunk_size: config.min_chunk_size,
            max_chunk_size: config.max_chunk_size,
            hasher,
            hash_input_limit,
            offset: 0,
        }
    }

    fn skip_min_chunk(&mut self, buf: &[u8])
    where
        H: RollingHash,
    {
        if self.hash_input_limit > 0 && self.offset < self.hash_input_limit {
            // Skip past the minimum chunk size to minimize the number of hash inputs.
            self.offset = std::cmp::min(self.hash_input_limit - 1, buf.len());
        }
        if self.min_chunk_size > 0 && self.offset < self.min_chunk_size {
            // Hash the last part (rolling hash window size bytes) of the minimal possible chunk.
            // There is no need to check the hash sum here since we're still below the minimal
            // chunk size. Still we need the bytes in the hash window to get correct sum when
            // reaching above the minimal chunk size.
            let hasher = &mut self.hasher;
            let input_end = std::cmp::min(self.min_chunk_size - 1, buf.len());
            buf[self.offset..input_end]
                .iter()
                .for_each(|&val| hasher.input(val));
            self.offset = input_end;
        }
    }

    // Scan until end of buffer, chunk boundary (hash sum match) or max chunk
    // size reached.
    fn scan_for_boundary(&mut self, buf: &[u8]) -> bool
    where
        H: RollingHash,
    {
        let hasher = &mut self.hasher;
        let filter_mask = self.filter_mask;
        let min_bytes = std::cmp::min(self.max_chunk_size, buf.len());
        let mut end_offset = self.offset;
        let found_boundary = buf[self.offset..min_bytes]
            .iter()
            .map(|&val| {
                end_offset += 1;
                hasher.input(val);
                hasher.sum()
            })
            .any(|sum| sum | filter_mask == sum);
        self.offset = end_offset;
        found_boundary || self.offset >= self.max_chunk_size
    }
}

impl<H> Chunker for RollingHashChunker<H>
where
    H: RollingHash,
{
    fn next(&mut self, buf: &mut BytesMut) -> Option<Chunk> {
        // Initialize the hasher if needed.
        while !self.hasher.init_done() && self.offset < buf.len() {
            self.hasher.init(buf[self.offset]);
            self.offset += 1;
        }
        self.skip_min_chunk(&buf[..]);
        let found_boundary = self.scan_for_boundary(buf);
        if found_boundary {
            let offset = self.offset;
            self.offset = 0;
            return Some(Chunk(buf.split_to(offset).freeze()));
        }
        None
    }
}
