use bytes::BytesMut;
use core::task::{Context, Poll};
use futures_util::ready;
use std::io;
use tokio::io::AsyncRead;

use super::{refill_read_buf, Chunker, FilterConfig, CHUNKER_BUF_SIZE};
use crate::{rolling_hash::RollingHash, Chunk};

pub struct RollingHashChunker<R, H> {
    source: R,
    hasher: H,
    filter_mask: u32,
    min_chunk_size: usize,
    max_chunk_size: usize,
    read_buf: BytesMut,
    hash_input_limit: usize,
    source_index: u64,
    buf_index: usize,
    chunk_start: u64,
}

impl<R, H> RollingHashChunker<R, H> {
    pub fn new(hasher: H, config: &FilterConfig, source: R) -> Self {
        // Allow for chunk size less than buzhash window
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
            read_buf: BytesMut::with_capacity(config.max_chunk_size + CHUNKER_BUF_SIZE),
            source,
            hash_input_limit,
            source_index: 0,
            buf_index: 0,
            chunk_start: 0,
        }
    }
    fn skip_min_chunk(&mut self)
    where
        H: RollingHash,
    {
        if self.hash_input_limit > 0 && self.buf_index < self.hash_input_limit {
            // Skip past the minimum chunk size to minimize the number of hash inputs
            self.buf_index = std::cmp::min(self.hash_input_limit - 1, self.read_buf.len());
        }
        if self.min_chunk_size > 0 && self.buf_index < self.min_chunk_size {
            // Hash the last part (rolling hash window size bytes) of the minimal possible chunk.
            // There is no need to check the hash sum here since we're still below the minimal
            // chunk size. Still we need the bytes in the hash window to get correct sum when
            // reaching above the minimal chunk size.
            let hasher = &mut self.hasher;
            let input_end = std::cmp::min(self.min_chunk_size - 1, self.read_buf.len());
            self.read_buf[self.buf_index..input_end]
                .iter()
                .for_each(|&val| hasher.input(val));
            self.buf_index = input_end;
        }
    }
    // Scan until end of buffer, chunk boundary (hash sum match) or max chunk size reached
    fn scan_for_boundary(&mut self) -> bool
    where
        H: RollingHash,
    {
        let hasher = &mut self.hasher;
        let filter_mask = self.filter_mask;
        let min_bytes = std::cmp::min(self.max_chunk_size, self.read_buf.len());
        let mut end_index = self.buf_index;
        let found_boundary = self.read_buf[self.buf_index..min_bytes]
            .iter()
            .map(|&val| {
                end_index += 1;
                hasher.input(val);
                hasher.sum()
            })
            .any(|sum| sum | filter_mask == sum);
        self.buf_index = end_index;
        found_boundary || self.buf_index >= self.max_chunk_size
    }
}

impl<R, H> Chunker for RollingHashChunker<R, H>
where
    R: AsyncRead + Unpin,
    H: RollingHash + Unpin,
{
    fn poll_chunk(&mut self, cx: &mut Context) -> Poll<Option<io::Result<(u64, Chunk)>>> {
        loop {
            if self.buf_index >= self.read_buf.len() {
                // Fill buffer from source
                match ready!(refill_read_buf(
                    cx,
                    CHUNKER_BUF_SIZE,
                    &mut self.read_buf,
                    &mut self.source
                )) {
                    Ok(n) if n == 0 => {
                        // EOF
                        if !self.read_buf.is_empty() {
                            let chunk = Chunk(self.read_buf.split().freeze());
                            self.read_buf.resize(0, 0);
                            self.buf_index = 0;
                            return Poll::Ready(Some(Ok((self.chunk_start, chunk))));
                        } else {
                            return Poll::Ready(None);
                        }
                    }
                    Err(e) => return Poll::Ready(Some(Err(e))),
                    _ => {}
                };
                while self.source_index < self.hasher.window_size() as u64
                    && self.buf_index < self.read_buf.len()
                {
                    // Initialize the buzhash
                    self.hasher.init(self.read_buf[self.buf_index]);
                    self.buf_index += 1;
                    self.source_index += 1;
                }
            }
            let start_index = self.buf_index;
            self.skip_min_chunk();
            let found_boundary = self.scan_for_boundary();
            self.source_index += (self.buf_index - start_index) as u64;
            if found_boundary {
                let chunk = Chunk(self.read_buf.split_to(self.buf_index).freeze());
                let chunk_start = self.chunk_start;
                self.buf_index = 0;
                self.chunk_start = self.source_index;
                return Poll::Ready(Some(Ok((chunk_start, chunk))));
            }
        }
    }
}
