use bytes::BytesMut;
use core::task::{Context, Poll};
use futures_util::ready;
use std::io;
use tokio::io::AsyncRead;

use super::{refill_read_buf, Chunker, CHUNKER_BUF_SIZE};
use crate::Chunk;

pub struct FixedSizeChunker<R> {
    source: R,
    chunk_size: usize,
    source_index: u64,
    chunk_start: u64,
    read_buf: BytesMut,
}

impl<'a, R> FixedSizeChunker<R> {
    pub fn new(fixed_size: usize, source: R) -> Self {
        Self {
            chunk_size: fixed_size,
            read_buf: BytesMut::with_capacity(fixed_size + CHUNKER_BUF_SIZE),
            source,
            chunk_start: 0,
            source_index: 0,
        }
    }
}
impl<'a, R> Chunker for FixedSizeChunker<R>
where
    R: AsyncRead + Unpin,
{
    fn poll_chunk(&mut self, cx: &mut Context) -> Poll<Option<io::Result<(u64, Chunk)>>> {
        loop {
            if self.read_buf.len() >= self.chunk_size {
                let chunk_start = self.chunk_start;
                let chunk = Chunk(self.read_buf.split_to(self.chunk_size).freeze());
                self.chunk_start = self.source_index;
                return Poll::Ready(Some(Ok((chunk_start, chunk))));
            } else {
                // Fill buffer from source
                let rc = match ready!(refill_read_buf(
                    cx,
                    CHUNKER_BUF_SIZE,
                    &mut self.read_buf,
                    &mut self.source,
                )) {
                    Ok(n) => n,
                    Err(e) => return Poll::Ready(Some(Err(e))),
                };
                if rc == 0 {
                    // EOF
                    if !self.read_buf.is_empty() {
                        let chunk_start = self.chunk_start;
                        let chunk = Chunk(self.read_buf.split().freeze());
                        self.chunk_start = self.source_index;
                        return Poll::Ready(Some(Ok((chunk_start, chunk))));
                    } else {
                        return Poll::Ready(None);
                    }
                }
            }
        }
    }
}
