use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::BytesMut;
use futures_util::{ready, Stream};
use tokio::io::{AsyncRead, ReadBuf};

use crate::{chunker::Chunker, Chunk};

const REFILL_SIZE: usize = 1024 * 1024;

/// A streaming chunker to use with any source which implements tokio AsyncRead.
pub struct StreamingChunker<C, R> {
    chunk_start: u64,
    buf: BytesMut,
    chunker: C,
    reader: R,
}

impl<C, R> StreamingChunker<C, R> {
    pub fn new(chunker: C, reader: R) -> Self {
        Self {
            chunk_start: 0,
            buf: BytesMut::with_capacity(REFILL_SIZE),
            chunker,
            reader,
        }
    }
}

impl<C, R> Stream for StreamingChunker<C, R>
where
    C: Chunker + Unpin + Send,
    R: AsyncRead + Unpin + Send,
{
    type Item = io::Result<(u64, Chunk)>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        loop {
            let me = &mut *self;
            if !me.buf.is_empty() {
                if let Some(chunk) = me.chunker.next(&mut me.buf) {
                    let offset = me.chunk_start;
                    me.chunk_start += chunk.len() as u64;
                    return Poll::Ready(Some(Ok((offset, chunk))));
                }
            }
            // No chunk found in the buffer. Read data and append to buffer.
            match ready!(refill_buf(cx, &mut me.buf, &mut me.reader)) {
                Ok(0) if me.buf.is_empty() => {
                    // EOF and empty buffer.
                    return Poll::Ready(None);
                }
                Ok(0) => {
                    // EOF but some data in buffer (last chunk).
                    let chunk = Chunk(me.buf.split().freeze());
                    return Poll::Ready(Some(Ok((me.chunk_start, chunk))));
                }
                Ok(_) => {
                    // Buffer refilled.
                }
                Err(err) => return Poll::Ready(Some(Err(err))),
            }
        }
    }
}

fn refill_buf<R>(cx: &mut Context, buf: &mut BytesMut, mut reader: R) -> Poll<io::Result<usize>>
where
    R: AsyncRead + Unpin,
{
    let mut read_count = 0;
    let before_size = buf.len();
    {
        let new_size = before_size + REFILL_SIZE;
        if buf.capacity() < new_size {
            buf.reserve(REFILL_SIZE);
        }
        unsafe {
            // Use unsafe set_len() here instead of resize as we don't care for
            // zeroing the content of buf.
            buf.set_len(new_size);
        }
    }
    while read_count < REFILL_SIZE {
        let offset = before_size + read_count;
        let mut read_buf = ReadBuf::new(&mut buf[offset..]);
        let rc = match Pin::new(&mut reader).poll_read(cx, &mut read_buf) {
            Poll::Ready(Ok(())) if read_buf.filled().is_empty() => break, // EOF
            Poll::Ready(Ok(())) => read_buf.filled().len(),
            Poll::Ready(Err(err)) => {
                buf.resize(before_size + read_count, 0);
                return Poll::Ready(Err(err));
            }
            Poll::Pending => {
                buf.resize(before_size + read_count, 0);
                if read_count > 0 {
                    return Poll::Ready(Ok(read_count));
                }
                return Poll::Pending;
            }
        };
        read_count += rc;
    }
    buf.resize(before_size + read_count, 0);
    Poll::Ready(Ok(read_count))
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::chunker::{Config, FilterBits, FilterConfig};
    use futures_util::StreamExt;
    use std::cmp;

    // The MockSource will return bytes_per_read bytes every other read
    // and Pending every other, to replicate a source with limited I/O.
    struct MockSource {
        data: Vec<u8>,
        offset: usize,
        bytes_per_read: usize,
        pending: bool,
    }

    impl MockSource {
        fn new(data: Vec<u8>, bytes_per_read: usize) -> Self {
            Self {
                data,
                offset: 0,
                bytes_per_read,
                pending: false,
            }
        }
    }

    impl AsyncRead for MockSource {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context,
            buf: &mut ReadBuf,
        ) -> Poll<io::Result<()>> {
            let data_available = self.data.len() - self.offset;
            if data_available == 0 {
                Poll::Ready(Ok(()))
            } else if self.pending {
                self.pending = false;
                Poll::Pending
            } else {
                let read = cmp::min(
                    data_available,
                    cmp::min(buf.initialized().len(), self.bytes_per_read),
                );
                buf.put_slice(&self.data[self.offset..self.offset + read]);
                self.offset += read;
                self.pending = true;
                Poll::Ready(Ok(()))
            }
        }
    }

    #[tokio::test]
    async fn single_byte_per_source_read() {
        for chunker_config in &[
            Config::RollSum(FilterConfig {
                filter_bits: FilterBits(10),
                min_chunk_size: 20,
                max_chunk_size: 600,
                window_size: 10,
            }),
            Config::BuzHash(FilterConfig {
                filter_bits: FilterBits(10),
                min_chunk_size: 20,
                max_chunk_size: 600,
                window_size: 10,
            }),
        ] {
            let source_data: Vec<u8> = {
                let mut seed: usize = 0xa3;
                (0..10000)
                    .map(|v| {
                        seed ^= seed.wrapping_mul(4);
                        (seed ^ v) as u8
                    })
                    .collect()
            };
            let expected_offsets = {
                chunker_config
                    .new_chunker(&mut Box::new(&source_data[..]))
                    .map(|result| {
                        let (offset, _chunk) = result.unwrap();
                        offset
                    })
                    .collect::<Vec<u64>>()
                    .await
            };
            // Only give back a single byte per read from source, should still result in the same
            // result as with unlimited I/O.
            let mut source = MockSource::new(source_data.clone(), 1);
            let offsets = chunker_config
                .new_chunker(&mut source)
                .map(|result| {
                    let (offset, _chunk) = result.unwrap();
                    offset
                })
                .collect::<Vec<u64>>()
                .await;
            assert_eq!(expected_offsets, offsets);
        }
    }

    #[tokio::test]
    async fn zero_data() {
        for chunker_config in &[
            Config::RollSum(FilterConfig {
                filter_bits: FilterBits(5),
                min_chunk_size: 3,
                max_chunk_size: 640,
                window_size: 5,
            }),
            Config::BuzHash(FilterConfig {
                filter_bits: FilterBits(5),
                min_chunk_size: 3,
                max_chunk_size: 640,
                window_size: 5,
            }),
        ] {
            let expected_chunk_offsets: [u64; 0] = [0; 0];
            static SRC: [u8; 0] = [];
            assert_eq!(
                chunker_config
                    .new_chunker(&mut Box::new(&SRC[..]))
                    .map(|result| {
                        let (offset, chunk) = result.unwrap();
                        assert_eq!(chunk.len(), 0);
                        offset
                    })
                    .collect::<Vec<u64>>()
                    .await,
                &expected_chunk_offsets
            );
        }
    }

    #[tokio::test]
    async fn source_smaller_than_hash_window() {
        for chunker_config in &[
            Config::RollSum(FilterConfig {
                filter_bits: FilterBits(5),
                min_chunk_size: 0,
                max_chunk_size: 40,
                window_size: 10,
            }),
            Config::BuzHash(FilterConfig {
                filter_bits: FilterBits(5),
                min_chunk_size: 0,
                max_chunk_size: 40,
                window_size: 10,
            }),
        ] {
            let expected_chunk_offsets: [u64; 1] = [0; 1];
            static SRC: [u8; 5] = [0x1f, 0x55, 0x39, 0x5e, 0xfa];
            assert_eq!(
                chunker_config
                    .new_chunker(&mut Box::new(&SRC[..]))
                    .map(|result| {
                        let (offset, chunk) = result.unwrap();
                        assert_eq!(chunk, Chunk::from(vec![0x1f, 0x55, 0x39, 0x5e, 0xfa]));
                        offset
                    })
                    .collect::<Vec<u64>>()
                    .await,
                &expected_chunk_offsets
            );
        }
    }

    #[tokio::test]
    async fn source_smaller_than_min_chunk() {
        for chunker_config in &[
            Config::RollSum(FilterConfig {
                filter_bits: FilterBits(5),
                min_chunk_size: 10,
                max_chunk_size: 40,
                window_size: 5,
            }),
            Config::BuzHash(FilterConfig {
                filter_bits: FilterBits(5),
                min_chunk_size: 10,
                max_chunk_size: 40,
                window_size: 5,
            }),
        ] {
            let expected_chunk_offsets: [u64; 1] = [0; 1];
            static SRC: [u8; 5] = [0x1f, 0x55, 0x39, 0x5e, 0xfa];
            assert_eq!(
                chunker_config
                    .new_chunker(&mut Box::new(&SRC[..]))
                    .map(|result| {
                        let (offset, chunk) = result.unwrap();
                        assert_eq!(chunk, Chunk::from(vec![0x1f, 0x55, 0x39, 0x5e, 0xfa]));
                        offset
                    })
                    .collect::<Vec<u64>>()
                    .await,
                &expected_chunk_offsets
            );
        }
    }
}
