//! Chunker related functions and types.
mod config;
mod fixed_size;
mod rolling_hash;
mod streaming_chunker;

pub use config::{Config, FilterBits, FilterConfig};
pub use fixed_size::FixedSizeChunker;
pub use rolling_hash::RollingHashChunker;
pub use streaming_chunker::StreamingChunker;

use bytes::BytesMut;

use crate::Chunk;

pub trait Chunker {
    /// Scan for the next chunk in the given buffer.
    ///
    /// If None is returned the caller is expected to append more data to the buffer
    /// and call again. If a chunk is returned then that chunk has been split from
    /// buffer and next() can be called again.
    fn next(&mut self, buf: &mut BytesMut) -> Option<Chunk>;
}
