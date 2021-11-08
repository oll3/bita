mod http_range_request;
mod http_reader;
mod io_reader;

use async_trait::async_trait;
use bytes::Bytes;
use core::pin::Pin;
use futures_util::stream::Stream;

// Re-export archive reader implementations.
pub use http_reader::{HttpReader, HttpReaderError};
pub use io_reader::IoReader;

use crate::ChunkOffset;

/// Trait may be implemented for any type to be read as an archive.
#[async_trait]
pub trait ArchiveReader {
    type Error;

    /// Read a single chunk from archive.
    async fn read_at<'a>(&'a mut self, offset: u64, size: usize) -> Result<Bytes, Self::Error>;

    /// Read multiple chunks from archive. Returns a stream of the requested chunks.
    fn read_chunks<'a>(
        &'a mut self,
        chunks: Vec<ChunkOffset>,
    ) -> Pin<Box<dyn Stream<Item = Result<Bytes, Self::Error>> + Send + 'a>>;
}
