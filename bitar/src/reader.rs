use async_trait::async_trait;
use bytes::Bytes;
use core::pin::Pin;
use futures_util::stream::Stream;

#[derive(Debug, Clone, PartialEq)]
pub struct ReadAt {
    pub offset: u64,
    pub size: usize,
}

impl ReadAt {
    pub fn new(offset: u64, size: usize) -> Self {
        Self { offset, size }
    }
    pub fn end_offset(&self) -> u64 {
        self.offset + self.size as u64
    }
}

/// Read bytes at offset.
#[async_trait]
pub trait Reader {
    type Error;
    async fn read_at<'a>(&'a mut self, offset: u64, size: usize) -> Result<Bytes, Self::Error>;
    fn read_chunks<'a>(
        &'a mut self,
        chunks: Vec<ReadAt>,
    ) -> Pin<Box<dyn Stream<Item = Result<Bytes, Self::Error>> + Send + 'a>>;
}
