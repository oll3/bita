use async_trait::async_trait;
use std::io::SeekFrom;
use tokio::io::{AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt};

use crate::HashSum;

/// Output while cloning.
#[async_trait]
pub trait CloneOutput {
    type Error: std::error::Error + Send + Sync;
    /// Write a single chunk to output at the given offsets.
    async fn write_chunk(
        &mut self,
        hash: &HashSum,
        offsets: &[u64],
        buf: &[u8],
    ) -> Result<(), Self::Error>;
}

#[async_trait]
impl<T> CloneOutput for T
where
    T: AsyncWrite + AsyncSeek + Unpin + Send,
{
    type Error = std::io::Error;
    async fn write_chunk(
        &mut self,
        _: &HashSum,
        offsets: &[u64],
        buf: &[u8],
    ) -> Result<(), std::io::Error> {
        for &offset in offsets {
            self.seek(SeekFrom::Start(offset)).await?;
            self.write_all(buf).await?;
        }
        Ok(())
    }
}
