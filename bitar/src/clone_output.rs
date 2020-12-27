use bytes::BytesMut;
use std::{
    collections::HashMap,
    io::{self, SeekFrom},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt};

use crate::{Chunk, ChunkIndex, HashSum, ReorderOp, VerifiedChunk};

pub struct CloneOutput<T> {
    pub(crate) inner: T,
    pub(crate) clone_index: ChunkIndex,
}

impl<T> CloneOutput<T> {
    pub fn new(output: T, clone_index: ChunkIndex) -> Self {
        Self {
            inner: output,
            clone_index,
        }
    }
    async fn write_offset(&mut self, offsets: &[u64], verified: &VerifiedChunk) -> io::Result<usize>
    where
        T: AsyncWrite + AsyncSeek + Unpin + Send,
    {
        let mut output_bytes = 0;
        for &offset in offsets {
            self.inner.seek(SeekFrom::Start(offset)).await?;
            self.inner.write_all(verified.data()).await?;
            output_bytes += verified.len();
        }
        Ok(output_bytes)
    }
    pub async fn feed(&mut self, verified: &VerifiedChunk) -> io::Result<usize>
    where
        T: AsyncWrite + AsyncSeek + Unpin + Send,
    {
        if let Some(location) = self.clone_index.remove(verified.hash()) {
            Ok(self.write_offset(location.offsets(), verified).await?)
        } else {
            Ok(0)
        }
    }
    pub fn chunks(&self) -> &ChunkIndex {
        &self.clone_index
    }
    pub fn len(&self) -> usize {
        self.clone_index.len()
    }
    pub fn is_empty(&self) -> bool {
        self.clone_index.is_empty()
    }
    pub fn into_inner(self) -> T {
        self.inner
    }
    /// Re-order chunks of output in place.
    pub async fn reorder_in_place(&mut self, output_index: ChunkIndex) -> io::Result<u64>
    where
        T: AsyncRead + AsyncWrite + AsyncSeek + Unpin + Send,
    {
        let mut total_moved: u64 = 0;
        let (already_in_place, in_place_total_size) =
            output_index.strip_chunks_already_in_place(&mut self.clone_index);
        log::debug!(
            "{} chunks ({}) are already in place in target",
            already_in_place,
            in_place_total_size
        );
        let reorder_ops = output_index.reorder_ops(&self.clone_index);
        let mut temp_store: HashMap<&HashSum, VerifiedChunk> = HashMap::new();
        let mut temp_buf = BytesMut::new();
        for op in reorder_ops {
            // Move chunks around internally in the output file
            match op {
                ReorderOp::Copy {
                    hash,
                    size,
                    source,
                    dest,
                } => {
                    if let Some(verified) = temp_store.remove(hash) {
                        self.write_offset(&dest[..], &verified).await?;
                    } else {
                        temp_buf.resize(size, 0);
                        self.inner.seek(SeekFrom::Start(source)).await?;
                        self.inner.read_exact(&mut temp_buf[..]).await?;
                        let verified = VerifiedChunk {
                            chunk: Chunk::from(temp_buf.clone().freeze()),
                            hash_sum: hash.clone(),
                        };
                        self.write_offset(&dest[..], &verified).await?;
                    };
                    total_moved += size as u64;
                    self.clone_index.remove(hash);
                }
                ReorderOp::StoreInMem { hash, size, source } => {
                    if !temp_store.contains_key(hash) {
                        let mut buf = BytesMut::new();
                        buf.resize(size, 0);
                        self.inner.seek(SeekFrom::Start(source)).await?;
                        self.inner.read_exact(&mut buf[..]).await?;
                        temp_store.insert(
                            hash,
                            VerifiedChunk {
                                chunk: Chunk::from(buf),
                                hash_sum: hash.clone(),
                            },
                        );
                    }
                }
            }
        }
        Ok(total_moved + in_place_total_size)
    }
}
