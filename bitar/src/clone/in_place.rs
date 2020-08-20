use futures_util::stream::StreamExt;
use log::*;
use std::collections::HashMap;
use std::io::SeekFrom;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite};

use crate::{chunker, clone, clone::CloneOutput, Archive, ChunkIndex, HashSum, ReorderOp};

#[derive(Debug)]
pub enum CloneInPlaceError {
    IO(std::io::Error),
}
impl std::error::Error for CloneInPlaceError {}
impl std::fmt::Display for CloneInPlaceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IO(err) => write!(f, "i/o error: {}", err),
        }
    }
}

/// Clone by re-ordering chunks of a target in-place.
///
/// Can be used for any target which is Read, Write and Seek:able.
pub async fn in_place<T>(
    opts: &clone::Options,
    chunks: &mut ChunkIndex,
    archive: &Archive,
    target: &mut T,
) -> Result<u64, CloneInPlaceError>
where
    T: AsyncRead + AsyncWrite + AsyncSeek + Unpin + Send,
{
    let mut total_moved: u64 = 0;
    target
        .seek(SeekFrom::Start(0))
        .await
        .map_err(CloneInPlaceError::IO)?;
    let target_index = chunk_index_from_readable(
        &archive.chunker_config(),
        archive.chunk_hash_length(),
        opts.get_max_buffered_chunks(),
        target,
    )
    .await
    .map_err(CloneInPlaceError::IO)?;

    let (already_in_place, in_place_total_size) =
        target_index.strip_chunks_already_in_place(chunks);
    debug!(
        "{} chunks ({}) are already in place in target",
        already_in_place, in_place_total_size
    );

    let reorder_ops = target_index.reorder_ops(chunks);
    let mut temp_store: HashMap<&HashSum, Vec<u8>> = HashMap::new();
    let mut temp_buf = Vec::new();
    for op in reorder_ops {
        // Move chunks around internally in the output file
        match op {
            ReorderOp::Copy {
                hash,
                size,
                source,
                dest,
            } => {
                if let Some(buf) = temp_store.remove(hash) {
                    target
                        .write_chunk(hash, &dest[..], &buf[..])
                        .await
                        .map_err(CloneInPlaceError::IO)?;
                } else {
                    temp_buf.resize(size, 0);
                    target
                        .seek(SeekFrom::Start(source))
                        .await
                        .map_err(CloneInPlaceError::IO)?;
                    target
                        .read_exact(&mut temp_buf[..])
                        .await
                        .map_err(CloneInPlaceError::IO)?;
                    target
                        .write_chunk(hash, &dest[..], &temp_buf[..])
                        .await
                        .map_err(CloneInPlaceError::IO)?;
                };
                total_moved += size as u64;
                chunks.remove(hash);
            }
            ReorderOp::StoreInMem { hash, size, source } => {
                if !temp_store.contains_key(hash) {
                    let mut buf: Vec<u8> = vec![0; size];
                    target
                        .seek(SeekFrom::Start(source))
                        .await
                        .map_err(CloneInPlaceError::IO)?;
                    target
                        .read_exact(&mut buf[..])
                        .await
                        .map_err(CloneInPlaceError::IO)?;
                    temp_store.insert(hash, buf);
                }
            }
        }
    }
    Ok(total_moved + in_place_total_size)
}

async fn chunk_index_from_readable<T>(
    chunker_config: &chunker::Config,
    hash_length: usize,
    max_buffered_chunks: usize,
    readable: &mut T,
) -> Result<ChunkIndex, std::io::Error>
where
    T: AsyncRead + Unpin,
{
    let chunker = chunker::Chunker::new(chunker_config, readable);
    let mut chunk_stream = chunker
        .map(|result| {
            tokio::task::spawn_blocking(move || {
                result.map(|(offset, chunk)| {
                    (
                        HashSum::b2_digest(&chunk, hash_length as usize),
                        chunk.len(),
                        offset,
                    )
                })
            })
        })
        .buffered(max_buffered_chunks);
    let mut ci = ChunkIndex::new_empty();
    while let Some(result) = chunk_stream.next().await {
        let (chunk_hash, chunk_size, chunk_offset) = result.unwrap()?;
        ci.add_chunk(chunk_hash, chunk_size, &[chunk_offset]);
    }
    Ok(ci)
}
