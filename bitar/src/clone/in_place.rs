use log::*;
use std::collections::HashMap;
use std::io::SeekFrom;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite};

use crate::{clone, clone::CloneOutput, Archive, ChunkIndex, HashSum, ReorderOp};

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
    let target_index = ChunkIndex::try_from_readable(
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
    for op in reorder_ops {
        // Move chunks around internally in the output file
        match op {
            ReorderOp::Copy {
                hash,
                size,
                source,
                dest,
            } => {
                let buf = if let Some(buf) = temp_store.remove(hash) {
                    buf
                } else {
                    let mut buf: Vec<u8> = Vec::new();
                    buf.resize(size, 0);
                    target
                        .seek(SeekFrom::Start(source))
                        .await
                        .map_err(CloneInPlaceError::IO)?;
                    target
                        .read_exact(&mut buf[..])
                        .await
                        .map_err(CloneInPlaceError::IO)?;
                    buf
                };
                target
                    .write_chunk(hash, &dest[..], &buf[..])
                    .await
                    .map_err(CloneInPlaceError::IO)?;
                total_moved += size as u64;
                chunks.remove(hash);
            }
            ReorderOp::StoreInMem { hash, size, source } => {
                if !temp_store.contains_key(hash) {
                    let mut buf: Vec<u8> = Vec::new();
                    buf.resize(size, 0);
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
