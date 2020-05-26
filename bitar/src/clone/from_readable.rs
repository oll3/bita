use futures_util::stream::StreamExt;
use log::*;
use tokio::io::AsyncRead;

use crate::{chunker::Chunker, clone, Archive, ChunkIndex, HashSum};

#[derive(Debug)]
pub enum CloneFromReadableError<T> {
    TargetError(T),
    SourceError(std::io::Error),
    ThreadJoin(tokio::task::JoinError),
}
impl<T> std::error::Error for CloneFromReadableError<T> where T: std::error::Error {}
impl<T> std::fmt::Display for CloneFromReadableError<T>
where
    T: std::error::Error,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TargetError(err) => write!(f, "target error: {}", err),
            Self::SourceError(err) => write!(f, "source error: {}", err),
            Self::ThreadJoin(err) => write!(f, "error joining thread: {}", err),
        }
    }
}

/// Clone from any readable source.
pub async fn from_readable<I, C>(
    opts: &clone::Options,
    input: &mut I,
    archive: &Archive,
    chunks: &mut ChunkIndex,
    output: &mut C,
) -> Result<u64, CloneFromReadableError<C::Error>>
where
    I: AsyncRead + Unpin,
    C: clone::CloneOutput,
{
    let mut total_read = 0;
    let hash_length = archive.chunk_hash_length();
    let seed_chunker = Chunker::new(archive.chunker_config(), input);
    let mut found_chunks = seed_chunker
        .map(|result| {
            tokio::task::spawn_blocking(move || {
                result.map(|(_offset, chunk)| {
                    (HashSum::b2_digest(&chunk, hash_length as usize), chunk)
                })
            })
        })
        .buffered(opts.get_max_buffered_chunks())
        .map(|result| match result {
            Ok(Ok((hash, chunk))) => Ok((hash, chunk)),
            Ok(Err(err)) => Err(err),
            Err(err) => Err(err.into()),
        });
    if chunks.is_empty() {
        // Nothing to do
        return Ok(0);
    }
    while let Some(result) = found_chunks.next().await {
        if chunks.is_empty() {
            // Nothing more to do
            break;
        }
        let (hash, chunk) = result.map_err(CloneFromReadableError::SourceError)?;
        if !chunks.remove(&hash) {
            continue;
        }
        debug!("Chunk '{}', size {} used", hash, chunk.len());
        let offsets: Vec<u64> = archive
            .source_index()
            .offsets(&hash)
            .unwrap_or_else(|| panic!("missing chunk ({}) in source!?", hash))
            .collect();
        output
            .write_chunk(&hash, &offsets[..], &chunk)
            .await
            .map_err(CloneFromReadableError::TargetError)?;
        total_read += chunk.len() as u64;
    }
    Ok(total_read)
}
