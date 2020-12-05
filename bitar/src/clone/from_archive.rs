use blake2::{Blake2b, Digest};
use bytes::Bytes;
use futures_util::stream::StreamExt;
use log::*;

use crate::{clone, Archive, ChunkIndex, HashSum, Reader};

#[derive(Debug)]
pub enum CloneFromArchiveError<T, S> {
    TargetError(T),
    SourceError(S),
    ChecksumMismatch,
    CompressionError(crate::CompressionError),
    ThreadJoin(tokio::task::JoinError),
}
impl<S, T> std::error::Error for CloneFromArchiveError<S, T>
where
    S: std::error::Error,
    T: std::error::Error,
{
}
impl<S, T> std::fmt::Display for CloneFromArchiveError<S, T>
where
    S: std::error::Error,
    T: std::error::Error,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TargetError(err) => write!(f, "target error: {}", err),
            Self::SourceError(err) => write!(f, "source error: {}", err),
            Self::ChecksumMismatch => write!(f, "chunk checksum mismatch"),
            Self::CompressionError(err) => write!(f, "compression error: {}", err),
            Self::ThreadJoin(err) => write!(f, "error joining thread: {}", err),
        }
    }
}
impl<S, T> From<crate::CompressionError> for CloneFromArchiveError<S, T> {
    fn from(err: crate::CompressionError) -> Self {
        Self::CompressionError(err)
    }
}
impl<S, T> From<tokio::task::JoinError> for CloneFromArchiveError<S, T> {
    fn from(err: tokio::task::JoinError) -> Self {
        Self::ThreadJoin(err)
    }
}

/// Clone chunks from an archive.
pub async fn from_archive<R, C>(
    opts: &clone::Options,
    reader: &mut R,
    archive: &Archive,
    chunks: &mut ChunkIndex,
    output: &mut C,
) -> Result<u64, CloneFromArchiveError<C::Error, R::Error>>
where
    R: Reader,
    R::Error: Sync + Send + 'static,
    C: clone::CloneOutput,
    C::Error: Sync + Send + 'static,
{
    let mut total_fetched = 0u64;
    let mut adjacent_chunks = crate::archive::AdjacentChunks::new(
        archive
            .chunk_descriptors()
            .iter()
            .filter(|chunk| chunks.contains(&chunk.checksum)),
    );
    let mut chunk_sizes = Vec::new();
    while let Some(group) = adjacent_chunks.next() {
        // For each group of chunks
        let start_offset = group[0].archive_offset;
        let compression = archive.chunk_compression();
        chunk_sizes.clear();
        chunk_sizes.extend(group.iter().map(|c| c.archive_size as usize));
        let mut chunk_stream = reader
            .read_chunks(start_offset, &chunk_sizes[..])
            .enumerate()
            .map(|(index, read_result)| {
                let checksum = group[index].checksum.clone();
                let source_size = group[index].source_size as usize;
                if let Ok(chunk) = &read_result {
                    total_fetched += chunk.len() as u64;
                }
                tokio::task::spawn_blocking(move || {
                    let chunk = read_result.map_err(CloneFromArchiveError::SourceError)?;
                    let chunk = decompress_and_verify(compression, &checksum, source_size, chunk)?;
                    Ok::<_, CloneFromArchiveError<C::Error, R::Error>>((checksum, chunk))
                })
            })
            .buffered(opts.get_max_buffered_chunks());

        let mut offsets: Vec<u64> = Vec::with_capacity(32);
        while let Some(result) = chunk_stream.next().await {
            // For each chunk read from archive
            let result = result?;
            let (hash, chunk) = result?;
            offsets.clear();
            offsets.extend(
                chunks
                    .offsets(&hash)
                    .unwrap_or_else(|| panic!("missing chunk ({}) in source", hash)),
            );
            debug!("Chunk '{}', size {} used from archive", hash, chunk.len());
            output
                .write_chunk(&hash, &offsets[..], &chunk)
                .await
                .map_err(CloneFromArchiveError::TargetError)?;
        }
    }
    Ok(total_fetched)
}

fn decompress_and_verify<T, S>(
    compression: crate::Compression,
    archive_checksum: &HashSum,
    source_size: usize,
    compressed: Bytes,
) -> Result<Bytes, CloneFromArchiveError<T, S>> {
    let mut hasher = Blake2b::new();
    let chunk = if compressed.len() == source_size {
        // Archive data is not compressed
        compressed
    } else {
        compression.decompress(compressed, source_size)?
    };
    // Verify data by hash
    hasher.update(&chunk);
    let checksum = HashSum::from(&hasher.finalize()[..archive_checksum.len()]);
    if checksum != *archive_checksum {
        debug!(
            "chunk checksum mismatch (expected: {}, got: {})",
            checksum, archive_checksum,
        );
        Err(CloneFromArchiveError::ChecksumMismatch)
    } else {
        Ok(chunk)
    }
}
