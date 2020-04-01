use futures_util::future;
use futures_util::stream::StreamExt;
use log::*;
use tokio::io::AsyncRead;

use crate::Archive;
use crate::ChunkIndex;
use crate::Error;
use crate::HashSum;
use crate::Output;
use crate::{Chunker, ChunkerConfig};

pub struct Seed<'a, I> {
    input: I,
    chunker_config: &'a ChunkerConfig,
    num_chunk_buffers: usize,
}

impl<'a, I> Seed<'a, I> {
    pub fn new(input: I, chunker_config: &'a ChunkerConfig, num_chunk_buffers: usize) -> Self {
        Self {
            input,
            chunker_config,
            num_chunk_buffers,
        }
    }

    pub async fn seed(
        mut self,
        archive: &Archive,
        chunks_left: &mut ChunkIndex,
        output: &mut dyn Output,
    ) -> Result<u64, Error>
    where
        I: AsyncRead + Unpin,
    {
        let mut bytes_used = 0;
        let hash_length = archive.chunk_hash_length();
        let seed_chunker = Chunker::new(self.chunker_config, &mut self.input);
        let mut found_chunks = seed_chunker
            .map(|result| {
                tokio::task::spawn(async move {
                    result.map(|(_offset, chunk)| {
                        (HashSum::b2_digest(&chunk, hash_length as usize), chunk)
                    })
                })
            })
            .buffered(self.num_chunk_buffers)
            .filter_map(|result| {
                // Filter unique chunks to be compressed
                future::ready(match result {
                    Ok(Ok((hash, chunk))) => {
                        if chunks_left.remove(&hash) {
                            Some(Ok((hash, chunk)))
                        } else {
                            None
                        }
                    }
                    Ok(Err(err)) => Some(Err(err)),
                    Err(err) => Some(Err(err.into())),
                })
            });

        while let Some(result) = found_chunks.next().await {
            let (hash, chunk) = result?;
            debug!("Chunk '{}', size {} used", hash, chunk.len());
            let offsets: Vec<u64> = archive
                .source_index()
                .offsets(&hash)
                .unwrap_or_else(|| panic!("missing chunk ({}) in source!?", hash))
                .collect();
            output.write_chunk(&hash, &offsets[..], &chunk).await?;
            bytes_used += chunk.len() as u64 * offsets.len() as u64;
        }
        Ok(bytes_used)
    }
}
