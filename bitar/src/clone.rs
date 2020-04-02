use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use log::*;
use std::collections::HashMap;

use crate::{Archive, ChunkIndex, CloneOutput, Error, HashSum, Reader, ReorderOp};

/// Clone by moving data in output in-place
pub async fn clone_in_place(
    output: &mut dyn CloneOutput,
    output_index: &ChunkIndex,
    chunks_left: &mut ChunkIndex,
) -> Result<u64, Error> {
    let mut total_read: u64 = 0;
    let reorder_ops = output_index.reorder_ops(chunks_left);

    // Move chunks around internally in the output file
    let mut temp_store: HashMap<&HashSum, Vec<u8>> = HashMap::new();
    for op in &reorder_ops {
        match op {
            ReorderOp::Copy { hash, source, dest } => {
                let buf = if let Some(buf) = temp_store.remove(hash) {
                    buf
                } else {
                    let mut buf: Vec<u8> = Vec::new();
                    buf.resize(source.size, 0);
                    output.seek_read(source.offset, &mut buf[..]).await?;
                    buf
                };
                output.write_chunk(hash, &dest[..], &buf[..]).await?;
                total_read += source.size as u64 * dest.len() as u64;
                chunks_left.remove(hash);
            }
            ReorderOp::StoreInMem { hash, source } => {
                if !temp_store.contains_key(hash) {
                    let mut buf: Vec<u8> = Vec::new();
                    buf.resize(source.size, 0);
                    output.seek_read(source.offset, &mut buf[..]).await?;
                    temp_store.insert(hash, buf);
                }
            }
        }
    }
    Ok(total_read)
}

/// Clone using data from archive
pub async fn clone_using_archive(
    reader: &mut dyn Reader,
    archive: &Archive,
    chunks_left: ChunkIndex,
    output: &mut dyn CloneOutput,
    num_chunk_buffers: usize,
) -> Result<u64, Error> {
    let mut total_written = 0u64;
    let mut total_fetched = 0u64;
    let grouped_chunks = archive.grouped_chunks(&chunks_left);
    for group in grouped_chunks {
        // For each group of chunks
        let start_offset = archive.chunk_data_offset() + group[0].archive_offset;
        let compression = archive.chunk_compression();
        let archive_chunk_stream = reader
            .read_chunks(
                start_offset,
                group.iter().map(|c| c.archive_size as usize).collect(),
            )
            .enumerate()
            .map(|(chunk_index, compressed_chunk)| {
                let compressed_chunk = compressed_chunk.expect("failed to read archive");
                let chunk_checksum = group[chunk_index].checksum.clone();
                let chunk_source_size = group[chunk_index].source_size as usize;
                total_fetched += compressed_chunk.len() as u64;
                tokio::task::spawn(async move {
                    (
                        chunk_checksum.clone(),
                        Archive::decompress_and_verify(
                            compression,
                            &chunk_checksum,
                            chunk_source_size,
                            compressed_chunk,
                        )
                        .expect("failed to decompress chunk"),
                    )
                })
            })
            .buffered(num_chunk_buffers);

        pin_mut!(archive_chunk_stream);
        while let Some(result) = archive_chunk_stream.next().await {
            // For each chunk read from archive
            let (hash, chunk) = result.expect("spawn error");
            let offsets: Vec<u64> = archive
                .source_index()
                .offsets(&hash)
                .unwrap_or_else(|| panic!("missing chunk ({}) in source", hash))
                .collect();
            debug!("Chunk '{}', size {} used from archive", hash, chunk.len(),);

            output
                .write_chunk(&hash, &offsets[..], &chunk)
                .await
                .expect("failed to write output");
            total_written += chunk.len() as u64 * offsets.len() as u64;
        }
    }
    Ok(total_written)
}
