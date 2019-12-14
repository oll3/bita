use blake2::{Blake2b, Digest};
use futures_util::future::FutureExt;
use futures_util::stream::StreamExt;
use log::*;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use tokio::fs::File;
use tokio::sync::oneshot;

use crate::config::DiffConfig;
use crate::info_cmd;
use bita::chunker::{Chunker, ChunkerConfig};
use bita::compression::Compression;
use bita::error::Error;
use bita::string_utils::*;

#[derive(Clone, Debug)]
struct ChunkDescriptor {
    source_size: usize,
    compressed_size: Option<usize>,
    occurrences: Vec<u64>,
}

#[derive(Clone, Debug)]
struct ChunkerResult {
    chunks: HashSet<Vec<u8>>,
    descriptors: HashMap<Vec<u8>, ChunkDescriptor>,
    total_size: u64,
    total_compressed_size: u64,
    total_chunks: usize,
}

async fn chunk_file(
    path: &Path,
    chunker_config: &ChunkerConfig,
    compression: Compression,
) -> Result<ChunkerResult, Error> {
    let mut descriptors: HashMap<Vec<u8>, ChunkDescriptor> = HashMap::new();
    let mut chunks = HashSet::new();
    let mut total_size = 0u64;
    let mut total_compressed_size = 0u64;
    let mut total_chunks = 0;
    {
        let mut file = File::open(path)
            .await
            .map_err(|e| ("failed to open output file", e))?;
        let mut unique_chunk = HashSet::new();
        let chunker = Chunker::new(chunker_config.clone(), &mut file);
        let mut chunk_stream = chunker
            .map(|result| {
                let (offset, chunk) = result.expect("error while chunking");
                let (tx, rx) = oneshot::channel();
                tokio::spawn(async move {
                    // Calculate strong hash for each chunk
                    let mut chunk_hasher = Blake2b::new();
                    chunk_hasher.input(&chunk);
                    tx.send((chunk_hasher.result().to_vec(), offset, chunk))
                        .expect("failed to send")
                });
                rx.map(|v| v.expect("failed to receive"))
            })
            .buffered(8)
            .map(|(hash, offset, chunk)| {
                if unique_chunk.contains(&hash) {
                    (hash, offset, chunk, false)
                } else {
                    unique_chunk.insert(hash.clone());
                    (hash, offset, chunk, true)
                }
            })
            .map(|(hash, offset, chunk, do_compress)| {
                let (tx, rx) = oneshot::channel();
                tokio::spawn(async move {
                    if do_compress {
                        // Compress unique chunks
                        let compressed_chunk = compression
                            .compress(&chunk)
                            .expect("failed to compress chunk");
                        tx.send((hash, offset, chunk.len(), Some(compressed_chunk.len())))
                            .expect("failed to send");
                    } else {
                        tx.send((hash, offset, chunk.len(), None))
                            .expect("failed to send");
                    }
                });
                rx.map(|v| v.expect("failed to receive"))
            })
            .buffered(8);

        while let Some((hash, offset, chunk_size, compressed_size)) = chunk_stream.next().await {
            total_chunks += 1;
            total_size += chunk_size as u64;
            chunks.insert(hash.clone());
            if let Some(descriptor) = descriptors.get_mut(&hash) {
                descriptor.occurrences.push(offset);
                if let Some(compressed_size) = compressed_size {
                    descriptor.compressed_size = Some(compressed_size);
                }
                total_compressed_size += descriptor.compressed_size.unwrap_or(0) as u64;
            } else {
                total_compressed_size += compressed_size.unwrap_or(0) as u64;
                descriptors.insert(
                    hash,
                    ChunkDescriptor {
                        source_size: chunk_size,
                        compressed_size,
                        occurrences: vec![offset],
                    },
                );
            }
        }
    }

    Ok(ChunkerResult {
        chunks,
        descriptors,
        total_size,
        total_compressed_size,
        total_chunks,
    })
}

fn print_info(path: &Path, result: &ChunkerResult, diff: &[Vec<u8>]) {
    let avarage_chunk_size: u64 = result
        .descriptors
        .iter()
        .map(|d| d.1.source_size as u64)
        .sum::<u64>()
        / result.descriptors.len() as u64;
    info!("{}:", path.display());
    info!(
        "  Chunks: {} (unique {})",
        result.total_chunks,
        result.descriptors.len(),
    );
    info!("  Average chunk size: {}", size_to_str(avarage_chunk_size));
    info!(
        "  Total size: {} (compressed size: {})",
        size_to_str(result.total_size),
        size_to_str(result.total_compressed_size)
    );
    info!(
        "  Chunks not in other: {}",
        selection_string(diff, &result.descriptors)
    );
}

fn selection_string(
    selection: &[Vec<u8>],
    descriptors: &HashMap<Vec<u8>, ChunkDescriptor>,
) -> String {
    let mut size = 0u64;
    let mut compressed_size = 0u64;
    for hash in selection {
        let d = descriptors.get(hash).unwrap();
        size += (d.source_size * d.occurrences.len()) as u64;
        compressed_size +=
            (d.compressed_size.unwrap_or(d.source_size) * d.occurrences.len()) as u64;
    }
    format!(
        "{} (size: {}, compressed size: {})",
        selection.len(),
        size_to_str(size),
        size_to_str(compressed_size),
    )
}

pub async fn run(config: DiffConfig) -> Result<(), Error> {
    let chunker_config = &config.chunker_config;
    let compression = config.compression;

    info!("Chunker config:");
    info_cmd::print_chunker_config(chunker_config);
    println!();

    info!("Scanning {} ...", config.input_a.display());
    let a = chunk_file(&config.input_a, chunker_config, compression).await?;

    info!("Scanning {} ...", config.input_b.display());
    let b = chunk_file(&config.input_b, chunker_config, compression).await?;

    let mut descriptors_ab: HashMap<Vec<u8>, ChunkDescriptor> = HashMap::new();
    for descriptor in a.descriptors.iter().chain(&b.descriptors) {
        if let Some(d) = descriptors_ab.get_mut(descriptor.0) {
            d.occurrences.append(&mut d.occurrences.clone());
        } else {
            descriptors_ab.insert(descriptor.0.clone(), descriptor.1.clone());
        }
    }

    let union_ab: Vec<Vec<u8>> = a.chunks.union(&b.chunks).cloned().collect();
    let intersection_ab: Vec<Vec<u8>> = a.chunks.intersection(&b.chunks).cloned().collect();
    let diff_ab: Vec<Vec<u8>> = a.chunks.difference(&b.chunks).cloned().collect();
    let diff_ba: Vec<Vec<u8>> = b.chunks.difference(&a.chunks).cloned().collect();

    println!();
    info!(
        "Total unique chunks: {}",
        selection_string(&union_ab, &descriptors_ab)
    );
    info!(
        "Chunks shared: {}",
        selection_string(&intersection_ab, &descriptors_ab)
    );

    println!();
    print_info(&config.input_a, &a, &diff_ab);
    println!();
    print_info(&config.input_b, &b, &diff_ba);
    println!();

    Ok(())
}
