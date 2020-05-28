use anyhow::Result;
use futures_util::stream::StreamExt;
use log::*;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use tokio::fs::File;

use crate::info_cmd;
use crate::string_utils::*;
use bitar::{chunker, Compression, HashSum};

#[derive(Clone, Debug)]
struct ChunkDescriptor {
    source_size: usize,
    compressed_size: Option<usize>,
    occurrences: Vec<u64>,
}

#[derive(Clone, Debug)]
struct ChunkerResult {
    chunks: HashSet<HashSum>,
    descriptors: HashMap<HashSum, ChunkDescriptor>,
    total_size: u64,
    total_compressed_size: u64,
    total_chunks: usize,
}

async fn chunk_file(
    path: &Path,
    chunker_config: &chunker::Config,
    compression: Compression,
    num_chunk_buffers: usize,
) -> Result<ChunkerResult> {
    let mut descriptors: HashMap<HashSum, ChunkDescriptor> = HashMap::new();
    let mut chunks = HashSet::new();
    let mut total_size = 0u64;
    let mut total_compressed_size = 0u64;
    let mut total_chunks = 0;
    {
        let mut file = File::open(path).await.expect("failed to open output file");
        let mut unique_chunk = HashSet::new();
        let chunker = chunker::Chunker::new(chunker_config, &mut file);
        let mut chunk_stream = chunker
            .map(|result| {
                let (offset, chunk) = result.expect("error while chunking");
                tokio::task::spawn_blocking(move || (HashSum::b2_digest(&chunk, 64), offset, chunk))
            })
            .buffered(num_chunk_buffers)
            .map(|result| {
                let (hash, offset, chunk) = result.expect("error while hashing chunk");
                if unique_chunk.contains(&hash) {
                    (hash, offset, chunk, false)
                } else {
                    unique_chunk.insert(hash.clone());
                    (hash, offset, chunk, true)
                }
            })
            .map(|(hash, offset, chunk, do_compress)| {
                tokio::task::spawn_blocking(move || {
                    if do_compress {
                        // Compress unique chunks
                        let compressed_chunk = compression
                            .compress(chunk.clone())
                            .expect("failed to compress chunk");
                        (hash, offset, chunk.len(), Some(compressed_chunk.len()))
                    } else {
                        (hash, offset, chunk.len(), None)
                    }
                })
            })
            .buffered(num_chunk_buffers);

        while let Some(result) = chunk_stream.next().await {
            let (hash, offset, chunk_size, compressed_size) =
                result.expect("error while compressing chunk");
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

fn print_info(path: &Path, result: &ChunkerResult, diff: &[HashSum]) {
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
    selection: &[HashSum],
    descriptors: &HashMap<HashSum, ChunkDescriptor>,
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

#[derive(Debug, Clone)]
pub struct Options {
    pub input_a: PathBuf,
    pub input_b: PathBuf,
    pub chunker_config: chunker::Config,
    pub compression: Compression,
    pub num_chunk_buffers: usize,
}

pub async fn diff_cmd(opts: Options) -> Result<()> {
    let chunker_config = &opts.chunker_config;
    let compression = opts.compression;

    info!("Chunker config:");
    info_cmd::print_chunker_config(chunker_config);
    println!();

    info!("Scanning {} ...", opts.input_a.display());
    let a = chunk_file(
        &opts.input_a,
        chunker_config,
        compression,
        opts.num_chunk_buffers,
    )
    .await?;

    info!("Scanning {} ...", opts.input_b.display());
    let b = chunk_file(
        &opts.input_b,
        chunker_config,
        compression,
        opts.num_chunk_buffers,
    )
    .await?;

    let mut descriptors_ab: HashMap<HashSum, ChunkDescriptor> = HashMap::new();
    for descriptor in a.descriptors.iter().chain(&b.descriptors) {
        if let Some(d) = descriptors_ab.get_mut(descriptor.0) {
            d.occurrences.append(&mut d.occurrences.clone());
        } else {
            descriptors_ab.insert(descriptor.0.clone(), descriptor.1.clone());
        }
    }

    let union_ab: Vec<HashSum> = a.chunks.union(&b.chunks).cloned().collect();
    let intersection_ab: Vec<HashSum> = a.chunks.intersection(&b.chunks).cloned().collect();
    let diff_ab: Vec<HashSum> = a.chunks.difference(&b.chunks).cloned().collect();
    let diff_ba: Vec<HashSum> = b.chunks.difference(&a.chunks).cloned().collect();

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
    print_info(&opts.input_a, &a, &diff_ab);
    println!();
    print_info(&opts.input_b, &b, &diff_ba);
    println!();

    Ok(())
}
