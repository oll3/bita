use anyhow::{anyhow, Context, Result};
use blake2::{Blake2b, Digest};
use futures_util::future;
use futures_util::stream::StreamExt;
use log::*;
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use tokio::fs::{File, OpenOptions};
use tokio::prelude::*;

use crate::info_cmd;
use crate::string_utils::*;
use bitar::chunk_dictionary as dict;
use bitar::{chunker, Compression, HashSum};

pub const PKG_VERSION: &str = env!("CARGO_PKG_VERSION");

async fn chunk_input<T>(
    mut input: T,
    chunker_config: &chunker::Config,
    compression: Compression,
    temp_file_path: &std::path::Path,
    hash_length: usize,
    num_chunk_buffers: usize,
) -> Result<(
    Vec<u8>,
    Vec<bitar::chunk_dictionary::ChunkDescriptor>,
    u64,
    Vec<usize>,
)>
where
    T: AsyncRead + Unpin,
{
    let mut source_hasher = Blake2b::new();
    let mut unique_chunks = HashMap::new();
    let mut source_size: u64 = 0;
    let mut chunk_order = Vec::new();
    let mut archive_offset: u64 = 0;
    let mut unique_chunk_index: usize = 0;
    let mut archive_chunks = Vec::new();

    let mut temp_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(temp_file_path)
        .await
        .context(format!(
            "Failed to open temp file {}",
            temp_file_path.display()
        ))?;
    {
        let chunker = chunker::Chunker::new(chunker_config, &mut input);
        let mut chunk_stream = chunker
            .map(|result| {
                let (offset, chunk) = result.expect("error while chunking");
                // Build hash of full source
                source_hasher.update(&chunk);
                source_size += chunk.len() as u64;
                tokio::task::spawn_blocking(move || {
                    (
                        HashSum::b2_digest(&chunk, hash_length as usize),
                        offset,
                        chunk,
                    )
                })
            })
            .buffered(num_chunk_buffers)
            .filter_map(|result| {
                // Filter unique chunks to be compressed
                let (hash, offset, chunk) = result.expect("error while hashing chunk");
                let (unique, chunk_index) = if unique_chunks.contains_key(&hash) {
                    (false, *unique_chunks.get(&hash).unwrap())
                } else {
                    let chunk_index = unique_chunk_index;
                    unique_chunks.insert(hash.clone(), chunk_index);
                    unique_chunk_index += 1;
                    (true, chunk_index)
                };
                // Store a pointer (as index) to unique chunk index for each chunk
                chunk_order.push(chunk_index);
                future::ready(if unique {
                    Some((chunk_index, hash, offset, chunk))
                } else {
                    None
                })
            })
            .map(|(chunk_index, hash, offset, chunk)| {
                tokio::task::spawn_blocking(move || {
                    // Compress each chunk
                    let compressed_chunk = compression
                        .compress(chunk.clone())
                        .expect("failed to compress chunk");
                    (chunk_index, hash, offset, chunk, compressed_chunk)
                })
            })
            .buffered(num_chunk_buffers);

        while let Some(result) = chunk_stream.next().await {
            let (index, hash, offset, chunk, compressed_chunk) =
                result.context("Error while compressing")?;
            let chunk_len = chunk.len();
            let use_uncompressed_chunk = compressed_chunk.len() >= chunk_len;
            debug!(
                "Chunk {}, '{}', offset: {}, size: {}, {}",
                index,
                hash,
                offset,
                size_to_str(chunk_len),
                if use_uncompressed_chunk {
                    "left uncompressed".to_owned()
                } else {
                    format!("compressed to: {}", size_to_str(compressed_chunk.len()))
                },
            );
            let use_data = if use_uncompressed_chunk {
                chunk
            } else {
                compressed_chunk
            };

            // Store a chunk descriptor which refres to the compressed data
            archive_chunks.push(dict::ChunkDescriptor {
                checksum: hash.to_vec(),
                source_size: chunk_len as u32,
                archive_offset,
                archive_size: use_data.len() as u32,
            });
            archive_offset += use_data.len() as u64;

            // Write the compressed chunk to temp file
            temp_file
                .write_all(&use_data)
                .await
                .context("Failed to write to temp file")?;
        }
    }
    Ok((
        source_hasher.finalize().to_vec(),
        archive_chunks,
        source_size,
        chunk_order,
    ))
}

#[derive(Debug, Clone)]
pub struct Options {
    pub force_create: bool,

    // Use stdin if input not given
    pub input: Option<PathBuf>,
    pub output: PathBuf,
    pub temp_file: PathBuf,
    pub hash_length: usize,
    pub chunker_config: chunker::Config,
    pub compression: Compression,
    pub num_chunk_buffers: usize,
}
pub async fn compress_cmd(opts: Options) -> Result<()> {
    let chunker_config = opts.chunker_config.clone();
    let compression = opts.compression;
    let mut output_file = std::fs::OpenOptions::new()
        .write(true)
        .read(true)
        .create(opts.force_create)
        .truncate(opts.force_create)
        .create_new(!opts.force_create)
        .open(&opts.output)
        .context(format!(
            "Failed to open output file {}",
            opts.output.display()
        ))?;

    let (source_hash, archive_chunks, source_size, chunk_order) =
        if let Some(input_path) = opts.input {
            chunk_input(
                File::open(&input_path).await.context(format!(
                    "Failed to open input file {}",
                    input_path.display()
                ))?,
                &chunker_config,
                compression,
                &opts.temp_file,
                opts.hash_length,
                opts.num_chunk_buffers,
            )
            .await?
        } else if !atty::is(atty::Stream::Stdin) {
            // Read source from stdin
            chunk_input(
                tokio::io::stdin(),
                &chunker_config,
                compression,
                &opts.temp_file,
                opts.hash_length,
                opts.num_chunk_buffers,
            )
            .await?
        } else {
            return Err(anyhow!("Missing input"));
        };

    let chunker_params = match opts.chunker_config {
        chunker::Config::BuzHash(hash_config) => dict::ChunkerParameters {
            chunk_filter_bits: hash_config.filter_bits.bits(),
            min_chunk_size: hash_config.min_chunk_size as u32,
            max_chunk_size: hash_config.max_chunk_size as u32,
            rolling_hash_window_size: hash_config.window_size as u32,
            chunk_hash_length: opts.hash_length as u32,
            chunking_algorithm: dict::chunker_parameters::ChunkingAlgorithm::Buzhash as i32,
        },
        chunker::Config::RollSum(hash_config) => dict::ChunkerParameters {
            chunk_filter_bits: hash_config.filter_bits.bits(),
            min_chunk_size: hash_config.min_chunk_size as u32,
            max_chunk_size: hash_config.max_chunk_size as u32,
            rolling_hash_window_size: hash_config.window_size as u32,
            chunk_hash_length: opts.hash_length as u32,
            chunking_algorithm: dict::chunker_parameters::ChunkingAlgorithm::Rollsum as i32,
        },
        chunker::Config::FixedSize(chunk_size) => dict::ChunkerParameters {
            min_chunk_size: 0,
            chunk_filter_bits: 0,
            rolling_hash_window_size: 0,
            max_chunk_size: chunk_size as u32,
            chunk_hash_length: opts.hash_length as u32,
            chunking_algorithm: dict::chunker_parameters::ChunkingAlgorithm::FixedSize as i32,
        },
    };

    // Build the final archive
    let file_header = dict::ChunkDictionary {
        rebuild_order: chunk_order.iter().map(|&index| index as u32).collect(),
        application_version: PKG_VERSION.to_string(),
        chunk_descriptors: archive_chunks,
        source_checksum: source_hash,
        chunk_compression: Some(opts.compression.into()),
        source_total_size: source_size,
        chunker_params: Some(chunker_params),
    };
    let header_buf = bitar::header::build(&file_header, None)?;
    output_file.write_all(&header_buf).context(format!(
        "Failed to write header to output file {}",
        opts.output.display()
    ))?;
    {
        let mut temp_file = std::fs::File::open(&opts.temp_file).context(format!(
            "Failed to open temp file {}",
            opts.temp_file.display()
        ))?;
        std::io::copy(&mut temp_file, &mut output_file).context(format!(
            "Failed to copy from temp file {} to output file {}",
            opts.temp_file.display(),
            opts.output.display()
        ))?;
    }
    std::fs::remove_file(&opts.temp_file).context(format!(
        "Failed to remove temporary file {}",
        opts.temp_file.display()
    ))?;
    drop(output_file);
    {
        // Print archive info
        let mut reader = File::open(opts.output).await?;
        info_cmd::print_archive_reader(&mut reader).await?;
    }
    Ok(())
}
