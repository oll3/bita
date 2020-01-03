use blake2::{Blake2b, Digest};
use futures_util::future;
use futures_util::stream::StreamExt;
use log::*;
use protobuf::{RepeatedField, SingularPtrField};
use std::collections::HashMap;
use std::io::Write;
use tokio::fs::{File, OpenOptions};
use tokio::prelude::*;

use crate::config::CompressConfig;
use crate::info_cmd;
use bita::archive;
use bita::chunk_dictionary;
use bita::chunker::{Chunker, ChunkerConfig};
use bita::compression::Compression;
use bita::error::Error;
use bita::string_utils::*;
use bita::HashSum;

#[derive(Debug, Clone)]
pub struct ChunkSourceDescriptor {
    pub unique_chunk_index: usize,
    pub offset: u64,
    pub size: usize,
    pub hash: HashSum,
}

pub const PKG_VERSION: &str = env!("CARGO_PKG_VERSION");

async fn chunk_input<T>(
    mut input: T,
    chunker_config: &ChunkerConfig,
    compression: Compression,
    temp_file_path: &std::path::Path,
    hash_length: usize,
) -> Result<
    (
        Vec<u8>,
        Vec<bita::chunk_dictionary::ChunkDescriptor>,
        u64,
        Vec<ChunkSourceDescriptor>,
    ),
    Error,
>
where
    T: AsyncRead + Unpin,
{
    let mut source_hasher = Blake2b::new();
    let mut unique_chunks = HashMap::new();
    let mut source_size: u64 = 0;
    let mut source_chunks = Vec::new();
    let mut archive_offset: u64 = 0;
    let mut unique_chunk_index: usize = 0;
    let mut archive_chunks = Vec::new();

    let mut temp_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(temp_file_path)
        .await
        .map_err(|e| ("failed to open temp file", e))?;
    {
        let chunker = Chunker::new(chunker_config, &mut input);
        let mut chunk_stream = chunker
            .map(|result| {
                let (offset, chunk) = result.expect("error while chunking");
                // Build hash of full source
                source_hasher.input(&chunk);
                source_size += chunk.len() as u64;
                tokio::task::spawn(async move {
                    // Calculate strong hash for each chunk
                    let mut chunk_hasher = Blake2b::new();
                    chunk_hasher.input(&chunk);
                    (
                        HashSum::from_slice(&chunk_hasher.result()[0..hash_length as usize]),
                        offset,
                        chunk,
                    )
                })
            })
            .buffered(8)
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
                {
                    // Also store a descriptor of each chunk
                    source_chunks.push(ChunkSourceDescriptor {
                        unique_chunk_index: chunk_index,
                        offset,
                        size: chunk.len(),
                        hash: hash.clone(),
                    });
                }
                if unique {
                    future::ready(Some((chunk_index, hash, offset, chunk)))
                } else {
                    future::ready(None)
                }
            })
            .map(|(chunk_index, hash, offset, chunk)| {
                tokio::task::spawn(async move {
                    // Compress each chunk
                    let compressed_chunk = compression
                        .compress(&chunk)
                        .expect("failed to compress chunk");
                    (chunk_index, hash, offset, chunk, compressed_chunk)
                })
            })
            .buffered(8);

        while let Some(result) = chunk_stream.next().await {
            let (index, hash, offset, chunk, compressed_chunk) =
                result.expect("error while compressing chunk");
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
            archive_chunks.push(chunk_dictionary::ChunkDescriptor {
                checksum: hash.to_vec(),
                source_size: chunk_len as u32,
                archive_offset,
                archive_size: use_data.len() as u32,
                unknown_fields: Default::default(),
                cached_size: Default::default(),
            });
            archive_offset += use_data.len() as u64;

            // Write the compressed chunk to temp file
            temp_file
                .write_all(&use_data)
                .await
                .map_err(|e| ("Failed to write to temp file", e))?;
        }
    }
    Ok((
        source_hasher.result().to_vec(),
        archive_chunks,
        source_size,
        source_chunks,
    ))
}

pub async fn run(config: CompressConfig) -> Result<(), Error> {
    let chunker_config = config.chunker_config.clone();
    let compression = config.compression;
    let mut output_file = std::fs::OpenOptions::new()
        .write(true)
        .read(true)
        .create(config.force_create)
        .truncate(config.force_create)
        .create_new(!config.force_create)
        .open(config.output.clone())
        .map_err(|e| ("failed to open output file", e))?;

    let (source_hash, archive_chunks, source_size, source_chunks) =
        if let Some(input_path) = config.input {
            chunk_input(
                File::open(input_path)
                    .await
                    .map_err(|err| ("failed to open input file", err))?,
                &chunker_config,
                compression,
                &config.temp_file,
                config.hash_length,
            )
            .await?
        } else if !atty::is(atty::Stream::Stdin) {
            // Read source from stdin
            chunk_input(
                tokio::io::stdin(),
                &chunker_config,
                compression,
                &config.temp_file,
                config.hash_length,
            )
            .await?
        } else {
            panic!("Missing input");
        };

    let chunker_params = match config.chunker_config {
        ChunkerConfig::BuzHash(hash_config) => chunk_dictionary::ChunkerParameters {
            chunk_filter_bits: hash_config.filter_bits.0,
            min_chunk_size: hash_config.min_chunk_size as u32,
            max_chunk_size: hash_config.max_chunk_size as u32,
            rolling_hash_window_size: hash_config.window_size as u32,
            chunk_hash_length: config.hash_length as u32,
            chunking_algorithm: chunk_dictionary::ChunkerParameters_ChunkingAlgorithm::BUZHASH,
            ..Default::default()
        },
        ChunkerConfig::RollSum(hash_config) => chunk_dictionary::ChunkerParameters {
            chunk_filter_bits: hash_config.filter_bits.0,
            min_chunk_size: hash_config.min_chunk_size as u32,
            max_chunk_size: hash_config.max_chunk_size as u32,
            rolling_hash_window_size: hash_config.window_size as u32,
            chunk_hash_length: config.hash_length as u32,
            chunking_algorithm: chunk_dictionary::ChunkerParameters_ChunkingAlgorithm::ROLLSUM,
            ..Default::default()
        },
        ChunkerConfig::FixedSize(chunk_size) => chunk_dictionary::ChunkerParameters {
            max_chunk_size: chunk_size as u32,
            chunk_hash_length: config.hash_length as u32,
            chunking_algorithm: chunk_dictionary::ChunkerParameters_ChunkingAlgorithm::FIXED_SIZE,
            ..Default::default()
        },
    };

    // Build the final archive
    let file_header = chunk_dictionary::ChunkDictionary {
        rebuild_order: source_chunks
            .iter()
            .map(|source_descriptor| source_descriptor.unique_chunk_index as u32)
            .collect(),
        application_version: PKG_VERSION.to_string(),
        chunk_descriptors: RepeatedField::from_vec(archive_chunks),
        source_checksum: source_hash,
        chunk_compression: SingularPtrField::some(config.compression.into()),
        source_total_size: source_size,
        chunker_params: SingularPtrField::some(chunker_params),
        unknown_fields: std::default::Default::default(),
        cached_size: std::default::Default::default(),
    };
    let header_buf = archive::build_header(&file_header, None)?;
    output_file
        .write_all(&header_buf)
        .map_err(|e| ("failed to write header", e))?;
    {
        let mut temp_file = std::fs::File::open(&config.temp_file)
            .map_err(|e| ("failed to open temporary file", e))?;
        std::io::copy(&mut temp_file, &mut output_file)
            .map_err(|e| ("failed to write chunk data to output file", e))?;
    }
    std::fs::remove_file(&config.temp_file).map_err(|e| ("unable to remove temporary file", e))?;
    drop(output_file);
    {
        // Print archive info
        let builder = bita::reader_backend::Builder::new_local(&config.output);
        info_cmd::print_archive_backend(builder).await?;
    }
    Ok(())
}
