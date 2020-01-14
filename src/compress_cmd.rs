use blake2::{Blake2b, Digest};
use futures_util::future;
use futures_util::stream::StreamExt;
use log::*;
use protobuf::{RepeatedField, SingularPtrField};
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use tokio::fs::{File, OpenOptions};
use tokio::prelude::*;

use crate::info_cmd;
use bita::archive;
use bita::chunk_dictionary;
use bita::chunker::{Chunker, ChunkerConfig};
use bita::compression::Compression;
use bita::error::Error;
use bita::string_utils::*;
use bita::HashSum;

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
        Vec<usize>,
    ),
    Error,
>
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
                    (
                        HashSum::b2_digest(&chunk, hash_length as usize),
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
                // Store a pointer (as index) to unique chunk index for each chunk
                chunk_order.push(chunk_index);

                future::ready(if unique {
                    Some((chunk_index, hash, offset, chunk))
                } else {
                    None
                })
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
        chunk_order,
    ))
}

#[derive(Debug, Clone)]
pub struct Command {
    pub force_create: bool,

    // Use stdin if input not given
    pub input: Option<PathBuf>,
    pub output: PathBuf,
    pub temp_file: PathBuf,
    pub hash_length: usize,
    pub chunker_config: ChunkerConfig,
    pub compression_level: u32,
    pub compression: Compression,
}
impl Command {
    pub async fn run(self) -> Result<(), Error> {
        let chunker_config = self.chunker_config.clone();
        let compression = self.compression;
        let mut output_file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(self.force_create)
            .truncate(self.force_create)
            .create_new(!self.force_create)
            .open(self.output.clone())
            .map_err(|e| ("failed to open output file", e))?;

        let (source_hash, archive_chunks, source_size, chunk_order) =
            if let Some(input_path) = self.input {
                chunk_input(
                    File::open(input_path)
                        .await
                        .map_err(|err| ("failed to open input file", err))?,
                    &chunker_config,
                    compression,
                    &self.temp_file,
                    self.hash_length,
                )
                .await?
            } else if !atty::is(atty::Stream::Stdin) {
                // Read source from stdin
                chunk_input(
                    tokio::io::stdin(),
                    &chunker_config,
                    compression,
                    &self.temp_file,
                    self.hash_length,
                )
                .await?
            } else {
                panic!("Missing input");
            };

        let chunker_params = match self.chunker_config {
            ChunkerConfig::BuzHash(hash_config) => chunk_dictionary::ChunkerParameters {
                chunk_filter_bits: hash_config.filter_bits.0,
                min_chunk_size: hash_config.min_chunk_size as u32,
                max_chunk_size: hash_config.max_chunk_size as u32,
                rolling_hash_window_size: hash_config.window_size as u32,
                chunk_hash_length: self.hash_length as u32,
                chunking_algorithm: chunk_dictionary::ChunkerParameters_ChunkingAlgorithm::BUZHASH,
                ..Default::default()
            },
            ChunkerConfig::RollSum(hash_config) => chunk_dictionary::ChunkerParameters {
                chunk_filter_bits: hash_config.filter_bits.0,
                min_chunk_size: hash_config.min_chunk_size as u32,
                max_chunk_size: hash_config.max_chunk_size as u32,
                rolling_hash_window_size: hash_config.window_size as u32,
                chunk_hash_length: self.hash_length as u32,
                chunking_algorithm: chunk_dictionary::ChunkerParameters_ChunkingAlgorithm::ROLLSUM,
                ..Default::default()
            },
            ChunkerConfig::FixedSize(chunk_size) => chunk_dictionary::ChunkerParameters {
                max_chunk_size: chunk_size as u32,
                chunk_hash_length: self.hash_length as u32,
                chunking_algorithm:
                    chunk_dictionary::ChunkerParameters_ChunkingAlgorithm::FIXED_SIZE,
                ..Default::default()
            },
        };

        // Build the final archive
        let file_header = chunk_dictionary::ChunkDictionary {
            rebuild_order: chunk_order.iter().map(|&index| index as u32).collect(),
            application_version: PKG_VERSION.to_string(),
            chunk_descriptors: RepeatedField::from_vec(archive_chunks),
            source_checksum: source_hash,
            chunk_compression: SingularPtrField::some(self.compression.into()),
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
            let mut temp_file = std::fs::File::open(&self.temp_file)
                .map_err(|e| ("failed to open temporary file", e))?;
            std::io::copy(&mut temp_file, &mut output_file)
                .map_err(|e| ("failed to write chunk data to output file", e))?;
        }
        std::fs::remove_file(&self.temp_file)
            .map_err(|e| ("unable to remove temporary file", e))?;
        drop(output_file);
        {
            // Print archive info
            let builder = bita::reader_backend::Builder::new_local(&self.output);
            info_cmd::print_archive_backend(builder).await?;
        }
        Ok(())
    }
}
