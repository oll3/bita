use blake2::{Blake2b, Digest};
use futures::future;
use log::*;
use protobuf::{RepeatedField, SingularPtrField};
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use tokio::fs::{File, OpenOptions};
use tokio::prelude::*;
use tokio::sync::oneshot;

use crate::config::CompressConfig;
use crate::info_cmd;
use crate::string_utils::*;
use bita::archive;
use bita::chunk_dictionary;
use bita::chunker::{Chunker, ChunkerParams};
use bita::error::Error;

#[derive(Debug, Clone)]
pub struct ChunkSourceDescriptor {
    pub unique_chunk_index: usize,
    pub offset: u64,
    pub size: usize,
    pub hash: archive::HashBuf,
}

pub const PKG_VERSION: &str = env!("CARGO_PKG_VERSION");

async fn create_chunker(
    chunker_params: ChunkerParams,
    input_path: Option<PathBuf>,
) -> Result<Chunker, Error> {
    if let Some(input_path) = input_path {
        // Read source from file
        let file = File::open(input_path)
            .await
            .map_err(|e| ("failed to open input file", e))?;
        Ok(Chunker::new(chunker_params, Box::new(file)))
    } else if !atty::is(atty::Stream::Stdin) {
        // Read source from stdin
        Ok(Chunker::new(chunker_params, Box::new(tokio::io::stdin())))
    } else {
        panic!("Bla");
    }
}

pub async fn run(config: CompressConfig) -> Result<(), Error> {
    let chunker_params = ChunkerParams::new(
        config.chunk_filter_bits,
        config.min_chunk_size,
        config.max_chunk_size,
        config.hash_window_size,
        archive::BUZHASH_SEED,
    );
    let compression = config.compression;
    let hash_length = config.hash_length;
    let mut unique_chunks = HashMap::new();
    let mut source_hasher = Blake2b::new();
    let mut source_size: u64 = 0;
    let mut source_chunks = Vec::new();
    let mut archive_offset: u64 = 0;
    let mut unique_chunk_index: usize = 0;
    let mut archive_chunks = Vec::new();

    let mut output_file = std::fs::OpenOptions::new()
        .write(true)
        .read(true)
        .create(config.force_create)
        .truncate(config.force_create)
        .create_new(!config.force_create)
        .open(config.output.clone())
        .map_err(|e| ("failed to open output file", e))?;

    let mut temp_file = OpenOptions::new()
        .write(true)
        .read(true)
        .create(true)
        .truncate(true)
        .open(config.temp_file.clone())
        .await
        .map_err(|e| ("failed to open temp file", e))?;

    {
        let chunker = create_chunker(chunker_params, config.input.clone()).await?;
        let mut chunk_stream = chunker
            .map(|result| {
                let (offset, chunk) = result.expect("error while chunking");
                // Build hash of full source
                source_hasher.input(&chunk);
                source_size += chunk.len() as u64;
                let (tx, rx) = oneshot::channel();
                tokio::spawn(async move {
                    // Calculate strong hash for each chunk
                    let mut chunk_hasher = Blake2b::new();
                    chunk_hasher.input(&chunk);
                    tx.send((
                        chunk_hasher.result()[0..hash_length as usize].to_vec(),
                        offset,
                        chunk,
                    ))
                    .unwrap();
                });
                rx.map(|v| v.expect("failed to receive"))
            })
            .buffered(8)
            .filter_map(|(hash, offset, chunk)| {
                // Filter unique chunks to be compressed
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
                        hash: hash.to_vec(),
                    });
                }
                if unique {
                    future::ready(Some((chunk_index, hash, offset, chunk)))
                } else {
                    future::ready(None)
                }
            })
            .map(|(chunk_index, hash, offset, chunk)| {
                let (tx, rx) = oneshot::channel();
                tokio::spawn(async move {
                    // Compress each chunk
                    let compressed_chunk = compression
                        .compress(&chunk)
                        .expect("failed to compress chunk");
                    tx.send((chunk_index, hash, offset, chunk, compressed_chunk))
                        .expect("failed to send");
                });
                rx.map(|v| v.expect("failed to receive"))
            })
            .buffered(8);

        while let Some((chunk_index, hash, offset, chunk, compressed_chunk)) =
            chunk_stream.next().await
        {
            let chunk_len = chunk.len();
            let use_uncompressed_chunk = compressed_chunk.len() >= chunk_len;
            debug!(
                "Chunk {}, '{}', offset: {}, size: {}, {}",
                chunk_index,
                HexSlice::new(&hash),
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
                checksum: hash.clone(),
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

    // Build the final archive
    let source_hash = source_hasher.result().to_vec();

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
        chunker_params: SingularPtrField::some(chunk_dictionary::ChunkerParameters {
            chunk_filter_bits: config.chunk_filter_bits,
            min_chunk_size: config.min_chunk_size as u32,
            max_chunk_size: config.max_chunk_size as u32,
            hash_window_size: config.hash_window_size as u32,
            chunk_hash_length: config.hash_length as u32,
            unknown_fields: std::default::Default::default(),
            cached_size: std::default::Default::default(),
        }),
        unknown_fields: std::default::Default::default(),
        cached_size: std::default::Default::default(),
    };
    let header_buf = archive::build_header(&file_header, None)?;
    output_file
        .write_all(&header_buf)
        .map_err(|e| ("failed to write header", e))?;
    drop(temp_file);
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
