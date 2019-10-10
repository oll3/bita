use crate::string_utils::*;
use atty::Stream;
use blake2::{Blake2b, Digest};
use log::*;
use protobuf::{RepeatedField, SingularPtrField};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Seek, SeekFrom, Write};
use threadpool::ThreadPool;

use crate::config::CompressConfig;
use crate::info_cmd;
use bita::archive;
use bita::chunk_dictionary;
use bita::chunker::{Chunker, ChunkerParams};
use bita::chunker_utils::*;
use bita::error::Error;

pub const PKG_VERSION: &str = env!("CARGO_PKG_VERSION");

struct ChunkFileDescriptor {
    total_file_size: usize,
    file_hash: HashBuf,
    chunk_descriptors: Vec<chunk_dictionary::ChunkDescriptor>,
    chunk_order: Vec<ChunkSourceDescriptor>,
}

fn chunk_into_file(
    config: &CompressConfig,
    pool: &ThreadPool,
    chunk_file: &mut File,
) -> Result<ChunkFileDescriptor, Error> {
    // Setup the chunker
    let chunker_params = ChunkerParams::new(
        config.chunk_filter_bits,
        config.min_chunk_size,
        config.max_chunk_size,
        config.hash_window_size,
        archive::BUZHASH_SEED,
    );

    // Compress a chunk
    let compression = config.compression;
    let chunk_compressor =
        move |data: &[u8]| -> Vec<u8> { compression.compress(data).expect("compress data") };

    // Generate strong hash for a chunk
    fn hasher(data: &[u8]) -> Vec<u8> {
        let mut h = Blake2b::new();
        h.input(data);
        h.result().to_vec()
    };

    let mut total_compressed_size = 0;
    let mut total_unique_chunks = 0;
    let mut total_unique_chunk_size = 0;
    let mut archive_offset: u64 = 0;
    let mut chunk_descriptors = Vec::new();
    let chunk_order;
    let total_file_size;
    let file_hash;
    {
        let process_chunk = |comp_chunk: CompressedChunk| {
            // For each unique and compressed chunk
            let hash = &comp_chunk.hash[0..config.hash_length as usize];

            let store_data = if comp_chunk.cdata.len() >= comp_chunk.data.len() {
                &comp_chunk.data
            } else {
                &comp_chunk.cdata
            };

            debug!(
                "Chunk {}, '{}', offset: {}, size: {}, {}",
                total_unique_chunks,
                HexSlice::new(&hash),
                comp_chunk.offset,
                size_to_str(comp_chunk.data.len()),
                if comp_chunk.cdata.len() >= comp_chunk.data.len() {
                    "left uncompressed".to_owned()
                } else {
                    format!("compressed to: {}", size_to_str(store_data.len()))
                },
            );

            total_unique_chunks += 1;
            total_unique_chunk_size += comp_chunk.data.len();
            total_compressed_size += store_data.len();

            // Store a chunk descriptor which referes to the compressed data
            chunk_descriptors.push(chunk_dictionary::ChunkDescriptor {
                checksum: hash.to_vec(),
                source_size: comp_chunk.data.len() as u32,
                archive_offset,
                archive_size: store_data.len() as u32,
                unknown_fields: std::default::Default::default(),
                cached_size: std::default::Default::default(),
            });

            chunk_file.write_all(store_data).expect("write chunk");
            archive_offset += store_data.len() as u64;
        };

        if let Some(ref input_path) = config.input {
            // Read source from file
            let mut src_file = File::open(&input_path).map_err(|e| {
                (
                    format!("unable to open input file ({})", input_path.display()),
                    e,
                )
            })?;
            let mut chunker = Chunker::new(chunker_params.clone(), &mut src_file);
            let (tmp_file_size, tmp_file_hash, tmp_chunks) = unique_compressed_chunks(
                &mut chunker,
                hasher,
                chunk_compressor,
                &pool,
                true,
                process_chunk,
            )?;
            total_file_size = tmp_file_size;
            file_hash = tmp_file_hash;
            chunk_order = tmp_chunks;
        } else if !atty::is(Stream::Stdin) {
            // Read source from stdin
            let stdin = io::stdin();
            let mut src_file = stdin.lock();
            let mut chunker = Chunker::new(chunker_params.clone(), &mut src_file);
            let (tmp_file_size, tmp_file_hash, tmp_chunks) = unique_compressed_chunks(
                &mut chunker,
                hasher,
                chunk_compressor,
                &pool,
                true,
                process_chunk,
            )?;
            total_file_size = tmp_file_size;
            file_hash = tmp_file_hash;
            chunk_order = tmp_chunks;
        } else {
            panic!("Missing input file")
        }
    }
    pool.join();

    Ok(ChunkFileDescriptor {
        total_file_size,
        file_hash,
        chunk_descriptors,
        chunk_order,
    })
}

pub fn run(config: &CompressConfig) -> Result<(), Error> {
    let pool = ThreadPool::new(num_cpus::get());
    let mut output_file = OpenOptions::new()
        .write(true)
        .read(true)
        .create(config.force_create)
        .truncate(config.force_create)
        .create_new(!config.force_create)
        .open(&config.output)
        .map_err(|e| {
            (
                format!("unable to create output file ({})", config.output.display()),
                e,
            )
        })?;

    let mut tmp_chunk_file = OpenOptions::new()
        .write(true)
        .read(true)
        .truncate(true)
        .create(true)
        .open(&config.temp_file)
        .map_err(|e| {
            (
                format!(
                    "unable to create temporary chunk file ({})",
                    config.temp_file.display(),
                ),
                e,
            )
        })?;

    // Generate chunks and store to a temp file
    let chunk_file_descriptor = chunk_into_file(&config, &pool, &mut tmp_chunk_file)?;

    // Store header to output file
    let file_header = chunk_dictionary::ChunkDictionary {
        rebuild_order: chunk_file_descriptor
            .chunk_order
            .iter()
            .map(|source_descriptor| source_descriptor.unique_chunk_index as u32)
            .collect(),
        application_version: PKG_VERSION.to_string(),
        chunk_descriptors: RepeatedField::from_vec(chunk_file_descriptor.chunk_descriptors),
        source_checksum: chunk_file_descriptor.file_hash,
        chunk_compression: SingularPtrField::some(config.compression.into()),
        source_total_size: chunk_file_descriptor.total_file_size as u64,
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

    // Copy chunks from temporary chunk tile to the output one
    let header_buf = archive::build_header(&file_header, None)?;

    output_file
        .write_all(&header_buf)
        .map_err(|e| ("failed to write header", e))?;
    tmp_chunk_file
        .seek(SeekFrom::Start(0))
        .map_err(|e| ("failed to seek", e))?;
    io::copy(&mut tmp_chunk_file, &mut output_file)
        .map_err(|e| ("failed to write chunk data to output file", e))?;
    drop(tmp_chunk_file);
    fs::remove_file(&config.temp_file).map_err(|e| ("unable to remove temporary file", e))?;

    output_file
        .seek(SeekFrom::Start(0))
        .map_err(|e| ("failed to seek", e))?;

    info!("Created archive {}", config.output.display());
    info_cmd::print_archive_backend(output_file)?;

    Ok(())
}
