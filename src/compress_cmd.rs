use crate::string_utils::*;
use atty::Stream;
use blake2::{Blake2b, Digest};
use protobuf::{RepeatedField, SingularPtrField};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Seek, SeekFrom, Write};
use threadpool::ThreadPool;

use crate::archive;
use crate::buzhash::BuzHash;
use crate::chunk_dictionary;
use crate::chunker::*;
use crate::chunker_utils::*;
use crate::compression::Compression;
use crate::config::CompressConfig;
use crate::errors::*;

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
) -> Result<ChunkFileDescriptor> {
    // Setup the chunker
    let chunker_params = ChunkerParams::new(
        config.chunk_filter_bits,
        config.min_chunk_size,
        config.max_chunk_size,
        BuzHash::new(config.hash_window_size as usize, crate::BUZHASH_SEED),
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
            let chunk_data;
            let hash = &comp_chunk.hash[0..config.hash_length as usize];
            let use_compression = if comp_chunk.cdata.len() < comp_chunk.data.len() {
                // Use the compressed data
                chunk_data = &comp_chunk.cdata;
                config.compression
            } else {
                // Compressed chunk bigger than raw - Use raw
                chunk_data = &comp_chunk.data;
                Compression::None
            };

            println!(
                "Chunk {}, '{}', offset: {}, size: {}, compressed to: {}, compression: {}",
                total_unique_chunks,
                HexSlice::new(&hash),
                comp_chunk.offset,
                size_to_str(comp_chunk.data.len()),
                size_to_str(comp_chunk.cdata.len()),
                use_compression
            );

            total_unique_chunks += 1;
            total_unique_chunk_size += comp_chunk.data.len();
            total_compressed_size += chunk_data.len();

            // Store a chunk descriptor which referes to the compressed data
            chunk_descriptors.push(chunk_dictionary::ChunkDescriptor {
                checksum: hash.to_vec(),
                source_size: comp_chunk.data.len() as u64,
                archive_offset,
                archive_size: chunk_data.len() as u64,
                compression: protobuf::SingularPtrField::from_option(Some(use_compression.into())),
                unknown_fields: std::default::Default::default(),
                cached_size: std::default::Default::default(),
            });

            chunk_file.write_all(chunk_data).expect("write chunk");
            archive_offset += chunk_data.len() as u64;
        };

        if let Some(ref input_path) = config.input {
            // Read source from file
            let mut src_file = File::open(&input_path)
                .chain_err(|| format!("unable to open input file ({})", input_path.display()))?;
            let mut chunker = Chunker::new(chunker_params.clone(), &mut src_file);
            let (tmp_file_size, tmp_file_hash, tmp_chunks) = unique_compressed_chunks(
                &mut chunker,
                hasher,
                chunk_compressor,
                &pool,
                true,
                process_chunk,
            )
            .chain_err(|| "unable to compress chunk")?;
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
            )
            .chain_err(|| "unable to compress chunk")?;
            total_file_size = tmp_file_size;
            file_hash = tmp_file_hash;
            chunk_order = tmp_chunks;
        } else {
            bail!("Missing input file")
        }
    }
    pool.join();

    println!(
        "Total chunks: {}, unique: {}, size: {}, avg chunk size: {}, compressed into: {}",
        chunk_order.len(),
        total_unique_chunks,
        size_to_str(total_unique_chunk_size),
        size_to_str(total_unique_chunk_size / total_unique_chunks),
        size_to_str(total_compressed_size)
    );

    Ok(ChunkFileDescriptor {
        total_file_size,
        file_hash,
        chunk_descriptors,
        chunk_order,
    })
}

pub fn run(config: &CompressConfig, pool: &ThreadPool) -> Result<()> {
    let mut output_file = OpenOptions::new()
        .write(true)
        .create(config.base.force_create)
        .truncate(config.base.force_create)
        .create_new(!config.base.force_create)
        .open(&config.output)
        .chain_err(|| format!("unable to create output file ({})", config.output.display()))?;

    let mut tmp_chunk_file = OpenOptions::new()
        .write(true)
        .read(true)
        .truncate(true)
        .create(true)
        .open(&config.temp_file)
        .chain_err(|| "unable to create temporary chunk file")?;

    // Generate chunks and store to a temp file
    let chunk_file_descriptor = chunk_into_file(&config, pool, &mut tmp_chunk_file)?;

    println!(
        "Source hash: {}",
        HexSlice::new(&chunk_file_descriptor.file_hash)
    );

    // Store header to output file
    let file_header = chunk_dictionary::ChunkDictionary {
        rebuild_order: chunk_file_descriptor
            .chunk_order
            .iter()
            .map(|source_descriptor| source_descriptor.unique_chunk_index as u32)
            .collect(),
        application_version: crate::PKG_VERSION.to_string(),
        chunk_descriptors: RepeatedField::from_vec(chunk_file_descriptor.chunk_descriptors),
        source_checksum: chunk_file_descriptor.file_hash,
        chunk_data_location: SingularPtrField::none(),
        source_total_size: chunk_file_descriptor.total_file_size as u64,
        chunker_params: SingularPtrField::some(chunk_dictionary::ChunkerParameters {
            chunk_filter_bits: config.chunk_filter_bits,
            min_chunk_size: config.min_chunk_size as u64,
            max_chunk_size: config.max_chunk_size as u64,
            hash_window_size: config.hash_window_size as u32,
            chunk_hash_length: config.hash_length as u32,
            unknown_fields: std::default::Default::default(),
            cached_size: std::default::Default::default(),
        }),
        unknown_fields: std::default::Default::default(),
        cached_size: std::default::Default::default(),
    };

    // Copy chunks from temporary chunk tile to the output one
    let header_buf = archive::build_header(&file_header)?;
    println!("Header size: {}", header_buf.len());
    output_file
        .write_all(&header_buf)
        .chain_err(|| "failed to write header")?;
    tmp_chunk_file
        .seek(SeekFrom::Start(0))
        .chain_err(|| "failed to seek")?;
    io::copy(&mut tmp_chunk_file, &mut output_file)
        .chain_err(|| "failed to write chunk data to output file")?;
    drop(tmp_chunk_file);
    fs::remove_file(&config.temp_file).chain_err(|| "unable to remove temporary file")?;

    Ok(())
}
