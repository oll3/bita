use lzma::LzmaWriter;
use sha2::{Digest, Sha512};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use string_utils::*;
use threadpool::ThreadPool;

use buzhash::BuzHash;
use chunker::*;
use chunker_utils::*;
use config::*;
use file_format;

fn chunks_to_file(
    config: &CompressConfig,
    pool: ThreadPool,
    chunk_file: &mut File,
) -> (Vec<u8>, Vec<ChunkDesc>, Vec<file_format::ChunkDescriptor>) {
    // Setup the chunker
    let chunker = Chunker::new(
        15,
        1024 * 1024,
        16 * 1024,
        16 * 1024 * 1024,
        BuzHash::new(16, 0x10324195),
    );

    // Compress a chunk
    let chunk_compressor = |data: &[u8]| {
        let mut result = vec![];
        {
            let mut f = LzmaWriter::new_compressor(&mut result, 6).unwrap();
            f.write(data);
            f.finish();
        }
        return result;
    };

    // Generate strong hash for a chunk
    let hasher = |data: &[u8]| {
        let mut hasher = Sha512::new();
        hasher.input(data);
        hasher.result().to_vec()
    };

    let mut total_compressed_size = 0;
    let mut total_unique_chunks = 0;
    let mut total_unique_chunk_size = 0;
    let mut chunk_offset: u64 = 0;
    let mut chunk_lookup = Vec::new();
    let chunks;
    let file_hash;
    {
        let process_chunk = |compressed_chunk: CompressedChunk| {
            // For each unique and compressed chunk
            let hash = &compressed_chunk.hash[0..config.hash_length];
            println!(
                "Chunk '{}', size: {}, compressed to: {}",
                HexSlice::new(&hash),
                size_to_str(&compressed_chunk.chunk.data.len()),
                size_to_str(&compressed_chunk.cdata.len())
            );
            total_unique_chunks += 1;
            total_unique_chunk_size += compressed_chunk.chunk.data.len();
            total_compressed_size += compressed_chunk.cdata.len();

            // Store a chunk descriptor which referes to the compressed data
            chunk_lookup.push(file_format::ChunkDescriptor {
                hash: hash.to_vec(),
                offset: chunk_offset,
                chunk_size: compressed_chunk.cdata.len() as u64,
                compressed: true,
            });

            chunk_file
                .write(&compressed_chunk.cdata)
                .expect("write chunk");

            chunk_offset += compressed_chunk.cdata.len() as u64;
        };

        if config.input.len() > 0 {
            // Read source from file
            let mut src_file = File::open(&config.input)
                .expect(&format!("failed to open file ({})", config.input));

            let (tmp_file_hash, tmp_chunks) = unique_compressed_chunks(
                &mut src_file,
                chunker,
                hasher,
                chunk_compressor,
                &pool,
                process_chunk,
            ).expect("compress chunks");
            file_hash = tmp_file_hash;
            chunks = tmp_chunks;
        } else {
            // Read source from stdin
            let stdin = io::stdin();
            let mut src_file = stdin.lock();
            let (tmp_file_hash, tmp_chunks) = unique_compressed_chunks(
                &mut src_file,
                chunker,
                hasher,
                chunk_compressor,
                &pool,
                process_chunk,
            ).expect("compress chunks");
            file_hash = tmp_file_hash;
            chunks = tmp_chunks;
        }
    }
    pool.join();

    println!(
        "Total chunks: {}, unique: {}, size: {}, avg chunk size: {}, compressed into: {}",
        chunks.len(),
        total_unique_chunks,
        size_to_str(&total_unique_chunk_size),
        size_to_str(&(total_unique_chunk_size / total_unique_chunks)),
        size_to_str(&total_compressed_size)
    );

    return (file_hash, chunks, chunk_lookup);
}

pub fn run(config: CompressConfig, pool: ThreadPool) {
    let mut output_file = OpenOptions::new()
        .write(true)
        .create(config.base.force_create)
        .truncate(config.base.force_create)
        .create_new(!config.base.force_create)
        .open(&config.output)
        .expect(&format!("failed to create file ({})", config.output));

    println!("config.temp_file={}", config.temp_file);

    let mut tmp_chunk_file = OpenOptions::new()
        .write(true)
        .read(true)
        .truncate(true)
        .create(true)
        .open(&config.temp_file)
        .expect("create temp file");

    // Generate chunks and store to a temp file
    let (file_hash, chunks, chunk_lookup) = chunks_to_file(&config, pool, &mut tmp_chunk_file);

    println!("Source hash: {}", HexSlice::new(&file_hash));

    // Generate the file chunk index
    let mut build_index = Vec::new();
    for chunk in chunks {
        build_index.push(chunk.unique_chunk_index as u64);
    }

    // Store header to output file
    let file_header = file_format::FileHeader {
        compression: file_format::Compression::LZMA,
        build_index: build_index,
        chunk_lookup: chunk_lookup,
        source_hash: file_hash,
    };

    // Copy chunks from temporary chunk tile to the output one
    output_file
        .write(&file_format::build_header(&file_header))
        .expect("write header");
    tmp_chunk_file.seek(SeekFrom::Start(0)).expect("seek");
    io::copy(&mut tmp_chunk_file, &mut output_file).expect("copy temp file");
    drop(tmp_chunk_file);
    fs::remove_file(&config.temp_file).expect("remove temp file");
}
