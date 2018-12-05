use blake2::{Blake2b, Digest};
use lzma::LzmaWriter;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Seek, SeekFrom, Write};
use string_utils::*;
use threadpool::ThreadPool;

use archive;
use buzhash::BuzHash;
use chunker::*;
use chunker_utils::*;
use config::*;

fn chunks_to_file(
    config: &CompressConfig,
    pool: ThreadPool,
    chunk_file: &mut File,
) -> (
    usize,
    Vec<u8>,
    Vec<ChunkDesc>,
    Vec<archive::ChunkDescriptor>,
) {
    // Setup the chunker
    let mut chunker = Chunker::new(
        1024 * 1024,
        config.chunk_filter_bits,
        config.min_chunk_size,
        config.max_chunk_size,
        BuzHash::new(config.hash_window_size as usize, 0x10324195),
    );

    // Compress a chunk
    fn chunk_compressor(data: &[u8]) -> Vec<u8> {
        let mut result = vec![];
        {
            let mut f = LzmaWriter::new_compressor(&mut result, 6).expect("new compressor");
            let mut wc = 0;
            while wc < data.len() {
                wc += f.write(&data[wc..]).expect("write compressor");
            }
            f.finish().expect("finish compressor");
        }
        return result;
    };

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
    let chunks;
    let file_size;
    let file_hash;
    {
        let process_chunk = |comp_chunk: CompressedChunk| {
            // For each unique and compressed chunk
            let chunk_data;
            let hash = &comp_chunk.hash[0..config.hash_length as usize];
            let use_compressed = comp_chunk.cdata.len() < comp_chunk.data.len();
            if use_compressed {
                // Use the compressed data
                chunk_data = &comp_chunk.cdata;
            } else {
                // Compressed chunk bigger than raw - Use raw
                chunk_data = &comp_chunk.data;
            }

            println!(
                "Chunk {}, '{}', offset: {}, size: {}, compressed to: {}, archive: {}",
                total_unique_chunks,
                HexSlice::new(&hash),
                comp_chunk.offset,
                size_to_str(comp_chunk.data.len()),
                size_to_str(comp_chunk.cdata.len()),
                match use_compressed {
                    true => "compressed",
                    false => "raw",
                }
            );

            total_unique_chunks += 1;
            total_unique_chunk_size += comp_chunk.data.len();
            total_compressed_size += chunk_data.len();

            // Store a chunk descriptor which referes to the compressed data
            chunk_descriptors.push(archive::ChunkDescriptor {
                hash: hash.to_vec(),
                source_offsets: vec![], // will be filled after chunking is done
                source_size: comp_chunk.data.len() as u64,
                archive_offset: archive_offset,
                archive_size: chunk_data.len() as u64,
                compressed: use_compressed,
            });

            chunk_file.write(chunk_data).expect("write chunk");
            archive_offset += chunk_data.len() as u64;
        };

        if config.input.len() > 0 {
            // Read source from file
            let mut src_file = File::open(&config.input)
                .expect(&format!("failed to open file ({})", config.input));

            let (tmp_file_size, tmp_file_hash, tmp_chunks) = unique_compressed_chunks(
                &mut src_file,
                &mut chunker,
                hasher,
                chunk_compressor,
                &pool,
                true,
                process_chunk,
            ).expect("compress chunks");
            file_size = tmp_file_size;
            file_hash = tmp_file_hash;
            chunks = tmp_chunks;
        } else {
            // Read source from stdin
            let stdin = io::stdin();
            let mut src_file = stdin.lock();
            let (tmp_file_size, tmp_file_hash, tmp_chunks) = unique_compressed_chunks(
                &mut src_file,
                &mut chunker,
                hasher,
                chunk_compressor,
                &pool,
                true,
                process_chunk,
            ).expect("compress chunks");
            file_size = tmp_file_size;
            file_hash = tmp_file_hash;
            chunks = tmp_chunks;
        }
    }
    pool.join();

    println!(
        "Total chunks: {}, unique: {}, size: {}, avg chunk size: {}, compressed into: {}",
        chunks.len(),
        total_unique_chunks,
        size_to_str(total_unique_chunk_size),
        size_to_str(total_unique_chunk_size / total_unique_chunks),
        size_to_str(total_compressed_size)
    );

    return (file_size, file_hash, chunks, chunk_descriptors);
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
    let (file_size, file_hash, chunks, mut chunk_descriptors) =
        chunks_to_file(&config, pool, &mut tmp_chunk_file);

    println!("Source hash: {}", HexSlice::new(&file_hash));

    // Fill out the destination offset of each chunk descriptor
    for chunk in chunks {
        chunk_descriptors[chunk.unique_chunk_index]
            .source_offsets
            .push(chunk.offset as u64);
    }

    // Store header to output file
    let file_header = archive::Header {
        version: archive::Version::V1(archive::HeaderV1 {
            compression: archive::Compression::LZMA,
            chunk_descriptors: chunk_descriptors,
            source_hash: file_hash,
            source_total_size: file_size as u64,
            chunk_filter_bits: config.chunk_filter_bits,
            min_chunk_size: config.min_chunk_size,
            max_chunk_size: config.max_chunk_size,
            hash_window_size: config.hash_window_size,
            hash_length: config.hash_length,
        }),
    };

    // Copy chunks from temporary chunk tile to the output one
    output_file
        .write(&archive::build_header(&file_header))
        .expect("write header");
    tmp_chunk_file.seek(SeekFrom::Start(0)).expect("seek");
    io::copy(&mut tmp_chunk_file, &mut output_file).expect("copy temp file");
    drop(tmp_chunk_file);
    fs::remove_file(&config.temp_file).expect("remove temp file");
}
