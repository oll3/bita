use deflate::{deflate_bytes_conf, Compression};
use sha2::{Digest, Sha512};
use std::fmt;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use string_utils::*;
use threadpool::ThreadPool;

use buzhash::BuzHash;
use chunker::*;
use chunker_utils::*;
use config::*;

pub fn run(config: CompressConfig, pool: ThreadPool) {
    // TODO: Check if provided files can be opened

    // Read from stdin

    // Setup the chunker
    let chunker = Chunker::new(
        15,
        1024 * 1024,
        16 * 1024,
        16 * 1024 * 1024,
        BuzHash::new(16, 0x10324195),
    );

    // Compress a chunk
    let compressor = |data: &[u8]| deflate_bytes_conf(data, Compression::Best);

    // Generate strong hash for a chunk
    let hasher = |data: &[u8]| {
        let mut hasher = Sha512::new();
        hasher.input(data);
        hasher.result().to_vec()
    };

    let mut total_compressed_size = 0;
    let mut total_unique_chunks = 0;
    let mut total_unique_chunk_size = 0;
    let chunks;
    {
        let stats = |compressed_chunk: CompressedChunk| {
            // For each unique and compressed chunk
            println!(
                "Chunk '{}', size: {}, compressed to: {}",
                HexSlice::new(&compressed_chunk.hash[0..8]),
                size_to_str(&compressed_chunk.chunk.data.len()),
                size_to_str(&compressed_chunk.cdata.len())
            );
            total_unique_chunks += 1;
            total_unique_chunk_size += compressed_chunk.chunk.data.len();
            total_compressed_size += compressed_chunk.cdata.len();
        };

        if let Some(source) = &config.input {
            let mut src_file =
                File::open(&source).expect(&format!("failed to open file ({})", source));
            chunks =
                unique_compressed_chunks(&mut src_file, chunker, hasher, compressor, &pool, stats)
                    .expect("compress chunks");
        } else {
            let stdin = io::stdin();
            let mut src_file = stdin.lock();
            chunks =
                unique_compressed_chunks(&mut src_file, chunker, hasher, compressor, &pool, stats)
                    .expect("compress chunks");
        }
    }
    pool.join();

    println!(
        "Total chunks: {}, unique: {}, size: {}, avg chunk size: {}, compressed: {}",
        chunks.len(),
        total_unique_chunks,
        size_to_str(&total_unique_chunk_size),
        size_to_str(&(total_unique_chunk_size / total_unique_chunks)),
        size_to_str(&total_compressed_size)
    );
}
