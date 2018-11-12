extern crate bincode;
extern crate getopts;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate deflate;
extern crate num_cpus;
extern crate sha2;
extern crate threadpool;

mod buzhash;
mod chunker;
mod ordered_mpsc;

use bincode::serialize_into;
use getopts::Options;
use sha2::{Digest, Sha512};
use std::collections::{hash_map::Entry, HashMap};
use std::env;
use std::fmt;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::prelude::*;
use std::process;
use std::sync::mpsc::{channel, Receiver, Sender};
use threadpool::ThreadPool;

use deflate::{deflate_bytes_conf, Compression};

use buzhash::BuzHash;
use chunker::*;
use ordered_mpsc::OrderedMPSC;

#[derive(Debug)]
struct Config {
    src: Option<String>,
    output: String,
    block_size: usize,
    truncate_hash: usize,
}

fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} <output file> [options]", program);
    print!("{}", opts.usage(&brief));
    println!();
    println!("    SIZE values can be given in units 'TiB', 'GiB', 'MiB', 'KiB', or 'B' (default).");
    println!();
}

fn parse_size(size_str: &str) -> usize {
    let size_val: String = size_str.chars().filter(|a| a.is_numeric()).collect();
    let size_val: usize = size_val.parse().expect("parse");
    let size_unit: String = size_str.chars().filter(|a| !a.is_numeric()).collect();
    if size_unit.len() == 0 {
        return size_val;
    }
    match size_unit.as_str() {
        "TiB" => 1024 * 1024 * 1024 * 1024 * size_val,
        "GiB" => 1024 * 1024 * 1024 * size_val,
        "MiB" => 1024 * 1024 * size_val,
        "KiB" => 1024 * size_val,
        "B" => size_val,
        _ => panic!("Invalid size unit"),
    }
}

fn parse_opts() -> Config {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();
    let mut opts = Options::new();

    opts.optopt(
        "s",
        "source",
        "set source file to digest (default is stdin)",
        "FILE",
    );
    opts.optopt("b", "block-size", "set block-size (default 1KiB)", "SIZE");
    opts.optopt(
        "t",
        "truncate-hash",
        "truncate hash value to bytes (default is 64)",
        "limit",
    );
    opts.optflag("h", "help", "print this help menu");
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => panic!(f.to_string()),
    };
    if matches.opt_present("h") {
        print_usage(&program, opts);
        process::exit(1);
    }
    let block_size = if let Some(ref val) = matches.opt_str("b") {
        parse_size(val)
    } else {
        1024
    };
    let truncate_hash: usize = if let Some(ref val) = matches.opt_str("t") {
        val.parse().expect("limit")
    } else {
        64
    };
    let src = matches.opt_str("s");
    let output = if matches.free.len() == 1 {
        matches.free[0].clone()
    } else {
        print_usage(&program, opts);
        process::exit(1);
    };

    Config {
        src: src,
        output: output,
        block_size: block_size,
        truncate_hash: truncate_hash,
    }
}

struct HexSlice<'a>(&'a [u8]);
impl<'a> HexSlice<'a> {
    fn new<T>(data: &'a T) -> HexSlice<'a>
    where
        T: ?Sized + AsRef<[u8]> + 'a,
    {
        HexSlice(data.as_ref())
    }
}
impl<'a> fmt::Display for HexSlice<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{:x}", byte)?;
        }
        Ok(())
    }
}
fn size_to_str(size: &usize) -> String {
    if *size > 1024 * 1024 {
        format!("{} MiB ({} bytes)", size / (1024 * 1024), size)
    } else if *size > 1024 {
        format!("{} KiB ({} bytes)", size / (1024), size)
    } else {
        format!("{} bytes", size)
    }
}

#[derive(Debug, Clone)]
struct HashedChunk {
    hash: Vec<u8>,
    chunk: Chunk,
}

#[derive(Debug, Clone)]
struct CompressedChunk {
    hash: Vec<u8>,
    chunk: Chunk,
    cdata: Vec<u8>,
}

#[derive(Debug, Clone)]
struct ChunkDesc {
    offset: usize,
    size: usize,
    hash: Vec<u8>,
}

// Calculate a strong hash on every chunk and forward each chunk
// Returns an array of chunk index.
fn chunk_and_hash<T, F, H>(
    src: &mut T,
    mut chunker: Chunker,
    hash_chunk: H,
    pool: &ThreadPool,
    mut result: F,
) -> io::Result<(Vec<ChunkDesc>)>
where
    T: Read,
    F: FnMut(HashedChunk),
    H: Fn(&[u8]) -> Vec<u8> + Send + 'static + Copy,
{
    let mut chunks: Vec<ChunkDesc> = Vec::new();
    let mut chunk_channel = OrderedMPSC::new();
    chunker
        .scan(src, |chunk| {
            // For each chunk in file
            println!("Got chunk (offset: {})", chunk.offset);
            let chunk = chunk.clone();
            let chunk_tx = chunk_channel.new_tx();
            pool.execute(move || {
                chunk_tx
                    .send(HashedChunk {
                        hash: hash_chunk(&chunk.data),
                        chunk: chunk,
                    })
                    .expect("chunk_tx");
            });

            chunk_channel.rx().try_iter().for_each(|hashed_chunk| {
                result(hashed_chunk);
            });
        })
        .expect("chunker");

    // Wait for threads to be done
    pool.join();

    // Forward the last hashed chunks
    chunk_channel.rx().try_iter().for_each(|hashed_chunk| {
        result(hashed_chunk);
    });
    Ok(chunks)
}

// Iterate only unique chunks of a source
fn unique_chunks<T, F, H>(
    src: &mut T,
    chunker: Chunker,
    hash_chunk: H,
    pool: &ThreadPool,
    mut result: F,
) -> io::Result<Vec<ChunkDesc>>
where
    T: Read,
    F: FnMut(HashedChunk),
    H: Fn(&[u8]) -> Vec<u8> + Send + 'static + Copy,
{
    let mut chunk_map: HashMap<Vec<u8>, usize> = HashMap::new();
    return chunk_and_hash(
        src,
        chunker,
        hash_chunk,
        pool,
        |hashed_chunk| match chunk_map.entry(hashed_chunk.hash.clone()) {
            Entry::Occupied(o) => {
                (*o.into_mut()) += 1;
            }
            Entry::Vacant(v) => {
                v.insert(1);
                result(hashed_chunk);
            }
        },
    );
}

// Iterate unique and compressed chunks
fn unique_compressed_chunks<T, F, C, H>(
    src: &mut T,
    chunker: Chunker,
    hash_chunk: H,
    compress_chunk: C,
    pool: &ThreadPool,
    mut result: F,
) -> io::Result<()>
where
    T: Read,
    F: FnMut(CompressedChunk),
    C: Fn(&[u8]) -> Vec<u8> + Send + 'static + Copy,
    H: Fn(&[u8]) -> Vec<u8> + Send + 'static + Copy,
{
    let mut chunk_channel = OrderedMPSC::new();
    unique_chunks(src, chunker, hash_chunk, &pool, |hashed_chunk| {
        // For each unique chunk
        let chunk_tx = chunk_channel.new_tx();
        pool.execute(move || {
            // Compress the chunk (in thread context)
            let cdata = compress_chunk(&hashed_chunk.chunk.data);
            chunk_tx
                .send(CompressedChunk {
                    hash: hashed_chunk.hash,
                    chunk: hashed_chunk.chunk,
                    cdata: cdata,
                })
                .expect("chunk_tx");
        });

        chunk_channel.rx().try_iter().for_each(|compressed_chunk| {
            result(compressed_chunk);
        });
    })?;

    // Wait for threads to be done
    pool.join();

    // Forward the compressed chunks
    chunk_channel.rx().try_iter().for_each(|compressed_chunk| {
        result(compressed_chunk);
    });
    Ok(())
}

fn main() {
    let config = parse_opts();
    let num_threads = num_cpus::get();
    let pool = ThreadPool::new(num_threads);

    // Read from stdin
    let stdin = io::stdin();
    let mut src_file = stdin.lock();

    // Setup the chunker
    let chunker = Chunker::new(
        15,
        1024 * 1024,
        16 * 1024,
        16 * 1024 * 1024,
        BuzHash::new(16, 0x10324195),
    );

    // Compress a chunk
    let compress_chunk = |data: &[u8]| deflate_bytes_conf(data, Compression::Best);

    // Generate strong hash for a chunk
    let hash_chunk = |data: &[u8]| {
        let mut hasher = Sha512::new();
        hasher.input(data);
        hasher.result().to_vec()
    };

    let mut total_compressed_size = 0;
    let mut total_unique_chunks = 0;
    let mut total_unique_chunk_size = 0;
    let chunk_index = unique_compressed_chunks(
        &mut src_file,
        chunker,
        hash_chunk,
        compress_chunk,
        &pool,
        |compressed_chunk| {
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
        },
    )
    .expect("chunk_and_hash");

    pool.join();

    println!(
        "Unique chunks: {}, total size: {}, avg chunk size: {}, compressed size: {}",
        total_unique_chunks,
        size_to_str(&total_unique_chunk_size),
        size_to_str(&(total_unique_chunk_size / total_unique_chunks)),
        size_to_str(&total_compressed_size)
    );

    /*    let output_file = OpenOptions::new()
        .truncate(true)
        .create(true)
        .write(true)
        .open(&config.output)
        .expect(&format!("failed to create file ({})", config.output));
    if let Some(source) = &config.src {
        let mut src_file = File::open(&source).expect(&format!("failed to open file ({})", source));
        chunk_file(&mut hasher, &mut src_file);
    } else {
        // Read from stdin
        let stdin = io::stdin();
        let mut src_file = stdin.lock();
        chunk_file(&mut hasher, &mut src_file);
    }*/
}
