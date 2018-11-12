extern crate bincode;
extern crate getopts;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate sha2;

mod buzhash;
mod chunker;

use bincode::serialize_into;
use getopts::Options;
use sha2::{Digest, Sha512};
use std::collections::{hash_map::Entry, HashMap};
use std::env;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::prelude::*;
use std::process;

use buzhash::BuzHash;
use chunker::*;

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

fn size_to_str(size: &usize) -> String {
    if *size > 1024 * 1024 {
        format!("{} MiB ({} bytes)", size / (1024 * 1024), size)
    } else if *size > 1024 {
        format!("{} KiB ({} bytes)", size / (1024), size)
    } else {
        format!("{} bytes", size)
    }
}

// Fill buffer with data from file, or until eof.
fn fill_buf<T>(file: &mut T, buf: &mut Vec<u8>) -> io::Result<usize>
where
    T: Read,
{
    let mut read_size = 0;
    let buf_size = buf.len();
    while read_size < buf_size {
        let rc = file.read(&mut buf[read_size..buf_size])?;
        if rc == 0 {
            break;
        } else {
            read_size += rc;
        }
    }
    return Ok(read_size);
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Block {
    size: u16,
    hash: Vec<u8>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Signature {
    // Point to a block by index
    index_to_block: Vec<u32>,

    // Block hash and block size
    block_hash: Vec<Block>,
}

struct BlockDesc {
    offset: usize,
    size: u16,
    hash: Vec<u8>,
}

struct SignatureDigest<H> {
    truncate_hash: usize,
    hasher: H,
    block_feed_index: usize,
    block_offset: usize,

    index_to_block: Vec<usize>,
    hash_to_block: HashMap<Vec<u8>, usize>,
    blocks: Vec<BlockDesc>,

    // Some stats
    block_dup: Vec<usize>,
    block_total_dup: usize,
    block_dedup_size: usize,
}

impl<H> SignatureDigest<H>
where
    H: sha2::Digest,
{
    fn new(hasher: H, truncate_hash: usize) -> Self {
        SignatureDigest {
            truncate_hash: truncate_hash,
            hasher: hasher,
            block_feed_index: 0,
            block_offset: 0,
            block_dup: Vec::new(),
            block_total_dup: 0,
            block_dedup_size: 0,

            index_to_block: Vec::new(),
            hash_to_block: HashMap::new(),
            blocks: Vec::new(),
        }
    }

    fn feed_block(&mut self, block: &[u8]) {
        // Hash the block data
        self.hasher.input(block);
        let mut block_hash = self.hasher.result_reset().to_vec();
        block_hash.truncate(self.truncate_hash);

        match self.hash_to_block.entry(block_hash) {
            Entry::Occupied(o) => {
                // Duplicated block
                //println!("DUP (current index: {}, dup at:
                let block_index = *o.into_mut();

                self.index_to_block.push(block_index);

                self.block_dup[block_index] += 1;
                self.block_total_dup += 1;
            }
            Entry::Vacant(v) => {
                // New block
                let block_index = self.block_dup.len();
                self.index_to_block.push(block_index);

                self.blocks.push(BlockDesc {
                    offset: self.block_offset,
                    size: block.len() as u16,
                    hash: v.key().to_vec(),
                });

                self.block_dup.push(1);
                self.block_dedup_size += block.len();

                v.insert(block_index);
            }
        };

        self.block_offset += block.len();
        self.block_feed_index += 1;
    }

    fn consume(self) -> Signature {
        Signature {
            index_to_block: self.index_to_block.iter().map(|i| *i as u32).collect(),
            block_hash: self
                .blocks
                .iter()
                .map(|b| Block {
                    size: b.size,
                    hash: b.hash.to_vec(),
                }).collect(),
        }
    }
}

fn generate_signature<T, H>(config: &Config, hasher: H, src: &mut T) -> io::Result<Signature>
where
    T: Read,
    H: sha2::Digest,
{
    let mut buf: Vec<u8> = vec![0; config.block_size as usize];
    let mut digest = SignatureDigest::new(hasher, config.truncate_hash);
    loop {
        let block_size = fill_buf(src, &mut buf)?;
        if block_size == 0 {
            break;
        }

        digest.feed_block(&buf[0..block_size]);
    }

    println!("Total file size: {}", size_to_str(&digest.block_offset));
    println!("Duplicated blocks: {}", digest.block_total_dup);
    println!("Dedup size: {}", size_to_str(&digest.block_dedup_size));
    return Ok(digest.consume());
}

fn main() {
    let config = parse_opts();

    let output_file = OpenOptions::new()
        .truncate(true)
        .create(true)
        .write(true)
        .open(&config.output)
        .expect(&format!("failed to create file ({})", config.output));

    let hasher = Sha512::new();

    let signature;
    if let Some(source) = &config.src {
        let mut src_file = File::open(&source).expect(&format!("failed to open file ({})", source));
        signature = generate_signature(&config, hasher, &mut src_file).expect("generate signature");
        serialize_into(output_file, &signature).expect("serialize");
    } else {
        // Read from stdin
        let stdin = io::stdin();
        let mut src_file = stdin.lock();

        let mut buzhash = BuzHash::new(32, 0x10324195);
        let mut chunker = Chunker::new(16, 1024 * 1024, buzhash, hasher);
        let mut size_array = Vec::new();
        let mut unique_chunks: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
        loop {
            if let Some(chunk) = chunker.scan(&mut src_file).expect("scan") {
                println!("Got chunk {}", chunk);
                size_array.push(chunk.length as u64);
                match unique_chunks.entry(chunk.hash) {
                    Entry::Occupied(o) => {
                        (*o.into_mut()).push(chunk.length);
                    }
                    Entry::Vacant(v) => {
                        v.insert(vec![chunk.length]);
                    }
                }
            } else {
                break;
            }
        }
        println!("Chunking done!");
        let tot_size: u64 = size_array.iter().sum();
        let tot_single_size: usize = unique_chunks
            .iter()
            .filter(|(_, v)| (**v).len() == 1)
            .map(|(_, v)| v[0])
            .sum();
        let tot_repeated_size: usize = unique_chunks
            .iter()
            .filter(|(_, v)| (**v).len() > 1)
            .map(|(_, v)| {
                let s: usize = v.iter().sum();
                s
            }).sum();

        println!(
            "Chunks: {},  avg size: {}, unique: {}, total singles size: {}, total repeated size: {}",
            size_array.len(),
            size_to_str(&((tot_size / size_array.len() as u64) as usize)),
            unique_chunks.len(),
            size_to_str(&tot_single_size),
            size_to_str(&tot_repeated_size)
        );

        //signature = generate_signature(&config, hasher, &mut src_file).expect("generate signature");
    }
}
