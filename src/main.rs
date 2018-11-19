extern crate bincode;
extern crate docopt;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate lzma;
extern crate num_cpus;
extern crate sha2;
extern crate threadpool;

mod buzhash;
mod chunker;
mod chunker_utils;
mod compress_cmd;
mod config;
mod file_format;
mod ordered_mpsc;
mod string_utils;

use std::env;
use std::process;
use threadpool::ThreadPool;

use config::*;
use docopt::Docopt;

const USAGE: &'static str = "
Usage:
  bita [options] compress [INPUT] <OUTPUT>
  bita [options] unpack <URL/FILE> [OUTPUT]

Generic options:
  -h, --help              Show this screen.
  -f, --force-create      Overwrite output files if they exist.

Compressor options:
  --hash-length LENGTH    Truncate the length of the stored strong hash [default: 64].

  --avg-chunk-size SIZE   Average size of chunks [default: 64KiB].
  --min-chunk-size SIZE   Minimal size of chunks [default: 16KiB].
  --max-chunk-size SIZE   Maximal size of chunks [default: 16MiB].

  --buzhash-window SIZE   Size of the buzhash window [default: 16B].

  SIZE parameter be given in units 'B' (default), 'KiB', 'MiB', 'GiB', and 'TiB'.
";

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
    let argv: Vec<String> = env::args().collect();

    let args = Docopt::new(USAGE)
        .and_then(|d| d.argv(argv.into_iter()).parse())
        .unwrap_or_else(|e| e.exit());

    if args.get_bool("compress") {
        let output = args.get_str("OUTPUT");
        let temp_file = output.to_string() + ".tmp";

        let avg_chunk_size = parse_size(args.get_str("--avg-chunk-size"));
        let min_chunk_size = parse_size(args.get_str("--min-chunk-size"));
        let max_chunk_size = parse_size(args.get_str("--max-chunk-size"));
        let hash_window_size = parse_size(args.get_str("--buzhash-window"));

        println!("avg_chunk_size={}", avg_chunk_size);
        println!("min_chunk_size={}", min_chunk_size);
        println!("max_chunk_size={}", max_chunk_size);

        if min_chunk_size > avg_chunk_size {
            panic!("min-chunk-size > avg-chunk-size");
        }
        if max_chunk_size < avg_chunk_size {
            panic!("max-chunk-size < avg-chunk-size");
        }

        Config::Compress(CompressConfig {
            base: BaseConfig {
                force_create: args.get_bool("--force-create"),
            },
            input: args.get_str("INPUT").to_string(),
            output: output.to_string(),
            hash_length: args.get_str("--hash-length").parse().expect("LENGTH"),
            temp_file: temp_file,
            avg_chunk_size: avg_chunk_size,
            min_chunk_size: min_chunk_size,
            max_chunk_size: max_chunk_size,
            hash_window_size: hash_window_size,
        })
    } else {
        println!("Unknown command");
        process::exit(1);
    }
}

fn main() {
    //let config = parse_opts();
    let num_threads = num_cpus::get();
    let pool = ThreadPool::new(num_threads);

    match parse_opts() {
        Config::Compress(config) => compress_cmd::run(config, pool),
        _ => process::exit(1),
    }
}
