extern crate bincode;
extern crate clap;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate lzma;
extern crate num_cpus;
extern crate sha2;
extern crate threadpool;

mod archive_reader;
mod buzhash;
mod chunker;
mod chunker_utils;
mod compress_cmd;
mod config;
mod file_format;
mod ordered_mpsc;
mod string_utils;
mod unpack_cmd;

use std::process;
use threadpool::ThreadPool;

use clap::{App, Arg, SubCommand};
use config::*;

const PKG_VERSION: &'static str = env!("CARGO_PKG_VERSION");
const PKG_NAME: &'static str = env!("CARGO_PKG_NAME");

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
    let matches = App::new(PKG_NAME)
        .version(PKG_VERSION)
        .arg(
            Arg::with_name("force-create")
                .short("f")
                .long("force-create")
                .help("Overwrite output files if they exist.")
                .global(true),
        ).subcommand(
            SubCommand::with_name("compress")
                .about("Compress a file or stream.")
                .arg(
                    Arg::with_name("INPUT")
                        .short("i")
                        .long("input")
                        .value_name("FILE")
                        .help("Input file. If none is given the stdin will be used.")
                        .required(false),
                ).arg(
                    Arg::with_name("OUTPUT")
                        .value_name("OUTPUT")
                        .help("Output file.")
                        .required(true),
                ).arg(
                    Arg::with_name("avg-chunk-size")
                        .long("avg-chunk-size")
                        .value_name("SIZE")
                        .help("Average size of chunks [default: 64KiB]."),
                ).arg(
                    Arg::with_name("min-chunk-size")
                        .long("min-chunk-size")
                        .value_name("SIZE")
                        .help("Minimal size of chunks [default: 16KiB]."),
                ).arg(
                    Arg::with_name("max-chunk-size")
                        .long("max-chunk-size")
                        .value_name("SIZE")
                        .help("Maximal size of chunks [default: 16MiB]."),
                ).arg(
                    Arg::with_name("buzhash-window")
                        .long("buzhash-window")
                        .value_name("SIZE")
                        .help("Size of the buzhash window [default: 16B]."),
                ).arg(
                    Arg::with_name("hash-length")
                        .long("hash-length")
                        .value_name("LENGTH")
                        .help("Truncate the length of the stored strong hash [default: 64]."),
                ),
        ).subcommand(
            SubCommand::with_name("unpack")
                .about("Unpack a file.")
                .arg(
                    Arg::with_name("INPUT")
                        .value_name("INPUT")
                        .help("Input file. Can be a local cba file or a URL.")
                        .required(true),
                ).arg(
                    Arg::with_name("OUTPUT")
                        .value_name("OUTPUT")
                        .help("Output file.")
                        .required(true),
                ).arg(
                    Arg::with_name("seed")
                        .value_name("FILE")
                        .long("seed")
                        .help("File(s) to use as seed while unpacking.")
                        .multiple(true),
                ),
        ).get_matches();

    let base_config = BaseConfig {
        force_create: matches.is_present("force-create"),
    };

    if let Some(matches) = matches.subcommand_matches("compress") {
        let output = matches.value_of("OUTPUT").unwrap();
        let input = matches.value_of("INPUT").unwrap_or("");
        let temp_file = output.to_string() + ".tmp";

        let avg_chunk_size = parse_size(matches.value_of("avg-chunk-size").unwrap_or("64KiB"));
        let min_chunk_size = parse_size(matches.value_of("min-chunk-size").unwrap_or("16KiB"));
        let max_chunk_size = parse_size(matches.value_of("max-chunk-size").unwrap_or("16MiB"));
        let hash_window_size = parse_size(matches.value_of("buzhash-window").unwrap_or("16B"));
        let hash_length = matches.value_of("hash-length").unwrap_or("64");

        if min_chunk_size > avg_chunk_size {
            panic!("min-chunk-size > avg-chunk-size");
        }
        if max_chunk_size < avg_chunk_size {
            panic!("max-chunk-size < avg-chunk-size");
        }

        Config::Compress(CompressConfig {
            base: base_config,
            input: input.to_string(),
            output: output.to_string(),
            hash_length: hash_length.parse().expect("LENGTH"),
            temp_file: temp_file,
            avg_chunk_size: avg_chunk_size,
            min_chunk_size: min_chunk_size,
            max_chunk_size: max_chunk_size,
            hash_window_size: hash_window_size,
        })
    } else if let Some(matches) = matches.subcommand_matches("unpack") {
        let input = matches.value_of("INPUT").unwrap();
        let output = matches.value_of("OUTPUT").unwrap_or("");
        let seed_files = matches
            .values_of("seed")
            .unwrap_or_default()
            .map(|s| s.to_string())
            .collect();
        Config::Unpack(UnpackConfig {
            base: base_config,
            input: input.to_string(),
            output: output.to_string(),
            seed_files: seed_files,
            seed_stdin: false,
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
        Config::Unpack(config) => unpack_cmd::run(config, pool),
        _ => process::exit(1),
    }
}
