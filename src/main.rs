#[macro_use]
extern crate error_chain;
extern crate bita;
extern crate chrono;
extern crate clap;
extern crate fern;
extern crate log;
extern crate num_cpus;
extern crate threadpool;

mod clone_cmd;
mod compress_cmd;
mod config;
mod info_cmd;
mod string_utils;

use clap::{App, Arg, SubCommand};
use log::*;
use std::path::Path;
use std::process;
use threadpool::ThreadPool;

use crate::config::*;
use crate::string_utils::hex_str_to_vec;
use bita::compression::Compression;
use bita::errors::*;

pub const PKG_NAME: &str = env!("CARGO_PKG_NAME");
pub const PKG_VERSION: &str = env!("CARGO_PKG_VERSION");

fn parse_size(size_str: &str) -> usize {
    let size_val: String = size_str.chars().filter(|a| a.is_numeric()).collect();
    let size_val: usize = size_val.parse().expect("parse");
    let size_unit: String = size_str.chars().filter(|a| !a.is_numeric()).collect();
    if size_unit.is_empty() {
        return size_val;
    }
    match size_unit.as_str() {
        "GiB" => 1024 * 1024 * 1024 * size_val,
        "MiB" => 1024 * 1024 * size_val,
        "KiB" => 1024 * size_val,
        "B" => size_val,
        _ => panic!("Invalid size unit"),
    }
}

fn init_log(level: log::LevelFilter) -> Result<()> {
    let local_level = level;
    fern::Dispatch::new()
        .format(move |out, message, record| {
            if local_level > log::LevelFilter::Info {
                // Add some extra info to each message in debug
                out.finish(format_args!(
                    "[{}]({})({}) {}",
                    chrono::Local::now().format("%Y-%m-%dT%H:%M:%S%.3f"),
                    record.target(),
                    record.level(),
                    message
                ))
            } else {
                out.finish(format_args!("{}", message))
            }
        })
        .level(level)
        .chain(std::io::stdout())
        .apply()
        .chain_err(|| "unable to initialize log")?;
    Ok(())
}

fn parse_opts() -> Result<Config> {
    let matches = App::new(PKG_NAME)
        .version(PKG_VERSION)
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .multiple(true)
                .global(true)
                .help("Set verbosity level"),
        )
        .subcommand(
            SubCommand::with_name("compress")
                .about("Compress a file or stream.")
                .arg(
                    Arg::with_name("INPUT")
                        .short("i")
                        .long("input")
                        .value_name("FILE")
                        .help("Input file, if none is given stdin is used")
                        .required(false),
                )
                .arg(
                    Arg::with_name("OUTPUT")
                        .value_name("OUTPUT")
                        .help("Output file")
                        .required(true),
                )
                .arg(
                    Arg::with_name("avg-chunk-size")
                        .long("avg-chunk-size")
                        .value_name("SIZE")
                        .help("Indication of target chunk size [default: 64KiB]"),
                )
                .arg(
                    Arg::with_name("min-chunk-size")
                        .long("min-chunk-size")
                        .value_name("SIZE")
                        .help("Set minimal size of chunks [default: 16KiB]"),
                )
                .arg(
                    Arg::with_name("max-chunk-size")
                        .long("max-chunk-size")
                        .value_name("SIZE")
                        .help("Set maximal size of chunks [default: 16MiB]"),
                )
                .arg(
                    Arg::with_name("buzhash-window")
                        .long("buzhash-window")
                        .value_name("SIZE")
                        .help("Set size of the buzhash window [default: 16B]"),
                )
                .arg(
                    Arg::with_name("hash-length")
                        .long("hash-length")
                        .value_name("LENGTH")
                        .help("Truncate the length of the stored strong hash [default: 64]"),
                )
                .arg(
                    Arg::with_name("compression-level")
                        .long("compression-level")
                        .value_name("LEVEL")
                        .help("Set the chunk data compression level (0-9) [default: 6]"),
                )
                .arg(
                    Arg::with_name("compression")
                        .long("compression")
                        .value_name("TYPE")
                        .help("Set the chunk data compression type (LZMA, ZSTD, NONE) [default: LZMA]"),
                )
                .arg(
                    Arg::with_name("force-create")
                        .short("f")
                        .long("force-create")
                        .help("Overwrite output files if they exist"),
                ),
        )
        .subcommand(
            SubCommand::with_name("clone")
                .about("Clone a remote (or local archive). The archive is unpacked while beeing cloned.")
                .arg(
                    Arg::with_name("INPUT")
                        .value_name("INPUT")
                        .help("Input file (can be a local archive or a URL)")
                        .required(true),
                )
                .arg(
                    Arg::with_name("OUTPUT")
                        .value_name("OUTPUT")
                        .help("Output file")
                        .required(true),
                )
                .arg(
                    Arg::with_name("seed")
                        .value_name("FILE")
                        .long("seed")
                        .help("File to use as seed while cloning or '-' to read from stdin")
                        .multiple(true),
                )
                .arg(
                    Arg::with_name("force-create")
                        .short("f")
                        .long("force-create")
                        .help("Overwrite output files if they exist"),
                ).arg(
                    Arg::with_name("verify-header")
                        .long("verify-header")
                        .value_name("CHECKSUM")
                        .help("Verify that the archive header checksum is the one given"),
                ),
        )
        .subcommand(
            SubCommand::with_name("info")
                .about("Print archive details.")
                .arg(
                    Arg::with_name("INPUT")
                        .value_name("INPUT")
                        .help("Input file (can be a local archive or a URL)")
                        .required(true),
                )
        )
        .get_matches();

    // Set log level
    init_log(match matches.occurrences_of("verbose") {
        0 => log::LevelFilter::Info,
        1 => log::LevelFilter::Debug,
        2 | _ => log::LevelFilter::Trace,
    })?;

    if let Some(matches) = matches.subcommand_matches("compress") {
        let output = Path::new(matches.value_of("OUTPUT").unwrap());
        let input = if let Some(input) = matches.value_of("INPUT") {
            Some(Path::new(input).to_path_buf())
        } else {
            None
        };
        let temp_file = Path::with_extension(output, ".tmp");

        let avg_chunk_size = parse_size(matches.value_of("avg-chunk-size").unwrap_or("64KiB"));
        let min_chunk_size = parse_size(matches.value_of("min-chunk-size").unwrap_or("16KiB"));
        let max_chunk_size = parse_size(matches.value_of("max-chunk-size").unwrap_or("16MiB"));
        let hash_window_size = parse_size(matches.value_of("buzhash-window").unwrap_or("16B"));
        let hash_length = matches.value_of("hash-length").unwrap_or("64");

        let compression_level = matches
            .value_of("compression-level")
            .unwrap_or("6")
            .parse()
            .chain_err(|| "invalid compression level value")?;

        if compression_level < 1 || compression_level > 19 {
            bail!("compression level not within range");
        }

        let compression = match matches.value_of("compression").unwrap_or("LZMA") {
            "LZMA" | "lzma" => Compression::LZMA(compression_level),
            "ZSTD" | "zstd" => Compression::ZSTD(compression_level),
            "NONE" | "none" => Compression::None,
            _ => bail!("invalid compression"),
        };

        let chunk_filter_bits = 30 - (avg_chunk_size as u32).leading_zeros();
        if min_chunk_size > avg_chunk_size {
            bail!("min-chunk-size > avg-chunk-size");
        }
        if max_chunk_size < avg_chunk_size {
            bail!("max-chunk-size < avg-chunk-size");
        }

        Ok(Config::Compress(CompressConfig {
            input,
            output: output.to_path_buf(),
            hash_length: hash_length
                .parse()
                .chain_err(|| "invalid hash length value")?,
            force_create: matches.is_present("force-create"),
            temp_file,
            chunk_filter_bits,
            min_chunk_size,
            max_chunk_size,
            hash_window_size,
            compression_level,
            compression,
        }))
    } else if let Some(matches) = matches.subcommand_matches("clone") {
        let input = matches.value_of("INPUT").unwrap();
        let output = matches.value_of("OUTPUT").unwrap_or("");
        let mut seed_stdin = false;
        let seed_files = matches
            .values_of("seed")
            .unwrap_or_default()
            .filter(|s| {
                if *s == "-" {
                    seed_stdin = true;
                    false
                } else {
                    true
                }
            })
            .map(|s| Path::new(s).to_path_buf())
            .collect();

        let verify_header = matches
            .value_of("verify-header")
            .map(|c| hex_str_to_vec(c).expect("failed to parse checksum"));

        Ok(Config::Clone(CloneConfig {
            input: input.to_string(),
            output: Path::new(output).to_path_buf(),
            force_create: matches.is_present("force-create"),
            header_checksum: verify_header,
            seed_files,
            seed_stdin,
        }))
    } else if let Some(matches) = matches.subcommand_matches("info") {
        let input = matches.value_of("INPUT").unwrap();
        Ok(Config::Info(InfoConfig {
            input: input.to_string(),
        }))
    } else {
        error!("Unknown command");
        process::exit(1);
    }
}

fn main() {
    let num_threads = num_cpus::get();
    let pool = ThreadPool::new(num_threads);

    let result = match parse_opts() {
        Ok(Config::Compress(config)) => compress_cmd::run(&config, &pool),
        Ok(Config::Clone(config)) => clone_cmd::run(&config, &pool),
        Ok(Config::Info(config)) => info_cmd::run(&config),
        Err(e) => Err(e),
    };
    if let Err(ref e) = result {
        error!("error: {}", e);

        for e in e.iter().skip(1) {
            error!("Caused by: {}", e);
        }
        if let Some(backtrace) = e.backtrace() {
            error!("backtrace: {:?}", backtrace);
        }
        ::std::process::exit(1);
    }
}
