use chrono;

use fern;
use log;

mod clone_cmd;
mod compress_cmd;
mod config;
mod diff_cmd;
mod info_cmd;

use clap::{App, Arg, SubCommand};
use log::*;
use std::path::Path;
use std::process;
use tokio;

use crate::config::*;
use bita::compression::Compression;
use bita::error::Error;
use bita::string_utils::hex_str_to_vec;
use bita::HashSum;

pub const PKG_NAME: &str = env!("CARGO_PKG_NAME");
pub const PKG_VERSION: &str = env!("CARGO_PKG_VERSION");

fn parse_chunker_config(matches: &clap::ArgMatches<'_>) -> ChunkerConfig {
    let avg_chunk_size = parse_size(matches.value_of("avg-chunk-size").unwrap_or("64KiB"));
    let min_chunk_size = parse_size(matches.value_of("min-chunk-size").unwrap_or("16KiB"));
    let max_chunk_size = parse_size(matches.value_of("max-chunk-size").unwrap_or("16MiB"));
    let hash_window_size = parse_size(matches.value_of("buzhash-window").unwrap_or("16B"));

    let compression_level = matches
        .value_of("compression-level")
        .unwrap_or("6")
        .parse()
        .expect("invalid compression level value");

    if compression_level < 1 || compression_level > 19 {
        panic!("compression level not within range");
    }

    let compression = match matches
        .value_of("compression")
        .unwrap_or("brotli")
        .to_lowercase()
        .as_ref()
    {
        #[cfg(feature = "lzma-compression")]
        "lzma" => Compression::LZMA(compression_level),
        #[cfg(feature = "zstd-compression")]
        "zstd" => Compression::ZSTD(compression_level),
        "brotli" => Compression::Brotli(compression_level),
        "none" => Compression::None,
        name => panic!("invalid compression {}", name),
    };

    let chunk_filter_bits = 30 - (avg_chunk_size as u32).leading_zeros();
    if min_chunk_size > avg_chunk_size {
        panic!("min-chunk-size > avg-chunk-size");
    }
    if max_chunk_size < avg_chunk_size {
        panic!("max-chunk-size < avg-chunk-size");
    }
    ChunkerConfig {
        chunk_filter_bits,
        min_chunk_size,
        max_chunk_size,
        hash_window_size,
        compression_level,
        compression,
    }
}

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

pub fn compression_names() -> String {
    let mut s = "(brotli, ".to_owned();
    #[cfg(feature = "lzma-compression")]
    {
        s += "lzma, ";
    }
    #[cfg(feature = "zstd-compression")]
    {
        s += "zstd, ";
    }
    s += "none) [default: brotli]";
    s
}

fn init_log(level: log::LevelFilter) {
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
        .expect("unable to initialize log");
}

fn parse_opts() -> Result<Config, Error> {
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
                        .help(&format!("Set the chunk data compression type {}", compression_names())),
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
                .about("Clone a remote (or local archive). The archive is unpacked while being cloned.")
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
                ).arg(
                    Arg::with_name("http-retry-count")
                        .long("http-retry-count")
                        .value_name("COUNT")
                        .help("Retry transfer on failure [default: 0]"),
                ).arg(
                    Arg::with_name("http-retry-delay")
                        .long("http-retry-delay")
                        .value_name("SECONDS")
                        .help("Delay retry for some time on transfer failure [default: 0]"),
                ).arg(
                    Arg::with_name("http-timeout")
                        .long("http-timeout")
                        .value_name("SECONDS")
                        .help("Fail transfer if unresponsive for some time [default: None]"),
                ).arg(
                    Arg::with_name("verify-output")
                        .long("verify-output")
                        .help("Verify that the checksum of the output matches with the archive."),
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
        .subcommand(
            SubCommand::with_name("diff")
                .about("Show the differential between two files.")
                .arg(
                    Arg::with_name("A")
                        .value_name("FILE")
                        .help("Input file A")
                        .required(true),
                )
                .arg(
                    Arg::with_name("B")
                        .value_name("FILE")
                        .help("Input file B")
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
                    Arg::with_name("compression-level")
                        .long("compression-level")
                        .value_name("LEVEL")
                        .help("Set the chunk data compression level (0-9) [default: 6]"),
                )
                .arg(
                    Arg::with_name("compression")
                        .long("compression")
                        .value_name("TYPE")
                        .help(&format!("Set the chunk data compression type {}", compression_names())),
                )
        )
        .get_matches();

    // Set log level
    init_log(match matches.occurrences_of("verbose") {
        0 => log::LevelFilter::Info,
        1 => log::LevelFilter::Debug,
        2 | _ => log::LevelFilter::Trace,
    });

    if let Some(matches) = matches.subcommand_matches("compress") {
        let output = Path::new(matches.value_of("OUTPUT").unwrap());
        let input = if let Some(input) = matches.value_of("INPUT") {
            Some(Path::new(input).to_path_buf())
        } else {
            None
        };
        let temp_file = Path::with_extension(output, ".tmp");
        let hash_length = matches.value_of("hash-length").unwrap_or("64");
        let chunker_config = parse_chunker_config(&matches);
        Ok(Config::Compress(CompressConfig {
            input,
            output: output.to_path_buf(),
            hash_length: hash_length.parse().expect("invalid hash length value"),
            force_create: matches.is_present("force-create"),
            temp_file,
            chunker_config,
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
            .map(|c| HashSum::from_vec(hex_str_to_vec(c).expect("failed to parse checksum")));

        let http_retry_count = matches
            .value_of("http-retry-count")
            .unwrap_or("0")
            .parse()
            .expect("failed to parse http-retry-count");

        let http_retry_delay = matches.value_of("http-retry-delay").map(|v| {
            std::time::Duration::from_secs(v.parse().expect("failed to parse http-retry-delay"))
        });

        let http_timeout = matches.value_of("http-timeout").map(|v| {
            std::time::Duration::from_secs(v.parse().expect("failed to parse http-timeout"))
        });

        Ok(Config::Clone(CloneConfig {
            input: input.to_string(),
            output: Path::new(output).to_path_buf(),
            force_create: matches.is_present("force-create"),
            header_checksum: verify_header,
            seed_files,
            seed_stdin,
            http_retry_count,
            http_retry_delay,
            http_timeout,
            verify_output: matches.is_present("verify-output"),
        }))
    } else if let Some(matches) = matches.subcommand_matches("info") {
        let input = matches.value_of("INPUT").unwrap();
        Ok(Config::Info(InfoConfig {
            input: input.to_string(),
        }))
    } else if let Some(matches) = matches.subcommand_matches("diff") {
        let input_a = Path::new(matches.value_of("A").unwrap());
        let input_b = Path::new(matches.value_of("B").unwrap());
        let chunker_config = parse_chunker_config(&matches);
        Ok(Config::Diff(DiffConfig {
            input_a: input_a.to_path_buf(),
            input_b: input_b.to_path_buf(),
            chunker_config,
        }))
    } else {
        error!("Unknown command");
        process::exit(1);
    }
}

#[tokio::main]
async fn main() {
    let result = match parse_opts() {
        Ok(Config::Compress(config)) => compress_cmd::run(config).await,
        Ok(Config::Clone(config)) => clone_cmd::run(config).await,
        Ok(Config::Info(config)) => info_cmd::run(config).await,
        Ok(Config::Diff(config)) => diff_cmd::run(config).await,
        Err(e) => Err(e),
    };
    if let Err(ref e) = result {
        error!("error: {}", e);
        ::std::process::exit(1);
    }
}
