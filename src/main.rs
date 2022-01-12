mod clone_cmd;
mod compress_cmd;
mod diff_cmd;
mod info_cmd;
mod string_utils;

use anyhow::{anyhow, bail, Context, Result};
use clap::{App, Arg, SubCommand};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use std::path::Path;
use std::time::Duration;
use url::Url;

use crate::string_utils::*;
use bitar::chunker;
use bitar::Compression;
use bitar::HashSum;

pub const PKG_NAME: &str = env!("CARGO_PKG_NAME");
pub const PKG_VERSION: &str = env!("CARGO_PKG_VERSION");

fn parse_hash_chunker_config(
    matches: &clap::ArgMatches<'_>,
    default_window_size: &str,
) -> Result<chunker::FilterConfig> {
    let avg_chunk_size = parse_size(matches.value_of("avg-chunk-size").unwrap_or("64KiB"))?;
    let min_chunk_size = parse_size(matches.value_of("min-chunk-size").unwrap_or("16KiB"))?;
    let max_chunk_size = parse_size(matches.value_of("max-chunk-size").unwrap_or("16MiB"))?;
    let filter_bits = chunker::FilterBits::from_size(avg_chunk_size as u32);
    if min_chunk_size > avg_chunk_size {
        return Err(anyhow!(
            "Min chunk size can't be bigger than the target average chunk size"
        ));
    }
    if max_chunk_size < avg_chunk_size {
        return Err(anyhow!(
            "Max chunk size can't be smaller than the target average chunk size"
        ));
    }
    let window_size = parse_size(
        matches
            .value_of("rolling-window-size")
            .unwrap_or(default_window_size),
    )?;
    Ok(chunker::FilterConfig {
        filter_bits,
        min_chunk_size,
        max_chunk_size,
        window_size,
    })
}

fn parse_chunker_config(matches: &clap::ArgMatches<'_>) -> Result<chunker::Config> {
    Ok(
        match (
            matches.value_of("fixed-size"),
            matches
                .value_of("hash-chunking")
                .unwrap_or("RollSum")
                .to_lowercase()
                .as_ref(),
        ) {
            (Some(fixed_size), _) => chunker::Config::FixedSize(parse_size(fixed_size)?),
            (_, "rollsum") => chunker::Config::RollSum(parse_hash_chunker_config(matches, "64B")?),
            (_, "buzhash") => chunker::Config::BuzHash(parse_hash_chunker_config(matches, "16B")?),
            _ => unreachable!(),
        },
    )
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

fn parse_compression(matches: &clap::ArgMatches<'_>) -> Result<Option<Compression>> {
    let compression_level = matches
        .value_of("compression-level")
        .unwrap_or("6")
        .parse()
        .context("Failed to parse compression level")?;
    Ok(
        match matches
            .value_of("compression")
            .unwrap_or("brotli")
            .to_lowercase()
            .as_ref()
        {
            #[cfg(feature = "lzma-compression")]
            "lzma" => Some(Compression::lzma(compression_level)?),
            #[cfg(feature = "zstd-compression")]
            "zstd" => Some(Compression::zstd(compression_level)?),
            "brotli" => Some(Compression::brotli(compression_level)?),
            "none" => None,
            name => return Err(anyhow!("Invalid compression ({})", name)),
        },
    )
}

fn parse_size(size_str: &str) -> Result<usize> {
    let size_val: String = size_str.chars().filter(|a| a.is_numeric()).collect();
    let size_val: usize = size_val.parse().context("Failed to parse")?;
    let size_unit: String = size_str.chars().filter(|a| !a.is_numeric()).collect();
    if size_unit.is_empty() {
        return Ok(size_val);
    }
    Ok(match size_unit.as_str() {
        "GiB" => 1024 * 1024 * 1024 * size_val,
        "MiB" => 1024 * 1024 * size_val,
        "KiB" => 1024 * size_val,
        "B" => size_val,
        unit => return Err(anyhow!("Invalid unit ({})", unit)),
    })
}

fn parse_input_config(matches: &clap::ArgMatches<'_>) -> Result<clone_cmd::InputArchive> {
    let input = matches.value_of("INPUT").unwrap().to_string();
    Ok(match input.parse::<Url>() {
        Ok(url) => {
            // Use as URL
            clone_cmd::InputArchive::Remote(Box::new(clone_cmd::RemoteInput {
                url,
                retries: matches
                    .value_of("http-retry-count")
                    .unwrap_or("0")
                    .parse()
                    .context("Failed to parse http-retry-count")?,
                retry_delay: Duration::from_secs(
                    matches
                        .value_of("http-retry-delay")
                        .map(|v| v.parse())
                        .unwrap_or(Ok(0))
                        .context("Failed to parse http-retry-delay")?,
                ),
                receive_timeout: if let Some(v) = matches.value_of("http-timeout") {
                    Some(Duration::from_secs(
                        v.parse().context("Failed to parse http-timeout")?,
                    ))
                } else {
                    None
                },
                headers: match matches.values_of("http-header") {
                    Some(values) => {
                        let mut headers = HeaderMap::new();
                        for header in values {
                            let mut split = header.splitn(2, ':');
                            let name = split.next().unwrap().trim_end_matches(": ").trim();
                            let value = split.next().context("Missing header value")?.trim();
                            headers.insert(
                                HeaderName::from_bytes(name.as_bytes())
                                    .context("Invalid header name")?,
                                HeaderValue::from_str(value).context("Invalid header value")?,
                            );
                        }
                        headers
                    }
                    None => HeaderMap::new(),
                },
            }))
        }
        Err(_) => {
            // Use as path
            clone_cmd::InputArchive::Local(input.into())
        }
    })
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
        .context("Unable to initialize log")?;
    Ok(())
}

fn add_chunker_args<'a, 'b>(
    sub_cmd: clap::App<'a, 'b>,
    compression_desc: &'b str,
) -> clap::App<'a, 'b> {
    sub_cmd
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
            Arg::with_name("hash-chunking")
                .long("hash-chunking")
                .value_name("HASH")
                .help("Set hash to use for chunking (RollSum/BuzHash). [default: RollSum]"),
        )
        .arg(
            Arg::with_name("rolling-window-size")
                .long("rolling-window-size")
                .value_name("SIZE")
                .help("Set size of the rolling hash window to use for chunking. [default: 64B for RollSum, 16B for BuzHash]")
        )
        .arg(
            Arg::with_name("fixed-size")
                .long("fixed-size")
                .value_name("SIZE")
                .help("Use fixed size chunking instead of rolling hash.")
                .conflicts_with("hash-chunking"),
        )
        .arg(
            Arg::with_name("compression-level")
                .long("compression-level")
                .value_name("LEVEL")
                .help("Set the chunk data compression level [default: 6]"),
        )
        .arg(
            Arg::with_name("compression")
                .long("compression")
                .value_name("TYPE")
                .help(compression_desc),
        ).arg(
            Arg::with_name("hash-length")
                .long("hash-length")
                .value_name("LENGTH")
                .help("Truncate the length of the stored chunk hash [default: 64]"),
        )
}

fn add_input_archive_args<'a, 'b>(sub_cmd: clap::App<'a, 'b>) -> clap::App<'a, 'b> {
    sub_cmd
        .arg(
            Arg::with_name("INPUT")
                .value_name("INPUT")
                .help("Input file (can be a local archive or a URL)")
                .required(true),
        )
        .arg(
            Arg::with_name("http-retry-count")
                .long("http-retry-count")
                .value_name("COUNT")
                .help("Retry transfer on failure [default: 0]"),
        )
        .arg(
            Arg::with_name("http-retry-delay")
                .long("http-retry-delay")
                .value_name("SECONDS")
                .help("Delay retry for some time on transfer failure [default: 0]"),
        )
        .arg(
            Arg::with_name("http-timeout")
                .long("http-timeout")
                .value_name("SECONDS")
                .help("Fail transfer if unresponsive for some time [default: None]"),
        )
        .arg(
            Arg::with_name("http-header")
                .long("http-header")
                .value_name("HEADER")
                .multiple(true)
                .help("Provide custom http header"),
        )
        .arg(
            Arg::with_name("verify-header")
                .long("verify-header")
                .value_name("CHECKSUM")
                .help("Verify that the archive header checksum is the one given"),
        )
}

async fn parse_opts() -> Result<()> {
    let compression_desc = format!(
        "Set the chunk data compression type {}",
        compression_names()
    );
    let compress_subcmd = add_chunker_args(
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
                Arg::with_name("force-create")
                    .short("f")
                    .long("force-create")
                    .help("Overwrite output files if they exist"),
            ),
        &compression_desc,
    );
    let clone_subcmd =
        add_input_archive_args(SubCommand::with_name("clone").about(
            "Clone a remote (or local archive). The archive is unpacked while being cloned.",
        ))
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
            Arg::with_name("seed-output")
                .long("seed-output")
                .help("Use the output file as seed and update in-place."),
        )
        .arg(
            Arg::with_name("force-create")
                .short("f")
                .long("force-create")
                .help("Overwrite output files if they exist"),
        )
        .arg(
            Arg::with_name("verify-output")
                .long("verify-output")
                .help("Vefify that the checksum of the output matches with the archive."),
        );
    let diff_subcmd = add_chunker_args(
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
            ),
        &compression_desc,
    );
    let matches =
        App::new(PKG_NAME)
            .version(PKG_VERSION)
            .arg(
                Arg::with_name("verbose")
                    .short("v")
                    .multiple(true)
                    .global(true)
                    .help("Set verbosity level"),
            )
            .arg(Arg::with_name("buffered-chunks").long("buffered-chunks").value_name("COUNT").global(true).help(
                "Limit number of chunks processed simultaneously [default: cores available x 2]",
            ))
            .subcommand(compress_subcmd)
            .subcommand(clone_subcmd)
            .subcommand(
                SubCommand::with_name("info")
                    .about("Print archive details.")
                    .arg(
                        Arg::with_name("INPUT")
                            .value_name("INPUT")
                            .help("Input file (can be a local archive or a URL)")
                            .required(true),
                    ),
            )
            .subcommand(diff_subcmd)
            .get_matches();

    // Set log level
    init_log(match matches.occurrences_of("verbose") {
        0 => log::LevelFilter::Info,
        1 => log::LevelFilter::Debug,
        _ => log::LevelFilter::Trace,
    })?;

    let num_chunk_buffers: usize = if let Some(v) = matches.value_of("buffered-chunks") {
        v.parse().context("Invalid buffered-chunks value")?
    } else {
        match num_cpus::get() {
            // Single buffer if we have a single core, otherwise number of cores x 2
            0 | 1 => 1,
            n => n * 2,
        }
    };
    if let Some(matches) = matches.subcommand_matches("compress") {
        let output = Path::new(matches.value_of("OUTPUT").unwrap());
        let input = matches
            .value_of("INPUT")
            .map(|input| Path::new(input).to_path_buf());
        let temp_file = Path::with_extension(output, ".tmp");
        let hash_length = if let Some(hash_length) = matches.value_of("hash-length") {
            let hash_length = hash_length.parse::<usize>().context("parse hash length")?;
            if !(4..=HashSum::MAX_LEN).contains(&hash_length) {
                bail!(
                    "Invalid hash length value (valid range is 4-{})",
                    HashSum::MAX_LEN
                );
            }
            hash_length
        } else {
            HashSum::MAX_LEN
        };
        let chunker_config = parse_chunker_config(matches)?;
        let compression = parse_compression(matches)?;
        compress_cmd::compress_cmd(compress_cmd::Options {
            input,
            output: output.to_path_buf(),
            hash_length,
            force_create: matches.is_present("force-create"),
            temp_file,
            chunker_config,
            compression,
            num_chunk_buffers,
        })
        .await
    } else if let Some(matches) = matches.subcommand_matches("clone") {
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
        let seed_output = matches.is_present("seed-output");
        let header_checksum = if let Some(c) = matches.value_of("verify-header") {
            Some(HashSum::from(
                hex_str_to_vec(c).context("Failed to parse checksum")?,
            ))
        } else {
            None
        };
        let input_archive = parse_input_config(matches)?;
        clone_cmd::clone_cmd(clone_cmd::Options {
            input_archive,
            header_checksum,
            output: Path::new(output).to_path_buf(),
            force_create: matches.is_present("force-create"),
            seed_files,
            seed_stdin,
            verify_output: matches.is_present("verify-output"),
            seed_output,
            num_chunk_buffers,
        })
        .await
    } else if let Some(matches) = matches.subcommand_matches("info") {
        let input = matches.value_of("INPUT").unwrap();
        info_cmd::info_cmd(input.to_string()).await
    } else if let Some(matches) = matches.subcommand_matches("diff") {
        let input_a = Path::new(matches.value_of("A").unwrap());
        let input_b = Path::new(matches.value_of("B").unwrap());
        let chunker_config = parse_chunker_config(matches)?;
        let compression = parse_compression(matches)?;
        diff_cmd::diff_cmd(diff_cmd::Options {
            input_a: input_a.to_path_buf(),
            input_b: input_b.to_path_buf(),
            chunker_config,
            compression,
            num_chunk_buffers,
        })
        .await
    } else {
        Err(anyhow!("Unknown command"))
    }
}

fn main() -> Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async { parse_opts().await })
}
