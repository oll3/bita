use clap::error::ErrorKind;
use clap::{value_parser, Arg, ArgAction, ArgMatches, Command};
use log::LevelFilter;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use std::collections::HashMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::Duration;
use url::Url;

use crate::clone_cmd;
use crate::compress_cmd;
use crate::diff_cmd;
use crate::info_cmd;
use crate::string_utils::*;
use crate::PKG_NAME;
use crate::PKG_VERSION;
use bitar::chunker;
use bitar::Compression;
use bitar::HashSum;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogOpts {
    pub filter: LevelFilter,
}

impl LogOpts {
    fn new(filter: LevelFilter) -> Self {
        Self { filter }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandOpts {
    Compress(compress_cmd::Options),
    Clone(clone_cmd::Options),
    Info(info_cmd::Options),
    Diff(diff_cmd::Options),
}

pub fn parse_opts<I, T>(args: I) -> Result<(CommandOpts, LogOpts), clap::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let compress_subcmd = add_chunker_args(
        Command::new("compress")
            .about("Compress a file or stream")
            .arg(
                Arg::new("INPUT")
                    .short('i')
                    .long("input")
                    .value_name("FILE")
                    .value_parser(value_parser!(PathBuf))
                    .help("Input file, if none is given stdin is used")
                    .required(false),
            )
            .arg(output_file_arg())
            .arg(force_create_arg())
            .arg(buffered_chunks_arg())
            .arg(
                Arg::new("metadata-file")
                    .long("metadata-file")
                    .num_args(2) // Expect exactly 2 values (key and path) each time
                    .action(clap::ArgAction::Append) // Append to the list of values
                    .value_names(&["KEY", "PATH"])
                    .help("Custom metadata key-value pair where the value is a file contents"),
            )
            .arg(
                Arg::new("metadata-value")
                    .long("metadata-value")
                    .num_args(2) // Expect exactly 2 values (key and path) each time
                    .action(clap::ArgAction::Append) // Append to the list of values
                    .value_names(&["NAME", "VALUE"])
                    .help("Custom metadata key-value pair where the value is a provided string"),
            ),
    );

    let clone_subcmd = add_archive_input_http_args(
        Command::new("clone")
            .about("Clone a remote (or local archive). The archive is unpacked while being cloned.")
            .arg(input_archive_arg())
            .arg(
                Arg::new("verify-header")
                    .long("verify-header")
                    .value_name("CHECKSUM")
                    .value_parser(parse_hash_sum)
                    .help("Verify that the archive header checksum is the one given"),
            )
            .arg(output_file_arg())
            .arg(
                Arg::new("seed")
                    .value_name("FILE")
                    .value_parser(value_parser!(OsString))
                    .action(ArgAction::Append)
                    .long("seed")
                    .help("File to use as seed while cloning or '-' to read from stdin"),
            )
            .arg(
                Arg::new("seed-output")
                    .long("seed-output")
                    .action(ArgAction::SetTrue)
                    .help("Use the output file as seed and update in-place"),
            )
            .arg(force_create_arg())
            .arg(
                Arg::new("verify-output")
                    .long("verify-output")
                    .action(ArgAction::SetTrue)
                    .help("Verify that the checksum of the output matches with the archive"),
            )
            .arg(buffered_chunks_arg()),
    );

    let diff_subcmd = add_chunker_args(
        Command::new("diff")
            .about("Show the differential between two files")
            .arg(
                Arg::new("A")
                    .value_name("FILE")
                    .value_parser(value_parser!(PathBuf))
                    .help("Input file A")
                    .required(true),
            )
            .arg(
                Arg::new("B")
                    .value_name("FILE")
                    .value_parser(value_parser!(PathBuf))
                    .help("Input file B")
                    .required(true),
            )
            .arg(buffered_chunks_arg()),
    );

    let info_subcmd = add_archive_input_http_args(
        Command::new("info")
            .about("Print archive details")
            .arg(
                Arg::new("metadata-key")
                    .long("metadata-key")
                    .value_name("KEY")
                    .help("Print only the metadata value for the given key"),
            )
            .arg(input_archive_arg()),
    );

    let mut cmd = Command::new(PKG_NAME)
        .version(PKG_VERSION)
        .arg_required_else_help(true)
        .arg(
            Arg::new("verbose")
                .short('v')
                .global(true)
                .action(ArgAction::Count)
                .help("Set verbosity level"),
        )
        .subcommand(compress_subcmd)
        .subcommand(clone_subcmd)
        .subcommand(info_subcmd)
        .subcommand(diff_subcmd);

    let matches = cmd.try_get_matches_from_mut(args)?;
    let log_opts = LogOpts::new(match matches.get_count("verbose") {
        0 => LevelFilter::Info,
        1 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    });

    let num_chunk_buffers = |m: &ArgMatches| {
        m.get_one::<usize>("buffered-chunks")
            .copied()
            .unwrap_or(match num_cpus::get() {
                0 | 1 => 1,
                n => n * 2,
            })
    };

    if let Some(matches) = matches.subcommand_matches("compress") {
        let output = matches.get_one::<PathBuf>("OUTPUT").unwrap();
        let input = matches.get_one::<PathBuf>("INPUT");
        let temp_file = Path::with_extension(output, ".tmp");
        let hash_length = *matches.get_one::<u32>("hash-length").unwrap();
        let chunker_config = parse_chunker_config(&mut cmd, matches)?;
        let compression = parse_compression(&mut cmd, matches)?;

        let mut metadata_files: HashMap<String, PathBuf> = HashMap::new();
        if let Some(values) = matches.get_many::<String>("metadata-file") {
            let values: Vec<_> = values.collect();
            for pair in values.chunks_exact(2) {
                if let [key, value] = *pair {
                    metadata_files.insert(key.to_owned(), PathBuf::from(value));
                }
            }
        }

        let mut metadata_strings: HashMap<String, String> = HashMap::new();
        if let Some(values) = matches.get_many::<String>("metadata-value") {
            let values: Vec<_> = values.collect();
            for pair in values.chunks_exact(2) {
                if let [key, value] = *pair {
                    metadata_strings.insert(key.to_owned(), value.to_owned());
                }
            }
        }

        Ok((
            CommandOpts::Compress(compress_cmd::Options {
                input: input.cloned(),
                output: output.to_path_buf(),
                hash_length: hash_length as usize,
                force_create: matches.get_flag("force-create"),
                temp_file,
                chunker_config,
                compression,
                num_chunk_buffers: num_chunk_buffers(matches),
                metadata_files,
                metadata_strings,
            }),
            log_opts,
        ))
    } else if let Some(matches) = matches.subcommand_matches("clone") {
        let output = matches.get_one::<PathBuf>("OUTPUT").unwrap();
        let mut seed_stdin = false;
        let seed_files = matches
            .get_many::<OsString>("seed")
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
        let seed_output = matches.get_flag("seed-output");
        let header_checksum = matches.get_one::<HashSum>("verify-header").cloned();
        let input_archive = parse_input_archive_config(&mut cmd, matches)?;
        Ok((
            CommandOpts::Clone(clone_cmd::Options {
                input_archive,
                header_checksum,
                output: output.clone(),
                force_create: matches.get_flag("force-create"),
                seed_files,
                seed_stdin,
                verify_output: matches.get_flag("verify-output"),
                seed_output,
                num_chunk_buffers: num_chunk_buffers(matches),
            }),
            log_opts,
        ))
    } else if let Some(matches) = matches.subcommand_matches("info") {
        let metadata_key = matches.get_one::<String>("metadata-key");
        let input_archive = parse_input_archive_config(&mut cmd, matches)?;
        Ok((
            CommandOpts::Info(info_cmd::Options {
                input_archive,
                metadata_key: metadata_key.cloned(),
            }),
            log_opts,
        ))
    } else if let Some(matches) = matches.subcommand_matches("diff") {
        let input_a = matches.get_one::<PathBuf>("A").unwrap();
        let input_b = matches.get_one::<PathBuf>("B").unwrap();
        let chunker_config = parse_chunker_config(&mut cmd, matches)?;
        let compression = parse_compression(&mut cmd, matches)?;
        Ok((
            CommandOpts::Diff(diff_cmd::Options {
                input_a: input_a.clone(),
                input_b: input_b.clone(),
                chunker_config,
                compression,
                num_chunk_buffers: num_chunk_buffers(matches),
            }),
            log_opts,
        ))
    } else {
        Err(cmd.error(ErrorKind::InvalidSubcommand, ""))
    }
}

fn parse_chunker_opts(
    cmd: &mut Command,
    matches: &clap::ArgMatches,
) -> Result<chunker::FilterConfig, clap::Error> {
    let avg_chunk_size = *matches.get_one::<usize>("avg-chunk-size").unwrap();
    let min_chunk_size = *matches.get_one::<usize>("min-chunk-size").unwrap();
    let max_chunk_size = *matches.get_one::<usize>("max-chunk-size").unwrap();
    let filter_bits = chunker::FilterBits::from_size(avg_chunk_size as u32);
    if min_chunk_size > avg_chunk_size {
        return Err(cmd.error(
            ErrorKind::ValueValidation,
            "Min chunk size can't be bigger than the target average chunk size",
        ));
    }
    if max_chunk_size < avg_chunk_size {
        return Err(cmd.error(
            ErrorKind::ValueValidation,
            "Max chunk size can't be smaller than the target average chunk size",
        ));
    }
    let window_size = *matches.get_one::<usize>("rolling-window-size").unwrap();
    Ok(chunker::FilterConfig {
        filter_bits,
        min_chunk_size,
        max_chunk_size,
        window_size,
    })
}

fn parse_chunker_config(
    cmd: &mut Command,
    matches: &clap::ArgMatches,
) -> Result<chunker::Config, clap::Error> {
    Ok(
        match (
            matches.get_one::<usize>("fixed-size"),
            matches.get_one::<String>("hash-chunking").unwrap().as_ref(),
        ) {
            (Some(fixed_size), _) => chunker::Config::FixedSize(*fixed_size),
            (_, "RollSum") => chunker::Config::RollSum(parse_chunker_opts(cmd, matches)?),
            (_, "BuzHash") => chunker::Config::BuzHash(parse_chunker_opts(cmd, matches)?),
            _ => unreachable!(),
        },
    )
}

fn parse_compression(
    cmd: &mut Command,
    matches: &clap::ArgMatches,
) -> Result<Option<Compression>, clap::Error> {
    let compression_level = *matches.get_one::<u32>("compression-level").unwrap();
    let validation_err = |err| cmd.error(ErrorKind::ValueValidation, err);
    Ok(
        match matches.get_one::<String>("compression").unwrap().as_ref() {
            #[cfg(feature = "lzma-compression")]
            "lzma" => Some(Compression::lzma(compression_level).map_err(validation_err)?),
            #[cfg(feature = "zstd-compression")]
            "zstd" => Some(Compression::zstd(compression_level).map_err(validation_err)?),
            "brotli" => Some(Compression::brotli(compression_level).map_err(validation_err)?),
            "none" => None,
            _name => return Err(cmd.error(ErrorKind::ValueValidation, "Invalid compression")),
        },
    )
}

fn parse_input_archive_config(
    cmd: &mut Command,
    matches: &clap::ArgMatches,
) -> Result<clone_cmd::InputArchive, clap::Error> {
    let input = matches.get_one::<OsString>("ARCHIVE").unwrap();
    if Path::new(&input).exists() {
        return Ok(clone_cmd::InputArchive::Local(input.into()));
    }

    // Absolute windows paths, e.g. C:\temp.cba can be parsed as a URL. This
    // check prevents invalid paths from being used as a remote path.
    if Url::from_file_path(input).is_ok() {
        return Err(cmd.error(
            ErrorKind::ValueValidation,
            format!("{} does not exist", Path::new(&input).display()),
        ));
    }

    if let Some(Ok(url)) = input.to_str().map(|s| s.parse::<Url>()) {
        // Use as URL
        return Ok(clone_cmd::InputArchive::Remote(Box::new(
            clone_cmd::RemoteInput {
                url,
                retries: *matches.get_one::<u32>("http-retry-count").unwrap(),
                retry_delay: Duration::from_secs(
                    *matches.get_one::<u64>("http-retry-delay").unwrap(),
                ),
                receive_timeout: matches
                    .get_one::<u64>("http-timeout")
                    .copied()
                    .map(Duration::from_secs),
                headers: match matches.get_many::<String>("http-header") {
                    Some(values) => {
                        let mut headers = HeaderMap::new();
                        for header in values {
                            let mut split = header.splitn(2, ':');
                            let name = split.next().unwrap().trim_end_matches(": ").trim();
                            let value = split
                                .next()
                                .ok_or_else(|| {
                                    cmd.error(ErrorKind::ValueValidation, "Missing header value")
                                })?
                                .trim();
                            headers.insert(
                                HeaderName::from_bytes(name.as_bytes())
                                    .map_err(|err| cmd.error(ErrorKind::ValueValidation, err))?,
                                HeaderValue::from_str(value)
                                    .map_err(|err| cmd.error(ErrorKind::ValueValidation, err))?,
                            );
                        }
                        headers
                    }
                    None => HeaderMap::new(),
                },
            },
        )));
    };

    Err(cmd.error(
        ErrorKind::ValueValidation,
        format!(
            "{} does not seem to be pointing to neither a local or remote archive",
            Path::new(&input).display()
        ),
    ))
}

fn parse_hash_sum(hex_str: &str) -> Result<HashSum, std::num::ParseIntError> {
    hex_str_to_vec(hex_str).map(HashSum::from)
}

fn add_archive_input_http_args(cmd: Command) -> Command {
    cmd.arg(
        Arg::new("http-retry-count")
            .long("http-retry-count")
            .value_name("COUNT")
            .value_parser(value_parser!(u32))
            .default_value("0")
            .help("Retry transfer on failure"),
    )
    .arg(
        Arg::new("http-retry-delay")
            .long("http-retry-delay")
            .value_name("SECONDS")
            .value_parser(value_parser!(u64))
            .default_value("0")
            .help("Delay retry for some time on transfer failure"),
    )
    .arg(
        Arg::new("http-timeout")
            .long("http-timeout")
            .value_name("SECONDS")
            .value_parser(value_parser!(u64))
            .help("Fail transfer if unresponsive for some time"),
    )
    .arg(
        Arg::new("http-header")
            .long("http-header")
            .value_name("HEADER")
            .action(ArgAction::Append)
            .help("Provide custom http header(s)"),
    )
}

fn add_chunker_args(cmd: Command) -> Command {
    cmd.arg(
        Arg::new("avg-chunk-size")
            .long("avg-chunk-size")
            .value_name("SIZE")
            .value_parser(parse_human_size)
            .default_value("64KiB")
            .help("Indication of target chunk size"),
    )
    .arg(
        Arg::new("min-chunk-size")
            .long("min-chunk-size")
            .value_name("SIZE")
            .value_parser(parse_human_size)
            .default_value("16KiB")
            .help("Set minimal size of chunks"),
    )
    .arg(
        Arg::new("max-chunk-size")
            .long("max-chunk-size")
            .value_name("SIZE")
            .value_parser(parse_human_size)
            .default_value("16MiB")
            .help("Set maximal size of chunks"),
    )
    .arg(
        Arg::new("hash-chunking")
            .long("hash-chunking")
            .value_name("HASH")
            .value_parser(["RollSum", "BuzHash"])
            .default_value("RollSum")
            .help("Set hash to use for chunking"),
    )
    .arg(
        Arg::new("rolling-window-size")
            .long("rolling-window-size")
            .value_name("SIZE")
            .value_parser(parse_human_size)
            .default_value_if("hash-chunking", "RollSum", "64B")
            .default_value_if("hash-chunking", "BuzHash", "16B")
            .default_value("64B")
            .help("Set size of the rolling hash window to use for chunking"),
    )
    .arg(
        Arg::new("fixed-size")
            .long("fixed-size")
            .value_name("SIZE")
            .value_parser(parse_human_size)
            .help("Use fixed size chunking instead of rolling hash")
            .conflicts_with("hash-chunking"),
    )
    .arg(
        Arg::new("compression-level")
            .long("compression-level")
            .value_name("LEVEL")
            .default_value("6")
            .value_parser(value_parser!(u32).range(1..22))
            .help("Set the chunk data compression level"),
    )
    .arg(
        Arg::new("compression")
            .long("compression")
            .value_name("TYPE")
            .value_parser([
                "brotli",
                #[cfg(feature = "lzma-compression")]
                "lzma",
                #[cfg(feature = "zstd-compression")]
                "zstd",
                "none",
            ])
            .default_value("brotli")
            .help("Set the chunk data compression type"),
    )
    .arg(
        Arg::new("hash-length")
            .long("hash-length")
            .value_name("LENGTH")
            .default_value("64")
            .value_parser(value_parser!(u32).range(4..=(HashSum::MAX_LEN as i64)))
            .help("Truncate the length of the stored chunk hash"),
    )
}

fn buffered_chunks_arg() -> Arg {
    let help = "Limit number of chunks processed simultaneously [default: CPU cores x 2]";
    Arg::new("buffered-chunks")
        .long("buffered-chunks")
        .value_name("COUNT")
        .value_parser(value_parser!(usize))
        .global(true)
        .help(help)
}

fn output_file_arg() -> Arg {
    Arg::new("OUTPUT")
        .value_name("OUTPUT")
        .value_parser(value_parser!(PathBuf))
        .help("Output file")
        .required(true)
}

fn input_archive_arg() -> Arg {
    Arg::new("ARCHIVE")
        .value_name("ARCHIVE")
        .value_parser(value_parser!(OsString))
        .help("Can either be the path to a local archive or the URL to a remote archive")
        .required(true)
}

fn force_create_arg() -> Arg {
    Arg::new("force-create")
        .short('f')
        .long("force-create")
        .action(ArgAction::SetTrue)
        .help("Overwrite output files if they exist")
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
    use tempfile::NamedTempFile;

    fn get_num_chunk_buffers() -> usize {
        match num_cpus::get() {
            0 | 1 => 1,
            n => n * 2,
        }
    }

    #[test]
    fn compress_command_default() {
        let (opts, log) = parse_opts([
            "bita",
            "compress",
            "-v",
            "-i",
            "./input.img",
            "./output.cba",
        ])
        .unwrap_or_else(|e| panic!("{}", e));
        assert_eq!(log, LogOpts::new(LevelFilter::Debug));
        assert_eq!(
            opts,
            CommandOpts::Compress(compress_cmd::Options {
                force_create: false,
                input: Some("./input.img".into()),
                output: "./output.cba".into(),
                temp_file: "./output..tmp".into(),
                hash_length: 64,
                chunker_config: chunker::Config::RollSum(chunker::FilterConfig {
                    filter_bits: chunker::FilterBits(15),
                    min_chunk_size: 16384,
                    max_chunk_size: 16777216,
                    window_size: 64
                }),
                compression: Some(
                    Compression::try_new(bitar::CompressionAlgorithm::Brotli, 6).unwrap()
                ),
                num_chunk_buffers: get_num_chunk_buffers(),
                metadata_files: std::collections::HashMap::new(),
                metadata_strings: std::collections::HashMap::new(),
            })
        );
    }

    #[test]
    fn compress_command_stdin() {
        let (opts, log) =
            parse_opts(["bita", "compress", "./output.cba"]).unwrap_or_else(|e| panic!("{}", e));
        assert_eq!(log, LogOpts::new(LevelFilter::Info));
        assert_eq!(
            opts,
            CommandOpts::Compress(compress_cmd::Options {
                force_create: false,
                input: None,
                output: "./output.cba".into(),
                temp_file: "./output..tmp".into(),
                hash_length: 64,
                chunker_config: chunker::Config::RollSum(chunker::FilterConfig {
                    filter_bits: chunker::FilterBits(15),
                    min_chunk_size: 16384,
                    max_chunk_size: 16777216,
                    window_size: 64
                }),
                compression: Some(
                    Compression::try_new(bitar::CompressionAlgorithm::Brotli, 6).unwrap()
                ),
                num_chunk_buffers: get_num_chunk_buffers(),
                metadata_files: std::collections::HashMap::new(),
                metadata_strings: std::collections::HashMap::new(),
            })
        );
    }

    #[test]
    fn compress_command_specific() {
        let (opts, log) = parse_opts([
            "bita",
            "compress",
            "-v",
            "--compression",
            "brotli",
            "--compression-level",
            "2",
            "--hash-chunking",
            "BuzHash",
            "--min-chunk-size",
            "2KiB",
            "--max-chunk-size",
            "1MiB",
            "--rolling-window-size",
            "10",
            "--hash-length",
            "12",
            "-f",
            "-i",
            "./input.img",
            "./output.cba",
        ])
        .unwrap_or_else(|e| panic!("{}", e));
        assert_eq!(log, LogOpts::new(LevelFilter::Debug));
        assert_eq!(
            opts,
            CommandOpts::Compress(compress_cmd::Options {
                force_create: true,
                input: Some("./input.img".into()),
                output: "./output.cba".into(),
                temp_file: "./output..tmp".into(),
                hash_length: 12,
                chunker_config: chunker::Config::BuzHash(chunker::FilterConfig {
                    filter_bits: chunker::FilterBits(15),
                    min_chunk_size: 2 * 1024,
                    max_chunk_size: 1024 * 1024,
                    window_size: 10
                }),
                compression: Some(
                    Compression::try_new(bitar::CompressionAlgorithm::Brotli, 2).unwrap()
                ),
                num_chunk_buffers: get_num_chunk_buffers(),
                metadata_files: std::collections::HashMap::new(),
                metadata_strings: std::collections::HashMap::new(),
            })
        );
    }

    #[test]
    fn clone_command_local_archive() {
        let input = NamedTempFile::new().unwrap();
        let input_path = input.path();
        let mut temp_file_path = input_path.to_path_buf();
        temp_file_path.set_extension(".tmp");

        let (opts, log) = parse_opts([
            "bita",
            "clone",
            "--seed",
            "./seed1.img",
            "--seed",
            "./seed2.img",
            &input.path().to_string_lossy(),
            "./output.img",
            "--verify-output",
        ])
        .unwrap_or_else(|e| panic!("{}", e));
        assert_eq!(log, LogOpts::new(LevelFilter::Info));
        assert_eq!(
            opts,
            CommandOpts::Clone(clone_cmd::Options {
                force_create: false,
                input_archive: clone_cmd::InputArchive::Local(input_path.into()),
                header_checksum: None,
                output: "./output.img".into(),
                seed_stdin: false,
                seed_files: vec!["./seed1.img".into(), "./seed2.img".into()],
                seed_output: false,
                verify_output: true,
                num_chunk_buffers: get_num_chunk_buffers(),
            })
        );
    }

    #[test]
    fn clone_command_seed_from_stdin() {
        let input = NamedTempFile::new().unwrap();
        let input_path = input.path();
        let mut temp_file_path = input_path.to_path_buf();
        temp_file_path.set_extension(".tmp");

        let (opts, log) = parse_opts([
            "bita",
            "clone",
            "--seed",
            "-",
            "--seed",
            "./seed.img",
            &input.path().to_string_lossy(),
            "./output.img",
        ])
        .unwrap_or_else(|e| panic!("{}", e));
        assert_eq!(log, LogOpts::new(LevelFilter::Info));
        assert_eq!(
            opts,
            CommandOpts::Clone(clone_cmd::Options {
                force_create: false,
                input_archive: clone_cmd::InputArchive::Local(input_path.into()),
                header_checksum: None,
                output: "./output.img".into(),
                seed_stdin: true,
                seed_files: vec!["./seed.img".into()],
                seed_output: false,
                verify_output: false,
                num_chunk_buffers: get_num_chunk_buffers(),
            })
        );
    }

    #[test]
    fn clone_command_verify_header() {
        let input = NamedTempFile::new().unwrap();
        let input_path = input.path();
        let mut temp_file_path = input_path.to_path_buf();
        temp_file_path.set_extension(".tmp");

        let (opts, log) = parse_opts([
            "bita",
            "clone",
            "--verify-header",
            "5520529d1175327f9a39df0a75fe6bd314f9e6bedd89734c508a043c66066c7ada2a7b493659794f916840d976e9f0b10ec94a09caec0296ced9666998ec7977",
            &input.path().to_string_lossy(),
            "./output.img",
        ])
            .unwrap_or_else(|e| panic!("{}", e));
        assert_eq!(log, LogOpts::new(LevelFilter::Info));
        assert_eq!(
            opts,
            CommandOpts::Clone(clone_cmd::Options {
                force_create: false,
                input_archive: clone_cmd::InputArchive::Local(input_path.into()),
                header_checksum: Some(parse_hash_sum("5520529d1175327f9a39df0a75fe6bd314f9e6bedd89734c508a043c66066c7ada2a7b493659794f916840d976e9f0b10ec94a09caec0296ced9666998ec7977").unwrap()),
                output: "./output.img".into(),
                seed_stdin: false,
                seed_files: vec![],
                seed_output: false,
                verify_output: false,
                num_chunk_buffers: get_num_chunk_buffers(),
            })
        );
    }

    #[test]
    fn clone_command_remote_archive() {
        let (opts, log) = parse_opts([
            "bita",
            "clone",
            "--seed",
            "./seed1.img",
            "--seed",
            "./seed2.img",
            "https://some-url.com/archive.cba",
            "./output.img",
        ])
        .unwrap_or_else(|e| panic!("{}", e));
        assert_eq!(log, LogOpts::new(LevelFilter::Info));
        assert_eq!(
            opts,
            CommandOpts::Clone(clone_cmd::Options {
                force_create: false,
                input_archive: clone_cmd::InputArchive::Remote(Box::new(clone_cmd::RemoteInput {
                    url: "https://some-url.com/archive.cba".try_into().unwrap(),
                    headers: HeaderMap::new(),
                    receive_timeout: None,
                    retries: 0,
                    retry_delay: Duration::from_secs(0),
                })),
                header_checksum: None,
                output: "./output.img".into(),
                seed_stdin: false,
                seed_files: vec!["./seed1.img".into(), "./seed2.img".into()],
                seed_output: false,
                verify_output: false,
                num_chunk_buffers: get_num_chunk_buffers(),
            })
        );
    }

    #[test]
    fn clone_command_remote_archive_headers() {
        let (opts, log) = parse_opts([
            "bita",
            "-vv",
            "clone",
            "-f",
            "--seed",
            "./seed1.img",
            "--seed",
            "./seed2.img",
            "--http-header",
            "Content-Type:application/x-binary",
            "--http-header",
            "Authorization:Bearer eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1lIjoiSm9obiBEb2UifQ.aB8iVwyPVgggFD179Gm2bhlTYFuT_P3D0nzqaJ4YhUa1LjLTrtCbcYtFTVZF6qFSnEhMKTYQ4-lHfkeXOH9dFw",
            "https://some-url.com/archive.cba",
            "./output.img",
        ]).unwrap_or_else(|e| panic!("{}", e));

        let mut headers = HeaderMap::new();
        headers.append(CONTENT_TYPE, "application/x-binary".parse().unwrap());
        headers.append(AUTHORIZATION, "Bearer eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1lIjoiSm9obiBEb2UifQ.aB8iVwyPVgggFD179Gm2bhlTYFuT_P3D0nzqaJ4YhUa1LjLTrtCbcYtFTVZF6qFSnEhMKTYQ4-lHfkeXOH9dFw".parse().unwrap());
        assert_eq!(log, LogOpts::new(LevelFilter::Trace));
        assert_eq!(
            opts,
            CommandOpts::Clone(clone_cmd::Options {
                force_create: true,
                input_archive: clone_cmd::InputArchive::Remote(Box::new(clone_cmd::RemoteInput {
                    url: "https://some-url.com/archive.cba".try_into().unwrap(),
                    headers,
                    receive_timeout: None,
                    retries: 0,
                    retry_delay: Duration::from_secs(0),
                })),
                header_checksum: None,
                output: "./output.img".into(),
                seed_stdin: false,
                seed_files: vec!["./seed1.img".into(), "./seed2.img".into()],
                seed_output: false,
                verify_output: false,
                num_chunk_buffers: get_num_chunk_buffers(),
            })
        );
    }

    #[test]
    fn info_command() {
        let input = NamedTempFile::new().unwrap();
        let input_path = input.path();
        let mut temp_file_path = input_path.to_path_buf();
        temp_file_path.set_extension(".tmp");

        let (info, log) = parse_opts(["bita", "info", "-v", &input.path().to_string_lossy()])
            .unwrap_or_else(|e| panic!("{}", e));
        assert_eq!(log, LogOpts::new(LevelFilter::Debug));
        assert_eq!(
            info,
            CommandOpts::Info(info_cmd::Options {
                input_archive: clone_cmd::InputArchive::Local(input_path.into()),
                metadata_key: None,
            }),
        );
    }

    #[test]
    fn info_command_missing_input() {
        parse_opts(["bita", "info"]).unwrap_err();
    }

    #[test]
    fn diff_command() {
        let (opts, log) =
            parse_opts(["bita", "diff", "file1", "file2"]).unwrap_or_else(|e| panic!("{}", e));
        assert_eq!(log, LogOpts::new(LevelFilter::Info));
        assert_eq!(
            opts,
            CommandOpts::Diff(diff_cmd::Options {
                input_a: "file1".into(),
                input_b: "file2".into(),
                chunker_config: chunker::Config::RollSum(chunker::FilterConfig {
                    filter_bits: chunker::FilterBits(15),
                    min_chunk_size: 16384,
                    max_chunk_size: 16777216,
                    window_size: 64
                }),
                compression: Some(
                    Compression::try_new(bitar::CompressionAlgorithm::Brotli, 6).unwrap()
                ),
                num_chunk_buffers: get_num_chunk_buffers(),
            })
        );
    }
}
