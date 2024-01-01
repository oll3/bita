use clap::ErrorKind;
use clap::{Arg, Command};
use log::LevelFilter;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use std::ffi::OsString;
use std::path::Path;
use std::time::Duration;
use url::Url;

use crate::clone_cmd;
use crate::compress_cmd;
use crate::diff_cmd;
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
    Info { input: String },
    Diff(diff_cmd::Options),
}

pub fn parse_opts<I, T>(args: I) -> Result<(CommandOpts, LogOpts), clap::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let compression_desc = format!(
        "Set the chunk data compression type {}",
        compression_names()
    );
    let compress_subcmd = add_chunker_args(
        Command::new("compress")
            .about("Compress a file or stream.")
            .arg(
                Arg::new("INPUT")
                    .short('i')
                    .long("input")
                    .value_name("FILE")
                    .help("Input file, if none is given stdin is used")
                    .required(false),
            )
            .arg(
                Arg::new("OUTPUT")
                    .value_name("OUTPUT")
                    .help("Output file")
                    .required(true),
            )
            .arg(
                Arg::new("force-create")
                    .short('f')
                    .long("force-create")
                    .help("Overwrite output files if they exist"),
            ),
        &compression_desc,
    );
    let clone_subcmd =
        add_input_archive_args(Command::new("clone").about(
            "Clone a remote (or local archive). The archive is unpacked while being cloned.",
        ))
        .arg(
            Arg::new("OUTPUT")
                .value_name("OUTPUT")
                .help("Output file")
                .required(true),
        )
        .arg(
            Arg::new("seed")
                .value_name("FILE")
                .long("seed")
                .help("File to use as seed while cloning or '-' to read from stdin")
                .multiple_occurrences(true),
        )
        .arg(
            Arg::new("seed-output")
                .long("seed-output")
                .help("Use the output file as seed and update in-place."),
        )
        .arg(
            Arg::new("force-create")
                .short('f')
                .long("force-create")
                .help("Overwrite output files if they exist"),
        )
        .arg(
            Arg::new("verify-output")
                .long("verify-output")
                .help("Vefify that the checksum of the output matches with the archive."),
        );
    let diff_subcmd = add_chunker_args(
        Command::new("diff")
            .about("Show the differential between two files.")
            .arg(
                Arg::new("A")
                    .value_name("FILE")
                    .help("Input file A")
                    .required(true),
            )
            .arg(
                Arg::new("B")
                    .value_name("FILE")
                    .help("Input file B")
                    .required(true),
            ),
        &compression_desc,
    );
    let mut cmd = Command::new(PKG_NAME)
    .version(PKG_VERSION)
    .arg_required_else_help(true)
    .arg(
        Arg::new("verbose")
            .short('v')
            .multiple_occurrences(true)
            .global(true)
            .help("Set verbosity level"),
    )
    .arg(Arg::new("buffered-chunks").long("buffered-chunks").value_name("COUNT").global(true).help(
        "Limit number of chunks processed simultaneously [default: cores available x 2]",
    ))
    .subcommand(compress_subcmd)
    .subcommand(clone_subcmd)
    .subcommand(
        Command::new("info")
            .about("Print archive details.")
            .arg(
                Arg::new("INPUT")
                    .value_name("INPUT")
                    .help("Input file (can be a local archive or a URL)")
                    .required(true),
            ),
    )
    .subcommand(diff_subcmd);
    let matches = cmd.try_get_matches_from_mut(args)?;

    let log_opts = LogOpts::new(match matches.occurrences_of("verbose") {
        0 => LevelFilter::Info,
        1 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    });

    let num_chunk_buffers: usize = if let Some(v) = matches.value_of("buffered-chunks") {
        v.parse()
            .map_err(|err| cmd.error(ErrorKind::ValueValidation, err))?
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
            let hash_length = hash_length
                .parse::<usize>()
                .map_err(|err| cmd.error(ErrorKind::ValueValidation, err))?;
            if !(4..=HashSum::MAX_LEN).contains(&hash_length) {
                return Err(cmd.error(
                    ErrorKind::ValueValidation,
                    format!("valid range is 4-{})", HashSum::MAX_LEN),
                ));
            }
            hash_length
        } else {
            HashSum::MAX_LEN
        };
        let chunker_config = parse_chunker_config(&mut cmd, matches)?;
        let compression = parse_compression(&mut cmd, matches)?;
        Ok((
            CommandOpts::Compress(compress_cmd::Options {
                input,
                output: output.to_path_buf(),
                hash_length,
                force_create: matches.is_present("force-create"),
                temp_file,
                chunker_config,
                compression,
                num_chunk_buffers,
            }),
            log_opts,
        ))
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
                hex_str_to_vec(c).map_err(|err| cmd.error(ErrorKind::ValueValidation, err))?,
            ))
        } else {
            None
        };
        let input_archive = parse_input_config(&mut cmd, matches)?;
        Ok((
            CommandOpts::Clone(clone_cmd::Options {
                input_archive,
                header_checksum,
                output: Path::new(output).to_path_buf(),
                force_create: matches.is_present("force-create"),
                seed_files,
                seed_stdin,
                verify_output: matches.is_present("verify-output"),
                seed_output,
                num_chunk_buffers,
            }),
            log_opts,
        ))
    } else if let Some(matches) = matches.subcommand_matches("info") {
        let input = matches.value_of("INPUT").unwrap();
        Ok((
            CommandOpts::Info {
                input: input.to_string(),
            },
            log_opts,
        ))
    } else if let Some(matches) = matches.subcommand_matches("diff") {
        let input_a = Path::new(matches.value_of("A").unwrap());
        let input_b = Path::new(matches.value_of("B").unwrap());
        let chunker_config = parse_chunker_config(&mut cmd, matches)?;
        let compression = parse_compression(&mut cmd, matches)?;
        Ok((
            CommandOpts::Diff(diff_cmd::Options {
                input_a: input_a.to_path_buf(),
                input_b: input_b.to_path_buf(),
                chunker_config,
                compression,
                num_chunk_buffers,
            }),
            log_opts,
        ))
    } else {
        Err(cmd.error(ErrorKind::InvalidSubcommand, ""))
    }
}

fn parse_chunker_opts(
    cmd: &mut Command<'_>,
    matches: &clap::ArgMatches,
    default_window_size: &str,
) -> Result<chunker::FilterConfig, clap::Error> {
    let avg_chunk_size = parse_size(matches.value_of("avg-chunk-size").unwrap_or("64KiB"))
        .map_err(|err| cmd.error(ErrorKind::ValueValidation, err))?;
    let min_chunk_size = parse_size(matches.value_of("min-chunk-size").unwrap_or("16KiB"))
        .map_err(|err| cmd.error(ErrorKind::ValueValidation, err))?;
    let max_chunk_size = parse_size(matches.value_of("max-chunk-size").unwrap_or("16MiB"))
        .map_err(|err| cmd.error(ErrorKind::ValueValidation, err))?;
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
    let window_size = parse_size(
        matches
            .value_of("rolling-window-size")
            .unwrap_or(default_window_size),
    )
    .map_err(|err| cmd.error(ErrorKind::ValueValidation, err))?;
    Ok(chunker::FilterConfig {
        filter_bits,
        min_chunk_size,
        max_chunk_size,
        window_size,
    })
}

fn parse_chunker_config(
    cmd: &mut Command<'_>,
    matches: &clap::ArgMatches,
) -> Result<chunker::Config, clap::Error> {
    Ok(
        match (
            matches.value_of("fixed-size"),
            matches
                .value_of("hash-chunking")
                .unwrap_or("RollSum")
                .to_lowercase()
                .as_ref(),
        ) {
            (Some(fixed_size), _) => chunker::Config::FixedSize(
                parse_size(fixed_size).map_err(|err| cmd.error(ErrorKind::ValueValidation, err))?,
            ),
            (_, "rollsum") => chunker::Config::RollSum(parse_chunker_opts(cmd, matches, "64B")?),
            (_, "buzhash") => chunker::Config::BuzHash(parse_chunker_opts(cmd, matches, "16B")?),
            _ => unreachable!(),
        },
    )
}

fn compression_names() -> String {
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

fn parse_compression(
    cmd: &mut Command<'_>,
    matches: &clap::ArgMatches,
) -> Result<Option<Compression>, clap::Error> {
    let compression_level = matches
        .value_of("compression-level")
        .unwrap_or("6")
        .parse()
        .map_err(|err| cmd.error(ErrorKind::ValueValidation, err))?;
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
            "brotli" => Some(
                Compression::brotli(compression_level)
                    .map_err(|err| cmd.error(ErrorKind::ValueValidation, err))?,
            ),
            "none" => None,
            _name => return Err(cmd.error(ErrorKind::ValueValidation, "Invalid compression")),
        },
    )
}

fn parse_size(size_str: &str) -> Result<usize, Box<dyn std::error::Error>> {
    let size_val: String = size_str.chars().filter(|a| a.is_numeric()).collect();
    let size_val: usize = size_val.parse()?;
    let size_unit: String = size_str.chars().filter(|a| !a.is_numeric()).collect();
    if size_unit.is_empty() {
        return Ok(size_val);
    }
    Ok(match size_unit.as_str() {
        "GiB" => 1024 * 1024 * 1024 * size_val,
        "MiB" => 1024 * 1024 * size_val,
        "KiB" => 1024 * size_val,
        "B" => size_val,
        unit => return Err(format!("Invalid unit ({})", unit).into()),
    })
}

fn parse_input_config(
    cmd: &mut Command<'_>,
    matches: &clap::ArgMatches,
) -> Result<clone_cmd::InputArchive, clap::Error> {
    let input = matches.value_of("INPUT").unwrap().to_string();

    let input_path = Path::new(&input);
    if input_path.exists() {
        return Ok(clone_cmd::InputArchive::Local(input.into()));
    }

    // Absolute windows paths, e.g. C:\temp.cba can be parsed as a URL. This
    // check prevents invalid paths from being used as a remote path.
    if Url::from_file_path(&input).is_ok() {
        return Err(cmd.error(
            ErrorKind::ValueValidation,
            format!("Input is not a valid path: {}", input_path.display()),
        ));
    }

    if let Ok(url) = input.parse::<Url>() {
        // Use as URL
        return Ok(clone_cmd::InputArchive::Remote(Box::new(
            clone_cmd::RemoteInput {
                url,
                retries: matches
                    .value_of("http-retry-count")
                    .unwrap_or("0")
                    .parse()
                    .map_err(|err| cmd.error(ErrorKind::ValueValidation, err))?,
                retry_delay: Duration::from_secs(
                    matches
                        .value_of("http-retry-delay")
                        .map(|v| v.parse())
                        .unwrap_or(Ok(0))
                        .map_err(|err| cmd.error(ErrorKind::ValueValidation, err))?,
                ),
                receive_timeout: if let Some(v) = matches.value_of("http-timeout") {
                    Some(Duration::from_secs(
                        v.parse()
                            .map_err(|err| cmd.error(ErrorKind::ValueValidation, err))?,
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
        format!("Input is not a valid local path or remote url: {}", input),
    ))
}

fn add_chunker_args<'a>(cmd: Command<'a>, compression_desc: &'a str) -> Command<'a> {
    cmd
        .arg(
            Arg::new("avg-chunk-size")
                .long("avg-chunk-size")
                .value_name("SIZE")
                .help("Indication of target chunk size [default: 64KiB]"),
        )
        .arg(
            Arg::new("min-chunk-size")
                .long("min-chunk-size")
                .value_name("SIZE")
                .help("Set minimal size of chunks [default: 16KiB]"),
        )
        .arg(
            Arg::new("max-chunk-size")
                .long("max-chunk-size")
                .value_name("SIZE")
                .help("Set maximal size of chunks [default: 16MiB]"),
        )
        .arg(
            Arg::new("hash-chunking")
                .long("hash-chunking")
                .value_name("HASH")
                .help("Set hash to use for chunking (RollSum/BuzHash). [default: RollSum]"),
        )
        .arg(
            Arg::new("rolling-window-size")
                .long("rolling-window-size")
                .value_name("SIZE")
                .help("Set size of the rolling hash window to use for chunking. [default: 64B for RollSum, 16B for BuzHash]")
        )
        .arg(
            Arg::new("fixed-size")
                .long("fixed-size")
                .value_name("SIZE")
                .help("Use fixed size chunking instead of rolling hash.")
                .conflicts_with("hash-chunking"),
        )
        .arg(
            Arg::new("compression-level")
                .long("compression-level")
                .value_name("LEVEL")
                .help("Set the chunk data compression level [default: 6]"),
        )
        .arg(
            Arg::new("compression")
                .long("compression")
                .value_name("TYPE")
                .help(compression_desc),
        ).arg(
            Arg::new("hash-length")
                .long("hash-length")
                .value_name("LENGTH")
                .help("Truncate the length of the stored chunk hash [default: 64]"),
        )
}

fn add_input_archive_args(cmd: Command<'_>) -> Command<'_> {
    cmd.arg(
        Arg::new("INPUT")
            .value_name("INPUT")
            .help("Input file (can be a local archive or a URL)")
            .required(true),
    )
    .arg(
        Arg::new("http-retry-count")
            .long("http-retry-count")
            .value_name("COUNT")
            .help("Retry transfer on failure [default: 0]"),
    )
    .arg(
        Arg::new("http-retry-delay")
            .long("http-retry-delay")
            .value_name("SECONDS")
            .help("Delay retry for some time on transfer failure [default: 0]"),
    )
    .arg(
        Arg::new("http-timeout")
            .long("http-timeout")
            .value_name("SECONDS")
            .help("Fail transfer if unresponsive for some time [default: None]"),
    )
    .arg(
        Arg::new("http-header")
            .long("http-header")
            .value_name("HEADER")
            .multiple_occurrences(true)
            .help("Provide custom http header"),
    )
    .arg(
        Arg::new("verify-header")
            .long("verify-header")
            .value_name("CHECKSUM")
            .help("Verify that the archive header checksum is the one given"),
    )
}

#[cfg(test)]
mod tests {

    use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
    use tempfile::NamedTempFile;

    use crate::clone_cmd::RemoteInput;

    use super::*;

    fn get_num_chunk_buffers() -> usize {
        match num_cpus::get() {
            0 | 1 => 1,
            n => n * 2,
        }
    }

    #[test]
    fn compress_command_default() {
        let (opts, log) = parse_opts(&[
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
            })
        );
    }

    #[test]
    fn compress_command_stdin() {
        let (opts, log) =
            parse_opts(&["bita", "compress", "./output.cba"]).unwrap_or_else(|e| panic!("{}", e));
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
            })
        );
    }

    #[test]
    fn compress_command_specific() {
        let (opts, log) = parse_opts(&[
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
            })
        );
    }

    #[test]
    fn clone_command_local_archive() {
        let input = NamedTempFile::new().unwrap();
        let input_path = input.path();
        let mut temp_file_path = input_path.to_path_buf();
        temp_file_path.set_extension(".tmp");

        let (opts, log) = parse_opts(&[
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

        let (opts, log) = parse_opts(&[
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

        let (opts, log) = parse_opts(&[
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
                header_checksum:  Some(HashSum::from(hex_str_to_vec("5520529d1175327f9a39df0a75fe6bd314f9e6bedd89734c508a043c66066c7ada2a7b493659794f916840d976e9f0b10ec94a09caec0296ced9666998ec7977").unwrap())),
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
        let (opts, log) = parse_opts(&[
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
                input_archive: clone_cmd::InputArchive::Remote(Box::new(RemoteInput {
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
        let (opts, log) = parse_opts(&[
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
                input_archive: clone_cmd::InputArchive::Remote(Box::new(RemoteInput {
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
        let (info, log) = parse_opts(&["bita", "info", "-v", "an-input-file.cba"])
            .unwrap_or_else(|e| panic!("{}", e));
        assert_eq!(log, LogOpts::new(LevelFilter::Debug));
        assert_eq!(
            info,
            CommandOpts::Info {
                input: "an-input-file.cba".to_owned()
            }
        );
    }

    #[test]
    fn info_command_missing_input() {
        parse_opts(&["bita", "info"]).unwrap_err();
    }

    #[test]
    fn diff_command() {
        let (opts, log) =
            parse_opts(&["bita", "diff", "file1", "file2"]).unwrap_or_else(|e| panic!("{}", e));
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
