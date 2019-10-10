use atty::Stream;
use blake2::{Blake2b, Digest};
use log::*;
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::io::BufWriter;
use std::io::SeekFrom;
use std::path::PathBuf;
use threadpool::ThreadPool;

use crate::config;
use crate::info_cmd;
use bita::archive_reader::{ArchiveBackend, ArchiveReader};
use bita::chunker::{Chunker, ChunkerParams};
use bita::chunker_utils::*;
use bita::error::Error;
use bita::remote_archive_backend::RemoteReader;
use bita::string_utils::*;

fn chunk_seed<T, F>(
    mut seed_input: T,
    chunker_params: &ChunkerParams,
    hash_length: usize,
    chunk_hash_set: &mut HashSet<HashBuf>,
    mut chunk_callback: F,
    pool: &ThreadPool,
) -> Result<(), Error>
where
    T: Read,
    F: FnMut(&HashBuf, &[u8]),
{
    // As the input file was not an archive we feed the data read so
    // far into the chunker.
    let mut chunker = Chunker::new(chunker_params.clone(), &mut seed_input);

    // If input is an archive also check if chunker parameter
    // matches, otherwise error or warn user?
    // Generate strong hash for a chunk
    let hasher = |data: &[u8]| {
        let mut hasher = Blake2b::new();
        hasher.input(data);
        hasher.result().to_vec()
    };
    unique_chunks(&mut chunker, hasher, &pool, false, |hashed_chunk| {
        let hash = &hashed_chunk.hash[0..hash_length].to_vec();
        if chunk_hash_set.contains(hash) {
            chunk_callback(hash, &hashed_chunk.data);
            chunk_hash_set.remove(hash);
        }
    })?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn clone_to_output<T, F>(
    pool: &ThreadPool,
    archive_backend: T,
    archive: &ArchiveReader,
    seed_files: &[PathBuf],
    seed_stdin: bool,
    chunker_params: ChunkerParams,
    mut chunks_left: HashSet<HashBuf>,
    mut chunk_output: F,
) -> Result<(), Error>
where
    T: ArchiveBackend,
    F: FnMut(&str, &HashBuf, &[u8]),
{
    let mut total_read_from_seed = 0;

    // Run input seed files through chunker and use chunks which are in the target file.
    // Start with scanning stdin, if not a tty.
    if seed_stdin && !atty::is(Stream::Stdin) {
        let stdin = io::stdin();
        let chunks_missing = chunks_left.len();
        let stdin = stdin.lock();
        info!("Scanning stdin for chunks...");
        chunk_seed(
            stdin,
            &chunker_params,
            archive.hash_length,
            &mut chunks_left,
            |checksum, chunk_data| {
                total_read_from_seed += chunk_data.len();
                chunk_output("seed (stdin)", checksum, chunk_data);
            },
            &pool,
        )?;
        info!(
            "Used {} chunks from stdin",
            chunks_missing - chunks_left.len()
        );
    }
    // Now scan through all given seed files
    for seed_path in seed_files {
        if !chunks_left.is_empty() {
            let chunks_missing = chunks_left.len();
            let seed_file = File::open(&seed_path).map_err(|e| {
                (
                    format!("failed to open seed file ({})", seed_path.display()),
                    e,
                )
            })?;
            info!("Scanning {} for chunks...", seed_path.display());
            chunk_seed(
                seed_file,
                &chunker_params,
                archive.hash_length,
                &mut chunks_left,
                |checksum, chunk_data| {
                    total_read_from_seed += chunk_data.len();
                    chunk_output(
                        &format!("seed ({})", seed_path.display()),
                        checksum,
                        chunk_data,
                    );
                },
                &pool,
            )?;
            info!(
                "Used {} chunks from seed file {}",
                chunks_missing - chunks_left.len(),
                seed_path.display(),
            );
        }
    }

    // Fetch rest of the chunks from archive
    let total_from_archive = archive.read_chunk_data(
        &pool,
        archive_backend,
        &chunks_left,
        |checksum, chunk_data| {
            chunk_output("archive", &checksum, chunk_data);
            Ok(())
        },
    )?;

    info!(
        "Successfully cloned archive using {} from remote and {} from seeds.",
        size_to_str(total_from_archive),
        size_to_str(total_read_from_seed)
    );

    Ok(())
}

fn prepare_unpack_output(output_file: &mut File, source_file_size: u64) -> Result<(), Error> {
    #[cfg(unix)]
    {
        use std::os::linux::fs::MetadataExt;
        let meta = output_file
            .metadata()
            .map_err(|e| ("unable to get file meta data", e))?;
        if meta.st_mode() & 0x6000 == 0x6000 {
            // Output is a block device
            let size = output_file
                .seek(SeekFrom::End(0))
                .map_err(|e| ("unable to seek output file", e))?;
            if size != source_file_size {
                panic!(
                    "Size of output device ({}) differ from size of archive target file ({})",
                    size_to_str(size),
                    size_to_str(source_file_size)
                );
            }
            output_file
                .seek(SeekFrom::Start(0))
                .map_err(|e| ("unable to seek output file", e))?;
        } else {
            // Output is a reqular file
            output_file
                .set_len(source_file_size)
                .map_err(|e| ("unable to resize output file", e))?;
        }
    }
    #[cfg(not(unix))]
    {
        output_file
            .set_len(source_file_size)
            .map_err(|e| ("unable to resize output file", e))?;
    }
    Ok(())
}

fn clone_archive<T>(
    mut archive_backend: T,
    config: &config::CloneConfig,
    pool: &ThreadPool,
) -> Result<(), Error>
where
    T: ArchiveBackend,
{
    let archive = ArchiveReader::try_init(&mut archive_backend, &mut Vec::new())?;
    let chunks_left = archive.chunk_hash_set();

    info_cmd::print_archive(&archive);
    println!();

    // Verify the header checksum if requested
    if let Some(ref expected_checksum) = config.header_checksum {
        if *expected_checksum != archive.header_checksum {
            return Err(Error::ChecksumMismatch(
                "Header checksum mismatch!".to_owned(),
            ));
        } else {
            info!("Header checksum verified OK");
        }
    }
    info!(
        "Cloning archive {} to {}...",
        config.input,
        config.output.display()
    );

    // Setup chunker to use when chunking seed input
    let chunker_params = archive.chunker_params.clone();

    // Create or open output file.
    let mut output_file = OpenOptions::new()
        .write(true)
        .create(config.force_create)
        .create_new(!config.force_create)
        .open(&config.output)
        .map_err(|e| {
            (
                format!("failed to open output file ({})", config.output.display()),
                e,
            )
        })?;

    // Clone and unpack archive

    // Check if the given output file is a regular file or block device.
    // If it is a block device we should check its size against the target size before
    // writing. If a regular file then resize that file to target size.
    prepare_unpack_output(&mut output_file, archive.source_total_size)?;

    let mut output_file = BufWriter::new(output_file);
    clone_to_output(
        pool,
        archive_backend,
        &archive,
        &config.seed_files,
        config.seed_stdin,
        chunker_params,
        chunks_left,
        |chunk_source: &str, hash: &HashBuf, chunk_data: &[u8]| {
            debug!(
                "Chunk '{}', size {} used from {}",
                HexSlice::new(hash),
                size_to_str(chunk_data.len()),
                chunk_source,
            );

            for offset in &archive.chunk_source_offsets(hash) {
                output_file
                    .seek(SeekFrom::Start(*offset))
                    .expect("seek output");
                output_file.write_all(chunk_data).expect("write output");
            }
        },
    )?;

    Ok(())
}

pub fn run(config: &config::CloneConfig) -> Result<(), Error> {
    let pool = ThreadPool::new(num_cpus::get());
    if &config.input[0..7] == "http://" || &config.input[0..8] == "https://" {
        let remote_source = RemoteReader::new(&config.input);
        clone_archive(remote_source, config, &pool)?;
    } else {
        let local_file = File::open(&config.input)
            .map_err(|e| (format!("unable to open {}", config.input), e))?;
        clone_archive(local_file, config, &pool)?;
    }

    Ok(())
}
