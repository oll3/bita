use atty::Stream;
use blake2::{Blake2b, Digest};
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::io::BufWriter;
use std::io::SeekFrom;
use std::os::linux::fs::MetadataExt;
use std::path::PathBuf;
use threadpool::ThreadPool;

use crate::archive_reader::*;
use crate::buzhash::BuzHash;
use crate::chunker::{Chunker, ChunkerParams};
use crate::chunker_utils::*;
use crate::config;
use crate::errors::*;
use crate::remote_archive_backend::RemoteReader;
use crate::string_utils::*;
use crate::{clone_output, clone_output::CloneOutput};

fn chunk_seed<T, F>(
    mut seed_input: T,
    chunker_params: &ChunkerParams,
    hash_length: usize,
    chunk_hash_set: &mut HashSet<HashBuf>,
    mut chunk_callback: F,
    pool: &ThreadPool,
) -> Result<()>
where
    T: Read,
    F: FnMut(&HashBuf, &[u8]),
{
    // Test if the given seed can be read as a bita archive.
    let mut archive_header = Vec::new();
    match ArchiveReader::try_init(&mut seed_input, &mut archive_header) {
        Err(Error(ErrorKind::NotAnArchive(_), _)) => {
            // As the input file was not an archive we feed the data read so
            // far into the chunker.
            let mut chunker = Chunker::new(chunker_params.clone(), &mut seed_input);
            chunker.preload(&archive_header);

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
            })
            .chain_err(|| "failed to get unique chunks")?;

            println!(
                "Chunker - scan time: {}.{:03} s, read time: {}.{:03} s",
                chunker.scan_time.as_secs(),
                chunker.scan_time.subsec_millis(),
                chunker.read_time.as_secs(),
                chunker.read_time.subsec_millis()
            );
            Ok(())
        }
        Err(err) => Err(err),
        Ok(ref mut archive) => {
            // Is an archive
            let current_input_offset = archive_header.len();

            archive.read_chunk_stream(
                &pool,
                seed_input,
                current_input_offset as u64,
                &chunk_hash_set.clone(),
                |checksum, chunk_data| {
                    // Got chunk data for a matching chunk
                    chunk_callback(&checksum, &chunk_data);
                    chunk_hash_set.remove(&checksum);
                    Ok(())
                },
            )?;

            Ok(())
        }
    }
}

fn clone_to_output<T, F>(
    pool: &ThreadPool,
    archive_backend: T,
    archive: &ArchiveReader,
    seed_files: &[PathBuf],
    chunker_params: ChunkerParams,
    mut chunks_left: HashSet<HashBuf>,
    mut chunk_output: F,
) -> Result<()>
where
    T: ArchiveBackend,
    F: FnMut(&str, &HashBuf, &[u8]),
{
    let mut total_read_from_seed = 0;

    // Run input seed files through chunker and use chunks which are in the target file.
    // Start with scanning stdin, if not a tty.
    if !atty::is(Stream::Stdin) {
        let stdin = io::stdin();
        let chunks_missing = chunks_left.len();
        let stdin = stdin.lock();
        println!("Scanning stdin for chunks...");
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
        println!(
            "Used {} chunks from stdin",
            chunks_missing - chunks_left.len()
        );
    }
    // Now scan through all given seed files
    for seed_path in seed_files {
        if !chunks_left.is_empty() {
            let chunks_missing = chunks_left.len();
            let seed_file = File::open(&seed_path)
                .chain_err(|| format!("failed to open seed file ({})", seed_path.display()))?;
            println!("Scanning {} for chunks...", seed_path.display());
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
            println!(
                "Used {} chunks from seed file {}",
                chunks_missing - chunks_left.len(),
                seed_path.display(),
            );
        }
    }

    let mut total_from_archive = 0;
    // Fetch rest of the chunks from archive
    archive.read_chunk_data(
        &pool,
        archive_backend,
        &chunks_left,
        |checksum, chunk_data| {
            total_from_archive += chunk_data.len();
            chunk_output("archive", &checksum, chunk_data);
            Ok(())
        },
    )?;

    println!(
        "Cloned using {} from seed and {} from archive.",
        size_to_str(total_read_from_seed),
        size_to_str(total_from_archive)
    );

    Ok(())
}

fn prepare_unpack_output(output_file: &mut File, source_file_size: u64) -> Result<()> {
    let meta = output_file
        .metadata()
        .chain_err(|| "unable to get file meta data")?;
    if meta.st_mode() & 0x6000 == 0x6000 {
        // Output is a block device
        let size = output_file
            .seek(SeekFrom::End(0))
            .chain_err(|| "unable to seek output file")?;
        if size != source_file_size {
            panic!(
                "Size of output device ({}) differ from size of archive target file ({})",
                size_to_str(size),
                size_to_str(source_file_size)
            );
        }
        output_file
            .seek(SeekFrom::Start(0))
            .chain_err(|| "unable to seek output file")?;
    } else {
        // Output is a reqular file
        output_file
            .set_len(source_file_size)
            .chain_err(|| "unable to resize output file")?;
    }
    Ok(())
}

fn clone_archive<T>(
    mut archive_backend: T,
    config: &config::CloneConfig,
    pool: &ThreadPool,
) -> Result<()>
where
    T: ArchiveBackend,
{
    let archive = ArchiveReader::try_init(&mut archive_backend, &mut Vec::new())?;
    let chunks_left = archive.chunk_hash_set();

    println!("Cloning archive ({})", archive);

    // Setup chunker to use when chunking seed input
    let chunker_params = ChunkerParams::new(
        archive.chunk_filter_bits,
        archive.min_chunk_size,
        archive.max_chunk_size,
        BuzHash::new(archive.hash_window_size as usize, crate::BUZHASH_SEED),
    );

    // Create or open output file.
    let mut output_file = OpenOptions::new()
        .write(true)
        .create(config.base.force_create)
        .create_new(!config.base.force_create)
        .open(match config.output {
            config::CloneOutput::Unpack(ref path) => path,
            config::CloneOutput::Archive(ref path) => path,
        })
        .chain_err(|| "failed to open output file")?;

    match config.output {
        config::CloneOutput::Unpack(_) => {
            // Clone and unpack archive

            // Check if the given output file is a regular file or block device.
            // If it is a block device we should check its size against the target size before
            // writing. If a regular file then resize that file to target size.
            prepare_unpack_output(&mut output_file, archive.source_total_size)?;

            let mut output = BufWriter::new(output_file);
            clone_to_output(
                pool,
                archive_backend,
                &archive,
                &config.seed_files,
                chunker_params,
                chunks_left,
                |chunk_source: &str, hash: &HashBuf, chunk_data: &[u8]| {
                    println!(
                        "Chunk '{}', size {} used from {}",
                        HexSlice::new(hash),
                        size_to_str(chunk_data.len()),
                        chunk_source,
                    );
                    output
                        .write_chunk(hash, &archive.chunk_source_offsets(hash), chunk_data)
                        .expect("write chunk");
                },
            )?;
        }
        config::CloneOutput::Archive(_) => {
            // Clone to archive
            let mut output =
                clone_output::ArchiveOutput::new(output_file, &archive, chunker_params.clone());

            clone_to_output(
                pool,
                archive_backend,
                &archive,
                &config.seed_files,
                chunker_params,
                chunks_left,
                |chunk_source: &str, hash: &HashBuf, chunk_data: &[u8]| {
                    println!(
                        "Chunk '{}', size {} used from {}",
                        HexSlice::new(hash),
                        size_to_str(chunk_data.len()),
                        chunk_source,
                    );
                    output
                        .write_chunk(hash, &archive.chunk_source_offsets(hash), chunk_data)
                        .expect("write chunk");
                },
            )?;

            // Write the header
            output.write_header()?;
        }
    }

    Ok(())
}

pub fn run(config: &config::CloneConfig, pool: &ThreadPool) -> Result<()> {
    if &config.input[0..7] == "http://" || &config.input[0..8] == "https://" {
        let remote_source = RemoteReader::new(&config.input);
        clone_archive(remote_source, config, pool)?;
    } else {
        let local_file =
            File::open(&config.input).chain_err(|| format!("unable to open {}", config.input))?;
        clone_archive(local_file, config, pool)?;
    }

    Ok(())
}
