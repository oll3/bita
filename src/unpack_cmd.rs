use crate::buzhash::BuzHash;
use atty::Stream;
use blake2::{Blake2b, Digest};
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::io::SeekFrom;
use threadpool::ThreadPool;

use crate::archive_reader::*;
use crate::chunker::Chunker;
use crate::chunker_utils::*;
use crate::config::*;
use crate::errors::*;
use crate::remote_archive_backend::RemoteReader;
use crate::string_utils::*;
use std::io::BufWriter;
use std::os::linux::fs::MetadataExt;

fn chunk_seed<T, F>(
    mut seed_input: T,
    mut chunker: Chunker,
    hash_length: usize,
    chunk_hash_set: &mut HashSet<HashBuf>,
    mut result: F,
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
            chunker.preload(&archive_header);

            // If input is an archive also check if chunker parameter
            // matches, otherwise error or warn user?
            // Generate strong hash for a chunk
            let hasher = |data: &[u8]| {
                let mut hasher = Blake2b::new();
                hasher.input(data);
                hasher.result().to_vec()
            };
            unique_chunks(
                &mut seed_input,
                &mut chunker,
                hasher,
                &pool,
                false,
                |hashed_chunk| {
                    let hash = &hashed_chunk.hash[0..hash_length].to_vec();
                    if chunk_hash_set.contains(hash) {
                        result(hash, &hashed_chunk.data);
                        chunk_hash_set.remove(hash);
                    }
                },
            )
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
                seed_input,
                current_input_offset as u64,
                &chunk_hash_set.clone(),
                |chunk_descriptor, chunk_data| {
                    // Got chunk data for a matching chunk
                    result(&chunk_descriptor.checksum, &chunk_data);
                    chunk_hash_set.remove(&chunk_descriptor.checksum);
                    Ok(())
                },
            )?;

            Ok(())
        }
    }
}

fn unpack_input<T>(mut input: T, config: &UnpackConfig, pool: &ThreadPool) -> Result<()>
where
    T: ArchiveBackend,
{
    let archive = ArchiveReader::try_init(&mut input, &mut Vec::new())?;
    let mut chunks_left = archive.chunk_hash_set();

    println!("Unpacking archive ({})", archive);

    // Create or open output file.
    let mut output_file = OpenOptions::new()
        .write(true)
        .create(config.base.force_create)
        .create_new(!config.base.force_create)
        .open(&config.output)
        .chain_err(|| format!("failed to open output file ({})", config.output))?;

    // Check if the given output file is a regular file or block device.
    // If it is a block device we should check its size against the target size before
    // writing. If a regular file then resize that file to target size.
    let meta = output_file
        .metadata()
        .chain_err(|| "unable to get file meta data")?;
    if meta.st_mode() & 0x6000 == 0x6000 {
        // Output is a block device
        let size = output_file
            .seek(SeekFrom::End(0))
            .chain_err(|| "unable to seek output file")?;
        if size != archive.source_total_size {
            panic!(
                "Size of output ({}) differ from size of archive target file ({})",
                size_to_str(size),
                size_to_str(archive.source_total_size)
            );
        }
        output_file
            .seek(SeekFrom::Start(0))
            .chain_err(|| "unable to seek output file")?;
    } else {
        // Output is a reqular file
        output_file
            .set_len(archive.source_total_size)
            .chain_err(|| "unable to resize output file")?;
    }

    let mut output_file = BufWriter::new(output_file);

    // Setup chunker to use when chunking seed input
    let chunker = Chunker::new(
        1024 * 1024,
        archive.chunk_filter_bits,
        archive.min_chunk_size,
        archive.max_chunk_size,
        BuzHash::new(archive.hash_window_size as usize, crate::BUZHASH_SEED),
    );

    let mut total_read_from_seed = 0;
    let mut total_from_archive = 0;

    {
        // Closure for writing chunks to output
        let mut chunk_output = |hash: &HashBuf, chunk_data: &[u8]| {
            println!(
                "Chunk '{}', size {} used from seed",
                HexSlice::new(hash),
                size_to_str(chunk_data.len()),
            );
            total_read_from_seed += chunk_data.len();
            for offset in archive.chunk_source_offsets(hash) {
                output_file
                    .seek(SeekFrom::Start(offset as u64))
                    .expect("seek output");
                output_file.write_all(&chunk_data).expect("write output");
            }
        };

        // Run input seed files through chunker and use chunks which are in the target file.
        // Start with scanning stdin, if not a tty.
        if !atty::is(Stream::Stdin) {
            let stdin = io::stdin();
            let seed_file = stdin.lock();
            println!("Scanning stdin for chunks...");
            chunk_seed(
                seed_file,
                chunker.clone(),
                archive.hash_length,
                &mut chunks_left,
                &mut chunk_output,
                &pool,
            )?;
            println!(
                "Reached end of stdin ({} chunks missing)",
                chunks_left.len()
            );
        }
        // Now scan through all given seed files
        for seed in &config.seed_files {
            if !chunks_left.is_empty() {
                let seed_file = File::open(&seed)
                    .chain_err(|| format!("failed to open seed file ({})", seed))?;
                println!("Scanning {} for chunks...", seed);
                chunk_seed(
                    seed_file,
                    chunker.clone(),
                    archive.hash_length,
                    &mut chunks_left,
                    &mut chunk_output,
                    &pool,
                )?;
                println!(
                    "Reached end of {} ({} chunks missing)",
                    seed,
                    chunks_left.len()
                );
            }
        }
    }

    // Fetch rest of the chunks from archive
    let archive_total_read =
        archive.read_chunk_data(input, &chunks_left, |chunk_descriptor, chunk_data| {
            let offsets = &archive.chunk_source_offsets(&chunk_descriptor.checksum);
            for offset in offsets {
                total_from_archive += chunk_data.len();
                output_file
                    .seek(SeekFrom::Start(*offset as u64))
                    .chain_err(|| "failed to seek output file")?;
                output_file
                    .write_all(chunk_data)
                    .chain_err(|| "failed to write output file")?;
            }

            println!(
                "Chunk '{}', size {}, decompressed to {}, insert at {:?}",
                HexSlice::new(&chunk_descriptor.checksum),
                size_to_str(chunk_descriptor.archive_size),
                size_to_str(chunk_data.len() * offsets.len()),
                offsets
            );

            Ok(())
        })?;

    println!(
        "Unpacked using {} from seed and {} from archive.",
        size_to_str(total_read_from_seed),
        size_to_str(archive_total_read)
    );

    Ok(())
}

pub fn run(config: &UnpackConfig, pool: &ThreadPool) -> Result<()> {
    if &config.input[0..7] == "http://" || &config.input[0..8] == "https://" {
        let remote_source = RemoteReader::new(&config.input);
        unpack_input(remote_source, config, pool)?;
    } else {
        let local_file =
            File::open(&config.input).chain_err(|| format!("unable to open {}", config.input))?;
        unpack_input(local_file, config, pool)?;
    }

    Ok(())
}
