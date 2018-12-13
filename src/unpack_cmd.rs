use atty::Stream;
use blake2::{Blake2b, Digest};
use buzhash::BuzHash;
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::io::SeekFrom;
use threadpool::ThreadPool;

use archive_reader::*;
use chunker::Chunker;
use chunker_utils::*;
use config::*;
use errors::*;
use remote_archive_backend::RemoteReader;
use std::io::BufWriter;
use std::os::linux::fs::MetadataExt;
use string_utils::*;

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
    F: FnMut(&HashBuf, &Vec<u8>),
{
    // Read chunks from seed file.
    // TODO: Should first check if input might be a archive file and then use its chunks as is.
    // If input is an archive also check if chunker parameter
    // matches, otherwise error or warn user?
    // Generate strong hash for a chunk
    let hasher = |data: &[u8]| {
        let mut hasher = Blake2b::new();
        hasher.input(data);
        hasher.result().to_vec()
    };
    let mut total_contained = 0;
    unique_chunks(
        &mut seed_input,
        &mut chunker,
        hasher,
        &pool,
        false,
        |hashed_chunk| {
            let hash = &hashed_chunk.hash[0..hash_length].to_vec();
            if chunk_hash_set.contains(hash) {
                total_contained += hashed_chunk.data.len();
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

fn unpack_input<T>(
    mut archive: ArchiveReader<T>,
    config: &UnpackConfig,
    pool: &ThreadPool,
) -> Result<()>
where
    T: ArchiveBackend,
{
    let mut chunks_left = archive.chunk_hash_set();

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
        BuzHash::new(archive.hash_window_size as usize, ::BUZHASH_SEED),
    );

    let mut total_read_from_seed = 0;
    let mut total_from_archive = 0;

    {
        // Closure for writing chunks to output
        let mut chunk_output = |hash: &HashBuf, chunk_data: &Vec<u8>| {
            println!(
                "Chunk '{}', size {} read from seed stdin",
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
    archive.read_chunk_data(&chunks_left, |chunk| {
        total_from_archive += chunk.data.len();
        output_file
            .seek(SeekFrom::Start(chunk.offset as u64))
            .chain_err(|| "failed to seek output file")?;
        output_file
            .write_all(&chunk.data)
            .chain_err(|| "failed to write output file")?;

        Ok(())
    })?;

    println!(
        "Unpacked using {} from seed and {} from archive.",
        size_to_str(total_read_from_seed),
        size_to_str(archive.total_read)
    );

    Ok(())
}

pub fn run(config: &UnpackConfig, pool: &ThreadPool) -> Result<()> {
    if &config.input[0..7] == "http://" || &config.input[0..8] == "https://" {
        let remote_source = RemoteReader::new(&config.input);
        let archive = ArchiveReader::init(remote_source)?;
        unpack_input(archive, config, pool)?;
    } else {
        let local_file =
            File::open(&config.input).chain_err(|| format!("unable to open {}", config.input))?;
        let archive = ArchiveReader::init(local_file)?;
        unpack_input(archive, config, pool)?;
    }

    Ok(())
}
