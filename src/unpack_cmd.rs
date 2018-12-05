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
use remote_reader::RemoteReader;
use std::io::BufWriter;
use std::os::linux::fs::MetadataExt;
use string_utils::*;

impl ArchiveBackend for File {
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        self.seek(SeekFrom::Start(offset))?;
        self.read_exact(buf)?;
        Ok(())
    }
    fn read_in_chunks<F: FnMut(Vec<u8>)>(
        &mut self,
        start_offset: u64,
        chunk_sizes: &Vec<u64>,
        mut chunk_callback: F,
    ) -> io::Result<()> {
        self.seek(SeekFrom::Start(start_offset))?;
        for chunk_size in chunk_sizes {
            let mut buf: Vec<u8> = vec![0; *chunk_size as usize];
            self.read_exact(&mut buf[..])?;
            chunk_callback(buf);
        }
        Ok(())
    }
}

fn chunk_seed<T, F>(
    mut seed_input: T,
    mut chunker: Chunker,
    hash_length: usize,
    chunk_hash_set: &mut HashSet<HashBuf>,
    mut result: F,
    pool: &ThreadPool,
) where
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
    ).expect("compress chunks");

    println!(
        "Chunker - scan time: {}.{:03} s, read time: {}.{:03} s",
        chunker.scan_time.as_secs(),
        chunker.scan_time.subsec_millis(),
        chunker.read_time.as_secs(),
        chunker.read_time.subsec_millis()
    );
}

fn unpack_input<T>(mut archive: ArchiveReader<T>, config: UnpackConfig, pool: ThreadPool)
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
        .expect(&format!("failed to open output file ({})", config.output));

    // Check if the given output file is a regular file or block device.
    // If it is a block device we should check its size against the target size before
    // writing. If a regular file then resize that file to target size.
    let meta = output_file.metadata().expect("meta");
    if meta.st_mode() & 0x6000 == 0x6000 {
        // Output is a block device
        let size = output_file.seek(SeekFrom::End(0)).expect("seek size");
        if size != archive.source_total_size {
            panic!(
                "Size of output ({}) differ from size of archive target file ({})",
                size_to_str(size),
                size_to_str(archive.source_total_size)
            );
        }
        output_file.seek(SeekFrom::Start(0)).expect("seek reset");
    } else {
        // Output is a reqular file
        output_file
            .set_len(archive.source_total_size)
            .expect("resize output file");
    }

    let mut output_file = BufWriter::new(output_file);

    // Setup chunker to use when chunking seed input
    let chunker = Chunker::new(
        1024 * 1024,
        archive.chunk_filter_bits,
        archive.min_chunk_size,
        archive.max_chunk_size,
        BuzHash::new(archive.hash_window_size as usize, 0x10324195),
    );

    let mut total_read_from_seed = 0;
    let mut total_from_archive = 0;

    for seed in config.seed_files {
        let seed_file = File::open(&seed).expect(&format!("failed to open file ({})", seed));

        println!("{} chunks missing. Search in {}.", chunks_left.len(), seed);
        chunk_seed(
            seed_file,
            chunker.clone(),
            archive.hash_length,
            &mut chunks_left,
            |hash, chunk_data| {
                // Got chunk
                println!(
                    "Chunk '{}', size {} read from seed {}",
                    HexSlice::new(hash),
                    size_to_str(chunk_data.len()),
                    seed,
                );

                total_read_from_seed += chunk_data.len();

                for offset in archive.chunk_source_offsets(hash) {
                    output_file
                        .seek(SeekFrom::Start(offset as u64))
                        .expect("seek output");
                    output_file.write_all(&chunk_data).expect("write output");
                }
            },
            &pool,
        );

        if chunks_left.len() == 0 {
            println!("All chunks was found in seed.");
            break;
        }
        println!("{} chunks still missing.", chunks_left.len());
    }

    archive.read_chunk_data(&chunks_left, |chunk| {
        total_from_archive += chunk.data.len();
        output_file
            .seek(SeekFrom::Start(chunk.offset as u64))
            .expect("seek output");
        output_file.write_all(&chunk.data).expect("write output");
    });

    println!(
        "Unpacked using {} from seed and {} from archive.",
        size_to_str(total_read_from_seed),
        size_to_str(archive.total_read)
    );
}

pub fn run(config: UnpackConfig, pool: ThreadPool) {
    println!("Do unpack ({:?})", config);

    if &config.input[0..7] == "http://" || &config.input[0..8] == "https://" {
        println!("Using remote reader");
        let remote_source = RemoteReader::new(&config.input);
        let archive = ArchiveReader::new(remote_source);
        unpack_input(archive, config, pool);
    } else {
        println!("Using file reader");
        let local_file =
            File::open(&config.input).expect(&format!("failed to open file ({})", config.input));
        let archive = ArchiveReader::new(local_file);
        unpack_input(archive, config, pool);
    }
}
