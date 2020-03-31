use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use log::*;
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs::File;

use crate::info_cmd;
use crate::output::Output;
use crate::seed_input::SeedInput;
use crate::string_utils::*;
use bitar::archive_reader::ArchiveReader;
use bitar::chunk_index::{ChunkIndex, ReorderOp};
use bitar::Error;
use bitar::HashSum;
use bitar::{ReaderBackend, ReaderBackendLocal, ReaderBackendRemote};

async fn update_in_place(
    output: &mut Output,
    output_index: &ChunkIndex,
    chunks_left: &mut ChunkIndex,
) -> Result<u64, Error> {
    let mut total_read: u64 = 0;
    let chunks_before = chunks_left.len();
    let now = std::time::Instant::now();
    let reorder_ops = output_index.reorder_ops(chunks_left);
    debug!(
        "Built reordering ops ({}) in {} ms",
        reorder_ops.len(),
        now.elapsed().as_secs_f64() * 1000.0
    );

    // Move chunks around internally in the output file
    let mut temp_store: HashMap<&HashSum, Vec<u8>> = HashMap::new();
    for op in &reorder_ops {
        match op {
            ReorderOp::Copy { hash, source, dest } => {
                let buf = if let Some(buf) = temp_store.remove(hash) {
                    buf
                } else {
                    let mut buf: Vec<u8> = Vec::new();
                    buf.resize(source.size, 0);
                    output
                        .seek_read(source.offset, &mut buf[..])
                        .await
                        .expect("error reading output");
                    buf
                };
                for &offset in dest {
                    output
                        .seek_write(offset, &buf[..])
                        .await
                        .expect("error writing output");
                    total_read += source.size as u64;
                }
                chunks_left.remove(hash);
            }
            ReorderOp::StoreInMem { hash, source } => {
                if !temp_store.contains_key(hash) {
                    let mut buf: Vec<u8> = Vec::new();
                    buf.resize(source.size, 0);
                    output
                        .seek_read(source.offset, &mut buf[..])
                        .await
                        .expect("error reading output");
                    temp_store.insert(hash, buf);
                }
            }
        }
    }
    info!(
        "Reorganized {} chunks ({}) in output",
        chunks_before - chunks_left.len(),
        size_to_str(total_read),
    );
    Ok(total_read)
}

async fn finish_using_archive(
    reader_backend: &mut dyn ReaderBackend,
    archive: &ArchiveReader,
    chunks_left: ChunkIndex,
    output: &mut Output,
    num_chunk_buffers: usize,
) -> Result<u64, Error> {
    let mut total_written = 0u64;
    let mut total_fetched = 0u64;
    let grouped_chunks = archive.grouped_chunks(&chunks_left);
    for group in grouped_chunks {
        // For each group of chunks
        let start_offset = archive.chunk_data_offset() + group[0].archive_offset;
        let compression = archive.chunk_compression();
        let archive_chunk_stream = reader_backend
            .read_chunks(
                start_offset,
                group.iter().map(|c| c.archive_size as usize).collect(),
            )
            .enumerate()
            .map(|(chunk_index, compressed_chunk)| {
                let compressed_chunk = compressed_chunk.expect("failed to read archive");
                let chunk_checksum = group[chunk_index].checksum.clone();
                let chunk_source_size = group[chunk_index].source_size as usize;
                total_fetched += compressed_chunk.len() as u64;
                tokio::task::spawn(async move {
                    (
                        chunk_checksum.clone(),
                        ArchiveReader::decompress_and_verify(
                            compression,
                            &chunk_checksum,
                            chunk_source_size,
                            compressed_chunk,
                        )
                        .expect("failed to decompress chunk"),
                    )
                })
            })
            .buffered(num_chunk_buffers);

        pin_mut!(archive_chunk_stream);
        while let Some(result) = archive_chunk_stream.next().await {
            // For each chunk read from archive
            let (hash, chunk) = result.expect("spawn error");
            debug!(
                "Chunk '{}', size {} used from archive",
                hash,
                size_to_str(chunk.len()),
            );
            for offset in archive
                .source_index()
                .offsets(&hash)
                .unwrap_or_else(|| panic!("missing chunk ({}) in source", hash))
            {
                total_written += chunk.len() as u64;
                output
                    .seek_write(offset, &chunk)
                    .await
                    .expect("failed to write output");
            }
        }
    }
    Ok(total_written)
}

async fn clone_archive(cmd: Command, reader_backend: &mut dyn ReaderBackend) -> Result<(), Error> {
    let archive = ArchiveReader::try_init(reader_backend).await?;
    let mut chunks_left = archive.source_index().clone();
    let mut total_read_from_seed = 0u64;

    info_cmd::print_archive(&archive);
    println!();

    // Verify the header checksum if requested
    if let Some(ref expected_checksum) = cmd.header_checksum {
        if *expected_checksum != *archive.header_checksum() {
            panic!("header checksum mismatch");
        } else {
            info!("Header checksum verified OK");
        }
    }
    info!(
        "Cloning archive {} to {}...",
        cmd.input_archive.source(),
        cmd.output.display()
    );

    // Setup chunker to use when chunking seed input
    let chunker_config = archive.chunker_config().clone();

    // Create or open output file
    let output_file = tokio::fs::OpenOptions::new()
        .write(true)
        .read(cmd.verify_output || cmd.seed_output)
        .create(cmd.force_create || cmd.seed_output)
        .create_new(!cmd.force_create && !cmd.seed_output)
        .open(&cmd.output)
        .await
        .expect("failed to open output file");

    let mut output = Output::new_from(output_file).await?;

    // Check if the given output file is a regular file or block device.
    // If it is a block device we should check its size against the target size before
    // writing. If a regular file then resize that file to target size.
    if output.is_block_dev() {
        let size = output.size().await?;
        if size != archive.total_source_size() {
            panic!(
                "size of output device ({}) differ from size of archive target file ({})",
                size_to_str(size),
                size_to_str(archive.total_source_size())
            );
        }
    }

    // Build an index of the output file's chunks
    let output_chunk_index = if cmd.seed_output {
        Some(
            output
                .chunk_index(&chunker_config, archive.chunk_hash_length())
                .await?,
        )
    } else {
        None
    };

    if let Some(output_index) = output_chunk_index {
        let already_in_place = output_index.strip_chunks_already_in_place(&mut chunks_left);
        info!("{} chunks are already in place in output", already_in_place,);
        total_read_from_seed +=
            update_in_place(&mut output, &output_index, &mut chunks_left).await?;
    }

    if !output.is_block_dev() {
        // Resize output file to same size as the archive source
        output.resize(archive.total_source_size()).await?;
    }

    // Read chunks from seed files
    if cmd.seed_stdin && !atty::is(atty::Stream::Stdin) {
        info!("Scanning stdin for chunks...");
        let seed = SeedInput::new(tokio::io::stdin(), &chunker_config, cmd.num_chunk_buffers);
        let stats = seed.seed(&archive, &mut chunks_left, &mut output).await?;
        info!(
            "Used {} chunks ({}) from stdin",
            stats.chunks_used,
            size_to_str(stats.bytes_used)
        );
    }
    for seed_path in &cmd.seed_files {
        let file = File::open(seed_path)
            .await
            .expect("failed to open seed file");
        info!("Scanning {} for chunks...", seed_path.display());
        let seed = SeedInput::new(file, &chunker_config, cmd.num_chunk_buffers);
        let stats = seed.seed(&archive, &mut chunks_left, &mut output).await?;
        info!(
            "Used {} chunks ({}) from {}",
            stats.chunks_used,
            size_to_str(stats.bytes_used),
            seed_path.display()
        );
    }

    // Read the rest from archive
    let total_output_from_remote = if !chunks_left.is_empty() {
        info!(
            "Fetching {} chunks from {}...",
            chunks_left.len(),
            cmd.input_archive.source()
        );
        finish_using_archive(
            reader_backend,
            &archive,
            chunks_left,
            &mut output,
            cmd.num_chunk_buffers,
        )
        .await?
    } else {
        0
    };

    if cmd.verify_output {
        info!("Verifying checksum of {}...", cmd.output.display());
        let sum = output.checksum().await?;
        let expected_checksum = archive.source_checksum();
        if sum == *expected_checksum {
            info!("Checksum verified Ok");
        } else {
            panic!(
                "checksum mismatch ({}: {}, {}: {})",
                cmd.output.display(),
                sum,
                cmd.input_archive.source(),
                expected_checksum
            );
        }
    }

    info!(
        "Successfully cloned archive using {} from remote and {} from seeds.",
        size_to_str(total_output_from_remote),
        size_to_str(total_read_from_seed)
    );

    Ok(())
}

#[derive(Debug, Clone)]
pub enum InputArchive {
    Local(std::path::PathBuf),
    Remote {
        url: reqwest::Url,
        retries: u32,
        retry_delay: Option<std::time::Duration>,
        receive_timeout: Option<std::time::Duration>,
    },
}
impl InputArchive {
    fn source(&self) -> String {
        match self {
            Self::Local(p) => format!("{}", p.display()),
            Self::Remote { url, .. } => url.to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Command {
    pub force_create: bool,
    pub input_archive: InputArchive,
    pub header_checksum: Option<HashSum>,
    pub output: PathBuf,
    pub seed_stdin: bool,
    pub seed_files: Vec<PathBuf>,
    pub seed_output: bool,
    pub verify_output: bool,
    pub num_chunk_buffers: usize,
}

impl Command {
    pub async fn run(self) -> Result<(), Error> {
        let mut reader_backend: Box<dyn ReaderBackend> = match &self.input_archive {
            InputArchive::Local(path) => Box::new(ReaderBackendLocal::new(
                File::open(path)
                    .await
                    .expect("failed to open local archive"),
            )),
            InputArchive::Remote {
                url,
                retries,
                retry_delay,
                receive_timeout,
            } => Box::new(ReaderBackendRemote::new(
                url.clone(),
                *retries,
                *retry_delay,
                *receive_timeout,
            )),
        };
        clone_archive(self, &mut *reader_backend).await
    }
}
