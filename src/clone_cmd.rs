use futures_util::future;
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use log::*;
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::AsyncRead;

use crate::info_cmd;
use crate::output::Output;
use crate::string_utils::*;
use bitar::archive_reader::ArchiveReader;
use bitar::chunk_index::{ChunkIndex, ReorderOp};
use bitar::chunker::{Chunker, ChunkerConfig};
use bitar::error::Error;
use bitar::reader_backend;
use bitar::HashSum;

async fn seed_input<T>(
    mut input: T,
    seed_name: &str,
    chunker_config: &ChunkerConfig,
    archive: &ArchiveReader,
    chunks_left: &mut ChunkIndex,
    output: &mut Output,
    num_chunk_buffers: usize,
) -> Result<u64, Error>
where
    T: AsyncRead + Unpin,
{
    info!("Scanning {} for chunks...", seed_name);
    let hash_length = archive.chunk_hash_length();
    let mut bytes_read_from_seed: u64 = 0;
    let mut found_chunks_count: usize = 0;
    let seed_chunker = Chunker::new(chunker_config, &mut input);
    let mut found_chunks = seed_chunker
        .map(|result| {
            tokio::task::spawn(async move {
                result
                    .map(|(_offset, chunk)| {
                        (HashSum::b2_digest(&chunk, hash_length as usize), chunk)
                    })
                    .map_err(|err| Error::from(("error while chunking seed", err)))
            })
        })
        .buffered(num_chunk_buffers)
        .filter_map(|result| {
            // Filter unique chunks to be compressed
            future::ready(match result {
                Ok(Ok((hash, chunk))) => {
                    if chunks_left.remove(&hash) {
                        Some(Ok((hash, chunk)))
                    } else {
                        None
                    }
                }
                Ok(Err(err)) => Some(Err(err)),
                Err(err) => Some(Err(("error while chunking seed", err).into())),
            })
        });

    while let Some(result) = found_chunks.next().await {
        let (hash, chunk) = result?;
        debug!(
            "Chunk '{}', size {} used from {}",
            hash,
            size_to_str(chunk.len()),
            seed_name,
        );
        for offset in archive
            .source_index()
            .offsets(&hash)
            .ok_or_else(|| format!("missing chunk ({}) in source!?", hash))?
        {
            bytes_read_from_seed += chunk.len() as u64;
            output
                .seek_write(offset, &chunk)
                .await
                .map_err(|err| ("error writing output", err))?;
        }
        found_chunks_count += 1;
    }
    info!(
        "Used {} chunks ({}) from {}",
        found_chunks_count,
        size_to_str(bytes_read_from_seed),
        seed_name
    );

    Ok(bytes_read_from_seed)
}

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
                        .map_err(|err| ("error reading output", err))?;
                    buf
                };
                for &offset in dest {
                    output
                        .seek_write(offset, &buf[..])
                        .await
                        .map_err(|err| ("error writing output", err))?;
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
                        .map_err(|err| ("error reading output", err))?;
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
    reader_builder: reader_backend::Builder,
    archive: &ArchiveReader,
    chunks_left: ChunkIndex,
    output: &mut Output,
    num_chunk_buffers: usize,
) -> Result<u64, Error> {
    let fetch_count = chunks_left.len();
    let source = reader_builder.source();
    info!("Fetching {} chunks from {}...", fetch_count, &source);
    let mut total_written = 0u64;
    let mut total_fetched = 0u64;
    let grouped_chunks = archive.grouped_chunks(&chunks_left);
    for group in grouped_chunks {
        // For each group of chunks
        let start_offset = archive.chunk_data_offset() + group[0].archive_offset;
        let compression = archive.chunk_compression();
        let chunk_sizes: Vec<usize> = group.iter().map(|c| c.archive_size as usize).collect();

        let archive_chunk_stream = reader_builder
            .read_chunks(start_offset, &chunk_sizes)
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
            let (hash, chunk) =
                result.map_err(|err| Error::Other(format!("spawn error {:?}", err)))?;
            debug!(
                "Chunk '{}', size {} used from archive",
                hash,
                size_to_str(chunk.len()),
            );
            for offset in archive
                .source_index()
                .offsets(&hash)
                .ok_or_else(|| format!("missing chunk ({}) in source!?", hash))?
            {
                total_written += chunk.len() as u64;
                output
                    .seek_write(offset, &chunk)
                    .await
                    .map_err(|err| ("failed to write output", err))?;
            }
        }
    }
    info!(
        "Fetched {} chunks ({} transferred) from {}",
        fetch_count,
        size_to_str(total_fetched),
        &source,
    );
    Ok(total_written)
}

async fn clone_archive(cmd: &Command) -> Result<(), Error> {
    let archive = ArchiveReader::try_init(cmd.input.clone()).await?;
    let mut chunks_left = archive.source_index().clone();
    let mut total_read_from_seed = 0u64;

    info_cmd::print_archive(&archive);
    println!();

    // Verify the header checksum if requested
    if let Some(ref expected_checksum) = cmd.header_checksum {
        if *expected_checksum != *archive.header_checksum() {
            return Err(Error::ChecksumMismatch(
                "Header checksum mismatch!".to_owned(),
            ));
        } else {
            info!("Header checksum verified OK");
        }
    }
    info!(
        "Cloning archive {} to {}...",
        cmd.input.source(),
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
        .map_err(|e| {
            (
                format!("failed to open output file ({})", cmd.output.display()),
                e,
            )
        })?;

    let mut output = Output::new_from(output_file).await?;

    // Check if the given output file is a regular file or block device.
    // If it is a block device we should check its size against the target size before
    // writing. If a regular file then resize that file to target size.
    if output.is_block_dev() {
        let size = output.size().await?;
        if size != archive.total_source_size() {
            return Err(format!(
                "Size of output device ({}) differ from size of archive target file ({})",
                size_to_str(size),
                size_to_str(archive.total_source_size())
            )
            .into());
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
        total_read_from_seed += seed_input(
            tokio::io::stdin(),
            "stdin",
            &chunker_config,
            &archive,
            &mut chunks_left,
            &mut output,
            cmd.num_chunk_buffers,
        )
        .await?;
    }
    for seed_path in &cmd.seed_files {
        let file = File::open(seed_path)
            .await
            .map_err(|e| ("failed to open seed file", e))?;
        total_read_from_seed += seed_input(
            file,
            &format!("{}", seed_path.display()),
            &chunker_config,
            &archive,
            &mut chunks_left,
            &mut output,
            cmd.num_chunk_buffers,
        )
        .await?;
    }

    // Read the rest from archive
    let total_output_from_remote = if !chunks_left.is_empty() {
        finish_using_archive(
            cmd.input.clone(),
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
            return Err(format!(
                "Checksum mismatch. {}: {}, {}: {}.",
                cmd.output.display(),
                sum,
                cmd.input.source(),
                expected_checksum
            )
            .into());
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
pub struct Command {
    pub force_create: bool,
    pub input: reader_backend::Builder,
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
        clone_archive(&self).await
    }
}
