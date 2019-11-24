use atty::Stream;
use blake2::{Blake2b, Digest};
use futures::future;
use log::*;
use std::collections::HashSet;
use std::io::SeekFrom;
use std::path::Path;
use tokio::fs::{File, OpenOptions};
use tokio::prelude::*;
use tokio::sync::oneshot;

use crate::config;
use crate::info_cmd;
use bita::archive_reader::ArchiveReader;
use bita::chunker::{Chunker, ChunkerParams};
use bita::error::Error;
use bita::reader_backend;
use bita::string_utils::*;

async fn seed_input<T>(
    input: T,
    seed_name: &str,
    hash_length: u32,
    chunker_params: ChunkerParams,
    archive: &ArchiveReader,
    chunks_left: &mut HashSet<Vec<u8>>,
    output_file: &mut File,
) -> Result<u64, Error>
where
    T: AsyncRead + Unpin,
{
    info!("Scanning {} for chunks...", seed_name);
    let mut bytes_read_from_seed: u64 = 0;
    let mut found_chunks_count: usize = 0;
    let seed_chunker = Chunker::new(chunker_params, input);
    let mut found_chunks = seed_chunker
        .map(|result| {
            let (_offset, chunk) = result.expect("error while chunking");
            // Build hash of full source
            async move {
                // Calculate strong hash for each chunk
                let mut chunk_hasher = Blake2b::new();
                chunk_hasher.input(&chunk);
                (
                    chunk_hasher.result()[0..hash_length as usize].to_vec(),
                    chunk,
                )
            }
        })
        .buffered(8)
        .filter_map(|(hash, chunk)| {
            // Filter unique chunks to be compressed
            if chunks_left.remove(&hash) {
                future::ready(Some((hash, chunk)))
            } else {
                future::ready(None)
            }
        });

    while let Some((hash, chunk)) = found_chunks.next().await {
        debug!(
            "Chunk '{}', size {} used from {}",
            HexSlice::new(&hash),
            size_to_str(chunk.len()),
            seed_name,
        );
        for offset in &archive.chunk_source_offsets(&hash) {
            bytes_read_from_seed += chunk.len() as u64;
            output_file
                .seek(SeekFrom::Start(*offset))
                .await
                .map_err(|err| ("failed to seek output", err))?;
            output_file
                .write_all(&chunk)
                .await
                .map_err(|err| ("failed to write output", err))?;
        }
        found_chunks_count += 1;
    }
    info!(
        "Used {} chunks from seed file {}",
        found_chunks_count, seed_name
    );

    Ok(bytes_read_from_seed)
}

async fn prepare_unpack_output(
    mut output_file: File,
    source_file_size: u64,
) -> Result<File, Error> {
    #[cfg(unix)]
    {
        use std::os::linux::fs::MetadataExt;
        let meta = output_file
            .metadata()
            .await
            .map_err(|e| ("unable to get file meta data", e))?;
        if meta.st_mode() & 0x6000 == 0x6000 {
            // Output is a block device
            let size = output_file
                .seek(SeekFrom::End(0))
                .await
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
                .await
                .map_err(|e| ("unable to seek output file", e))?;
        } else {
            // Output is a reqular file
            output_file
                .set_len(source_file_size)
                .await
                .map_err(|e| ("unable to resize output file", e))?;
        }
    }
    #[cfg(not(unix))]
    {
        output_file
            .set_len(source_file_size)
            .await
            .map_err(|e| ("unable to resize output file", e))?;
    }
    Ok(output_file)
}

#[allow(clippy::cognitive_complexity)]
async fn clone_archive(
    reader_builder: reader_backend::Builder,
    config: &config::CloneConfig,
) -> Result<(), Error> {
    let archive = ArchiveReader::init(reader_builder.clone()).await?;
    let mut chunks_left = archive.chunk_hash_set();
    let hash_length = archive.hash_length;
    let mut total_read_from_seed = 0u64;
    let mut total_read_from_archive = 0u64;

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

    // Create or open output file
    let mut output_file = OpenOptions::new()
        .write(true)
        .read(config.verify_output)
        .create(config.force_create)
        .create_new(!config.force_create)
        .open(&config.output)
        .await
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
    output_file = prepare_unpack_output(output_file, archive.source_total_size).await?;

    if config.seed_stdin && !atty::is(Stream::Stdin) {
        total_read_from_seed += seed_input(
            tokio::io::stdin(),
            "stdin",
            hash_length as u32,
            chunker_params.clone(),
            &archive,
            &mut chunks_left,
            &mut output_file,
        )
        .await?;
    }
    for seed_path in &config.seed_files {
        let file = File::open(seed_path)
            .await
            .map_err(|e| ("failed to open seed file", e))?;
        total_read_from_seed += seed_input(
            file,
            &format!("{}", seed_path.display()),
            hash_length as u32,
            chunker_params.clone(),
            &archive,
            &mut chunks_left,
            &mut output_file,
        )
        .await?;
    }

    // Read the rest from archive
    let grouped_chunks = archive.grouped_chunks(&chunks_left);
    for group in grouped_chunks {
        // For each group of chunks
        let start_offset = archive.archive_chunks_offset + group[0].archive_offset;
        let compression = archive.chunk_compression;
        let chunk_sizes: Vec<usize> = group.iter().map(|c| c.archive_size as usize).collect();

        let mut archive_chunk_stream = reader_builder
            .read_chunks(start_offset, &chunk_sizes)?
            .enumerate()
            .map(|(chunk_index, chunk)| {
                let chunk = chunk.expect("failed to read archive");
                //let chunk_descriptor = &group[chunk_index];
                let chunk_checksum = group[chunk_index].checksum.clone();
                let chunk_source_size = group[chunk_index].source_size as usize;
                let (tx, rx) = oneshot::channel();
                tokio::spawn(async move {
                    tx.send((
                        chunk_checksum.clone(),
                        ArchiveReader::decompress_and_verify(
                            hash_length,
                            compression,
                            &chunk_checksum,
                            chunk_source_size,
                            chunk,
                        )
                        .expect("failed to decompresss chunk"),
                    ))
                    .expect("failed to send");
                });
                rx.map(|v| v.expect("failed to receive"))
            })
            .buffered(8);

        while let Some((hash, chunk)) = archive_chunk_stream.next().await {
            // For each chunk read from archive
            debug!(
                "Chunk '{}', size {} used from archive",
                HexSlice::new(&hash),
                size_to_str(chunk.len()),
            );
            for offset in &archive.chunk_source_offsets(&hash) {
                total_read_from_archive += chunk.len() as u64;
                output_file
                    .seek(SeekFrom::Start(*offset))
                    .await
                    .map_err(|err| ("failed to seek output", err))?;
                output_file
                    .write_all(&chunk)
                    .await
                    .map_err(|err| ("failed to write output", err))?;
            }
        }
    }

    if config.verify_output {
        info!("Verifying checksum of {}...", config.output.display());
        output_file
            .seek(SeekFrom::Start(0))
            .await
            .map_err(|err| ("failed to seek output", err))?;
        let mut output_hasher = Blake2b::new();
        let mut buffer: Vec<u8> = vec![0; 4 * 1024 * 1024];
        loop {
            let rc = output_file
                .read(&mut buffer)
                .await
                .map_err(|err| ("failed to read output", err))?;
            if rc == 0 {
                break;
            }
            output_hasher.input(&buffer[0..rc]);
        }
        let sum = output_hasher.result().to_vec();
        if sum == archive.source_checksum {
            info!("Checksum verified Ok");
        } else {
            panic!(format!(
                "Checksum mismatch. {}: {}, {}: {}.",
                config.output.display(),
                HexSlice::new(&sum),
                config.input,
                HexSlice::new(&archive.source_checksum)
            ));
        }
    }

    info!(
        "Successfully cloned archive using {} from remote and {} from seeds.",
        size_to_str(total_read_from_archive),
        size_to_str(total_read_from_seed)
    );

    Ok(())
}

pub async fn run(config: config::CloneConfig) -> Result<(), Error> {
    let reader_builder = if &config.input[0..7] == "http://" || &config.input[0..8] == "https://" {
        reader_backend::Builder::new_remote(
            config.input.parse().unwrap(),
            config.http_retry_count,
            config.http_retry_delay,
            config.http_timeout,
        )
    } else {
        reader_backend::Builder::new_local(&Path::new(&config.input))
    };
    clone_archive(reader_builder, &config).await
}
