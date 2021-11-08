use anyhow::{anyhow, Context, Result};
use blake2::{Blake2b, Digest};
use futures_util::StreamExt;
use log::*;
use reqwest::header::HeaderMap;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::time::Duration;
use tokio::fs::File;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite},
    task::spawn_blocking,
};
use url::Url;

use crate::{human_size, info_cmd};
use bitar::{
    archive_reader::{ArchiveReader, HttpReader, IoReader},
    chunker, Archive, ChunkIndex, CloneOutput, HashSum, VerifiedChunk,
};

async fn file_size(file: &mut File) -> Result<u64, std::io::Error> {
    file.seek(SeekFrom::Start(0)).await?;
    file.seek(SeekFrom::End(0)).await
}

async fn file_checksum(file: &mut File) -> Result<HashSum, std::io::Error> {
    file.seek(SeekFrom::Start(0)).await?;
    let mut output_hasher = Blake2b::new();
    let mut buffer: Vec<u8> = vec![0; 4 * 1024 * 1024];
    loop {
        let rc = file.read(&mut buffer).await?;
        if rc == 0 {
            break;
        }
        output_hasher.update(&buffer[0..rc]);
    }
    Ok(HashSum::from(&output_hasher.finalize()[..]))
}

// Check if file is a regular file or block device
#[cfg(unix)]
async fn is_block_dev(file: &mut File) -> Result<bool, std::io::Error> {
    use std::os::linux::fs::MetadataExt;
    let meta = file.metadata().await?;
    if meta.st_mode() & 0x6000 == 0x6000 {
        Ok(true)
    } else {
        Ok(false)
    }
}

#[cfg(not(unix))]
async fn is_block_dev(_file: &mut File) -> Result<bool, std::io::Error> {
    Ok(false)
}

async fn feed_output<S, C>(output: &mut CloneOutput<C>, mut chunk_stream: S) -> Result<u64>
where
    S: StreamExt<Item = Result<VerifiedChunk>> + Unpin,
    C: AsyncWrite + AsyncSeek + Unpin + Send,
{
    let mut output_bytes = 0;
    while let Some(result) = chunk_stream.next().await {
        let verified = result?;
        let wc = output.feed(&verified).await?;
        if wc > 0 {
            debug!("Chunk '{}', size {} used", verified.hash(), verified.len());
        }
        output_bytes += wc as u64;
    }
    Ok(output_bytes)
}

async fn clone_from_readable<I, C>(
    max_buffered_chunks: usize,
    config: &chunker::Config,
    input: I,
    output: &mut CloneOutput<C>,
) -> Result<u64>
where
    I: AsyncRead + Unpin + Send,
    C: AsyncWrite + AsyncSeek + Unpin + Send,
{
    let chunk_stream = config
        .new_chunker(input)
        .map(|r| spawn_blocking(|| r.map(|(_, chunk)| chunk.verify())))
        .buffered(max_buffered_chunks)
        .map(|r| match r {
            Ok(inner) => Ok(inner?),
            Err(err) => Err(anyhow!(err)),
        });
    feed_output(output, chunk_stream).await
}

async fn clone_from_archive<R, C>(
    max_buffered_chunks: usize,
    archive: &mut Archive<R>,
    output: &mut CloneOutput<C>,
) -> Result<u64>
where
    R: ArchiveReader,
    R::Error: std::error::Error + Sync + Send + 'static,
    C: AsyncWrite + AsyncSeek + Unpin + Send,
{
    let mut total_fetched = 0u64;
    let chunk_stream = archive
        .chunk_stream(output.chunks())
        .map(|r| {
            if let Ok(compressed) = &r {
                total_fetched += compressed.len() as u64;
            }
            spawn_blocking(move || -> Result<VerifiedChunk> {
                let compressed = r.context("read archive")?;
                let verified = compressed
                    .decompress()
                    .context("decompress chunk")?
                    .verify()
                    .context("verify chunk")?;
                Ok(verified)
            })
        })
        .buffered(max_buffered_chunks)
        .map(|r| match r {
            Ok(inner) => inner,
            Err(err) => Err(anyhow!(err)),
        });
    let total_written = feed_output(output, chunk_stream).await?;
    info!(
        "Fetched {} from archive and decompressed to {}.",
        human_size!(total_fetched),
        human_size!(total_written)
    );
    Ok(total_fetched)
}

async fn chunk_index_from_readable<R>(
    hash_length: usize,
    config: &chunker::Config,
    max_buffered_chunks: usize,
    readable: &mut R,
) -> Result<ChunkIndex>
where
    R: AsyncRead + Unpin + Send,
{
    let mut chunk_stream = config
        .new_chunker(readable)
        .map(|r| spawn_blocking(|| r.map(|(offset, chunk)| (offset, chunk.verify()))))
        .buffered(max_buffered_chunks);
    let mut index = ChunkIndex::new_empty(hash_length);
    while let Some(r) = chunk_stream.next().await {
        let (chunk_offset, verified) = r??;
        let (hash, chunk) = verified.into_parts();
        index.add_chunk(hash, chunk.len(), &[chunk_offset]);
    }
    Ok(index)
}

async fn clone_archive<R>(opts: Options, reader: R) -> Result<()>
where
    R: ArchiveReader,
    R::Error: std::error::Error + Send + Sync + 'static,
{
    let mut archive = Archive::try_init(reader).await.context(format!(
        "Failed to read archive at {}",
        opts.input_archive.source()
    ))?;
    let clone_index = archive.build_source_index();
    let mut total_read_from_seed = 0u64;

    info_cmd::print_archive(&archive);
    println!();

    // Verify the header checksum if requested
    if let Some(ref expected_checksum) = opts.header_checksum {
        if *expected_checksum != *archive.header_checksum() {
            return Err(anyhow!("Header checksum mismatch"));
        } else {
            info!("Header checksum verified OK");
        }
    }
    info!(
        "Cloning archive {} to {}...",
        opts.input_archive.source(),
        opts.output.display()
    );

    // Create or open output file
    let mut output_file = tokio::fs::OpenOptions::new()
        .write(true)
        .read(opts.verify_output || opts.seed_output)
        .create(opts.force_create || opts.seed_output)
        .create_new(!opts.force_create && !opts.seed_output)
        .open(&opts.output)
        .await
        .context(format!("Failed to open {}", opts.output.display()))?;

    // Check if the given output file is a regular file or block device.
    // If it is a block device we should check its size against the target size before
    // writing. If a regular file then resize that file to target size.
    let output_is_block_dev = is_block_dev(&mut output_file).await?;
    if output_is_block_dev {
        let size = file_size(&mut output_file).await?;
        if size < archive.total_source_size() {
            return Err(anyhow!(
                "Size of output device ({}) is less than archive target file ({})",
                human_size!(size),
                human_size!(archive.total_source_size())
            ));
        }
    }

    // Build an index of the output file's chunks
    let output_index = if opts.seed_output {
        info!("Building chunk index of {}...", opts.output.display());
        Some(
            chunk_index_from_readable(
                archive.chunk_hash_length(),
                archive.chunker_config(),
                opts.num_chunk_buffers,
                &mut output_file,
            )
            .await?,
        )
    } else {
        None
    };

    let mut output = CloneOutput::new(output_file, clone_index);
    if let Some(output_index) = output_index {
        info!("Re-ordering chunks of {}...", opts.output.display());
        let used_from_self = output
            .reorder_in_place(output_index)
            .await
            .context("Failed to clone in place")?;
        info!(
            "Used {} from {}",
            human_size!(used_from_self),
            opts.output.display()
        );
        total_read_from_seed += used_from_self;
    }

    // Read chunks from seed files
    if opts.seed_stdin && !atty::is(atty::Stream::Stdin) {
        info!(
            "Scanning stdin for chunks ({} left to find)...",
            output.len()
        );
        let bytes_to_output = clone_from_readable(
            opts.num_chunk_buffers,
            archive.chunker_config(),
            &mut tokio::io::stdin(),
            &mut output,
        )
        .await
        .context("Failed to clone from stdin")?;
        info!("Used {} bytes from stdin", human_size!(bytes_to_output));
        total_read_from_seed += bytes_to_output;
    }
    for seed_path in &opts.seed_files {
        let file = File::open(seed_path)
            .await
            .context(format!("Failed to open seed file {}", seed_path.display()))?;
        info!(
            "Scanning {} for chunks ({} left to find)...",
            seed_path.display(),
            output.len()
        );
        let bytes_to_output = clone_from_readable(
            opts.num_chunk_buffers,
            archive.chunker_config(),
            file,
            &mut output,
        )
        .await
        .context(format!("Failed to clone from {}", seed_path.display()))?;
        info!(
            "Used {} bytes from {}",
            human_size!(bytes_to_output),
            seed_path.display()
        );
        total_read_from_seed += bytes_to_output;
    }

    // Read the rest from archive
    info!(
        "Fetching {} chunks from {}...",
        output.len(),
        opts.input_archive.source()
    );

    let total_read_from_remote =
        clone_from_archive(opts.num_chunk_buffers, &mut archive, &mut output)
            .await
            .context(format!(
                "Failed to clone from archive at {}",
                opts.input_archive.source()
            ))?;

    let mut output_file = output.into_inner();
    if !output_is_block_dev {
        // Resize output file to same size as the archive source
        output_file
            .set_len(archive.total_source_size())
            .await
            .context(format!("Failed to resize {}", opts.output.display()))?;
    }

    if opts.verify_output {
        info!("Verifying checksum of {}...", opts.output.display());
        let sum = file_checksum(&mut output_file).await.context(format!(
            "Failed to create checksum of {}",
            opts.output.display()
        ))?;
        let expected_checksum = archive.source_checksum();
        if sum == *expected_checksum {
            info!("Checksum verified Ok");
        } else {
            return Err(anyhow!(
                "Checksum mismatch ({}: {}, {}: {})",
                opts.output.display(),
                sum,
                opts.input_archive.source(),
                expected_checksum
            ));
        }
    }

    info!(
        "Successfully cloned archive using {} from archive and {} from seeds.",
        human_size!(total_read_from_remote),
        human_size!(total_read_from_seed)
    );

    Ok(())
}

#[derive(Debug, Clone)]
pub struct RemoteInput {
    pub url: Url,
    pub retries: u32,
    pub retry_delay: Duration,
    pub receive_timeout: Option<Duration>,
    pub headers: HeaderMap,
}

#[derive(Debug, Clone)]
pub enum InputArchive {
    Local(std::path::PathBuf),
    Remote(Box<RemoteInput>),
}
impl InputArchive {
    fn source(&self) -> String {
        match self {
            Self::Local(p) => format!("{}", p.display()),
            Self::Remote(input) => input.url.to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Options {
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

pub async fn clone_cmd(opts: Options) -> Result<()> {
    match opts.input_archive.clone() {
        InputArchive::Local(path) => {
            clone_archive(
                opts,
                IoReader::new(
                    File::open(&path)
                        .await
                        .context(format!("Failed to open {}", path.display()))?,
                ),
            )
            .await
        }
        InputArchive::Remote(input) => {
            let mut request = reqwest::Client::new()
                .get(input.url.clone())
                .headers(input.headers.clone());
            if let Some(timeout) = input.receive_timeout {
                request = request.timeout(timeout);
            }
            clone_archive(
                opts,
                HttpReader::from_request(request)
                    .retries(input.retries)
                    .retry_delay(input.retry_delay),
            )
            .await
        }
    }
}
