use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use blake2::{Blake2b, Digest};
use log::*;
use reqwest::header::HeaderMap;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use url::Url;

use crate::info_cmd;
use crate::string_utils::*;
use bitar::{clone, clone::CloneOutput, Archive, HashSum, Reader, ReaderRemote};

struct OutputFile {
    file: File,
    block_dev: bool,
}

impl OutputFile {
    async fn new_from(mut file: File) -> Result<Self, std::io::Error> {
        let block_dev = is_block_dev(&mut file).await?;
        Ok(Self { file, block_dev })
    }

    fn is_block_dev(&self) -> bool {
        self.block_dev
    }

    async fn size(&mut self) -> Result<u64, std::io::Error> {
        self.file.seek(SeekFrom::Start(0)).await?;
        let size = self.file.seek(SeekFrom::End(0)).await?;
        Ok(size)
    }

    async fn resize(&mut self, source_file_size: u64) -> Result<(), std::io::Error> {
        self.file.set_len(source_file_size).await?;
        Ok(())
    }

    async fn checksum(&mut self) -> Result<HashSum, std::io::Error> {
        self.file.seek(SeekFrom::Start(0)).await?;
        let mut output_hasher = Blake2b::new();
        let mut buffer: Vec<u8> = vec![0; 4 * 1024 * 1024];
        loop {
            let rc = self.file.read(&mut buffer).await?;
            if rc == 0 {
                break;
            }
            output_hasher.input(&buffer[0..rc]);
        }
        Ok(HashSum::from_slice(&output_hasher.result()[..]))
    }
}

#[async_trait]
impl CloneOutput for OutputFile {
    type Error = std::io::Error;
    async fn write_chunk(
        &mut self,
        _hash: &HashSum,
        offsets: &[u64],
        buf: &[u8],
    ) -> Result<(), Self::Error> {
        for &offset in offsets {
            self.file.seek(SeekFrom::Start(offset)).await?;
            self.file.write_all(buf).await?;
        }
        Ok(())
    }
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

async fn clone_archive<R>(opts: Options, mut reader: R) -> Result<()>
where
    R: Reader,
    R::Error: std::error::Error + Send + Sync + 'static,
{
    let archive = Archive::try_init(&mut reader).await.context(format!(
        "Failed to read archive at {}",
        opts.input_archive.source()
    ))?;
    let mut chunks_left = archive.build_source_index();
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
    let output_file = tokio::fs::OpenOptions::new()
        .write(true)
        .read(opts.verify_output || opts.seed_output)
        .create(opts.force_create || opts.seed_output)
        .create_new(!opts.force_create && !opts.seed_output)
        .open(&opts.output)
        .await
        .context(format!("Failed to open {}", opts.output.display()))?;

    let mut output = OutputFile::new_from(output_file).await?;

    // Check if the given output file is a regular file or block device.
    // If it is a block device we should check its size against the target size before
    // writing. If a regular file then resize that file to target size.
    if output.is_block_dev() {
        let size = output.size().await?;
        if size != archive.total_source_size() {
            return Err(anyhow!(
                "Size of output device ({}) differ from size of archive target file ({})",
                size_to_str(size),
                size_to_str(archive.total_source_size())
            ));
        }
    }

    // Build an index of the output file's chunks
    let clone_opts = clone::Options::default().max_buffered_chunks(opts.num_chunk_buffers);
    if opts.seed_output {
        info!("Updating chunks of {} in-place...", opts.output.display());
        let used_from_self =
            clone::in_place(&clone_opts, &mut chunks_left, &archive, &mut output.file)
                .await
                .context("Failed to clone in place")?;
        info!(
            "Used {} from {}",
            size_to_str(used_from_self),
            opts.output.display()
        );
        total_read_from_seed += used_from_self;
    }

    if !output.is_block_dev() {
        // Resize output file to same size as the archive source
        output
            .resize(archive.total_source_size())
            .await
            .context(format!("Failed to resize {}", opts.output.display()))?;
    }

    // Read chunks from seed files
    if opts.seed_stdin && !atty::is(atty::Stream::Stdin) {
        info!(
            "Scanning stdin for chunks ({} left to find)...",
            chunks_left.len()
        );
        let bytes_to_output = clone::from_readable(
            &clone_opts,
            &mut tokio::io::stdin(),
            &archive,
            &mut chunks_left,
            &mut output,
        )
        .await
        .context("Failed to clone from stdin")?;
        info!("Used {} bytes from stdin", size_to_str(bytes_to_output));
    }
    for seed_path in &opts.seed_files {
        let mut file = File::open(seed_path)
            .await
            .context(format!("Failed to open seed file {}", seed_path.display()))?;
        info!(
            "Scanning {} for chunks ({} left to find)...",
            seed_path.display(),
            chunks_left.len()
        );
        let bytes_to_output = clone::from_readable(
            &clone_opts,
            &mut file,
            &archive,
            &mut chunks_left,
            &mut output,
        )
        .await
        .context(format!("Failed to clone from {}", seed_path.display()))?;
        info!(
            "Used {} bytes from {}",
            size_to_str(bytes_to_output),
            seed_path.display()
        );
    }

    // Read the rest from archive
    info!(
        "Fetching {} chunks from {}...",
        chunks_left.len(),
        opts.input_archive.source()
    );
    let total_read_from_remote = clone::from_archive(
        &clone_opts,
        &mut reader,
        &archive,
        &mut chunks_left,
        &mut output,
    )
    .await
    .context(format!(
        "Failed to clone from archive at {}",
        opts.input_archive.source()
    ))?;
    info!(
        "Used {} bytes from {}",
        size_to_str(total_read_from_remote),
        opts.input_archive.source()
    );

    if opts.verify_output {
        info!("Verifying checksum of {}...", opts.output.display());
        let sum = output.checksum().await.context(format!(
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
        size_to_str(total_read_from_remote),
        size_to_str(total_read_from_seed)
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
                File::open(&path)
                    .await
                    .context(format!("Failed to open {}", path.display()))?,
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
                ReaderRemote::from_request(request)
                    .retries(input.retries)
                    .retry_delay(input.retry_delay),
            )
            .await
        }
    }
}
