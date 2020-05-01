use async_trait::async_trait;
use blake2::{Blake2b, Digest};
use log::*;
use reqwest::header::HeaderMap;
use std::io::SeekFrom;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use url::Url;

use crate::info_cmd;
use crate::string_utils::*;
use bitar::{
    clone_from_archive, clone_from_readable, clone_in_place, Archive, CloneOptions, CloneOutput,
    Error, HashSum, Reader, ReaderRemote,
};

struct OutputFile {
    file: File,
    block_dev: bool,
}

impl OutputFile {
    async fn new_from(mut file: File) -> Result<Self, Error> {
        let block_dev = is_block_dev(&mut file).await?;
        Ok(Self { file, block_dev })
    }

    fn is_block_dev(&self) -> bool {
        self.block_dev
    }

    async fn size(&mut self) -> Result<u64, Error> {
        self.file.seek(SeekFrom::Start(0)).await?;
        let size = self.file.seek(SeekFrom::End(0)).await?;
        Ok(size)
    }

    async fn resize(&mut self, source_file_size: u64) -> Result<(), Error> {
        self.file.set_len(source_file_size).await?;
        Ok(())
    }

    async fn checksum(&mut self) -> Result<HashSum, Error> {
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
    async fn write_chunk(
        &mut self,
        _hash: &HashSum,
        offsets: &[u64],
        buf: &[u8],
    ) -> Result<(), Error> {
        for &offset in offsets {
            self.file.seek(SeekFrom::Start(offset)).await?;
            self.file.write_all(buf).await?;
        }
        Ok(())
    }
}

// Check if file is a regular file or block device
#[cfg(unix)]
async fn is_block_dev(file: &mut File) -> Result<bool, Error> {
    use std::os::linux::fs::MetadataExt;
    let meta = file.metadata().await?;
    if meta.st_mode() & 0x6000 == 0x6000 {
        Ok(true)
    } else {
        Ok(false)
    }
}

#[cfg(not(unix))]
async fn is_block_dev(_file: &mut File) -> Result<bool, Error> {
    Ok(false)
}
async fn clone_archive(cmd: Command, reader: &mut dyn Reader) -> Result<(), Error> {
    let archive = Archive::try_init(reader).await?;
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

    // Create or open output file
    let output_file = tokio::fs::OpenOptions::new()
        .write(true)
        .read(cmd.verify_output || cmd.seed_output)
        .create(cmd.force_create || cmd.seed_output)
        .create_new(!cmd.force_create && !cmd.seed_output)
        .open(&cmd.output)
        .await
        .expect("failed to open output file");

    let mut output = OutputFile::new_from(output_file).await?;

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
    let clone_opts = CloneOptions::default().max_buffered_chunks(cmd.num_chunk_buffers);
    if cmd.seed_output {
        info!("Updating chunks of {} in-place...", cmd.output.display());
        let used_from_self =
            clone_in_place(&clone_opts, &mut chunks_left, &archive, &mut output.file).await?;
        info!(
            "Used {} from {}",
            size_to_str(used_from_self),
            cmd.output.display()
        );
        total_read_from_seed += used_from_self;
    }

    if !output.is_block_dev() {
        // Resize output file to same size as the archive source
        output.resize(archive.total_source_size()).await?;
    }

    // Read chunks from seed files
    if cmd.seed_stdin && !atty::is(atty::Stream::Stdin) {
        info!(
            "Scanning stdin for chunks ({} left to find)...",
            chunks_left.len()
        );
        let bytes_to_output = clone_from_readable(
            &clone_opts,
            &mut tokio::io::stdin(),
            &archive,
            &mut chunks_left,
            &mut output,
        )
        .await?;
        info!("Used {} bytes from stdin", size_to_str(bytes_to_output));
    }
    for seed_path in &cmd.seed_files {
        info!(
            "Scanning {} for chunks ({} left to find)...",
            seed_path.display(),
            chunks_left.len()
        );
        let mut file = File::open(seed_path)
            .await
            .expect("failed to open seed file");
        let bytes_to_output = clone_from_readable(
            &clone_opts,
            &mut file,
            &archive,
            &mut chunks_left,
            &mut output,
        )
        .await?;
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
        cmd.input_archive.source()
    );
    let total_read_from_remote =
        clone_from_archive(&clone_opts, reader, &archive, &mut chunks_left, &mut output).await?;
    info!(
        "Used {} bytes from {}",
        size_to_str(total_read_from_remote),
        cmd.input_archive.source()
    );

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
        "Successfully cloned archive using {} from archive and {} from seeds.",
        size_to_str(total_read_from_remote),
        size_to_str(total_read_from_seed)
    );

    Ok(())
}

#[derive(Debug, Clone)]
pub enum InputArchive {
    Local(std::path::PathBuf),
    Remote {
        url: Url,
        retries: u32,
        retry_delay: Option<std::time::Duration>,
        receive_timeout: Option<std::time::Duration>,
        headers: HeaderMap,
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
        let mut reader: Box<dyn Reader> = match &self.input_archive {
            InputArchive::Local(path) => Box::new(
                File::open(path)
                    .await
                    .expect("failed to open local archive"),
            ),
            InputArchive::Remote {
                url,
                retries,
                retry_delay,
                receive_timeout,
                headers,
            } => {
                let mut request = reqwest::Client::new()
                    .get(url.clone())
                    .headers(headers.clone());
                if let Some(timeout) = receive_timeout {
                    request = request.timeout(*timeout);
                }
                Box::new(ReaderRemote::new(request, *retries, *retry_delay))
            }
        };
        clone_archive(self, &mut *reader).await
    }
}
