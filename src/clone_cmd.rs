use log::*;
use std::path::PathBuf;
use tokio::fs::File;

use crate::info_cmd;
use crate::string_utils::*;
use bitar::{
    clone_in_place, clone_using_archive, Archive, CloneOutput, CloneOutputFile, Error, HashSum,
    Reader, ReaderLocal, ReaderRemote, Seed,
};

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

    let mut output = CloneOutputFile::new_from(output_file).await?;

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
            clone_in_place(&mut output, &output_index, &mut chunks_left).await?;
    }

    if !output.is_block_dev() {
        // Resize output file to same size as the archive source
        output.resize(archive.total_source_size()).await?;
    }

    // Read chunks from seed files
    if cmd.seed_stdin && !atty::is(atty::Stream::Stdin) {
        info!("Scanning stdin for chunks...");
        let seed = Seed::new(tokio::io::stdin(), &chunker_config, cmd.num_chunk_buffers);
        let bytes_to_output = seed.seed(&archive, &mut chunks_left, &mut output).await?;
        info!("Used {} bytes from stdin", size_to_str(bytes_to_output));
    }
    for seed_path in &cmd.seed_files {
        let file = File::open(seed_path)
            .await
            .expect("failed to open seed file");
        info!("Scanning {} for chunks...", seed_path.display());
        let seed = Seed::new(file, &chunker_config, cmd.num_chunk_buffers);
        let bytes_to_output = seed.seed(&archive, &mut chunks_left, &mut output).await?;
        info!("Used {} bytes from stdin", size_to_str(bytes_to_output));
    }

    // Read the rest from archive
    let total_output_from_remote = if !chunks_left.is_empty() {
        info!(
            "Fetching {} chunks from {}...",
            chunks_left.len(),
            cmd.input_archive.source()
        );
        clone_using_archive(
            reader,
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
        let mut reader: Box<dyn Reader> = match &self.input_archive {
            InputArchive::Local(path) => Box::new(ReaderLocal::new(
                File::open(path)
                    .await
                    .expect("failed to open local archive"),
            )),
            InputArchive::Remote {
                url,
                retries,
                retry_delay,
                receive_timeout,
            } => Box::new(ReaderRemote::new(
                url.clone(),
                *retries,
                *retry_delay,
                *receive_timeout,
            )),
        };
        clone_archive(self, &mut *reader).await
    }
}
