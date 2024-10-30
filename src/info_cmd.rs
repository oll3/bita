use anyhow::{bail, Result};
use log::*;
use std::collections::HashMap;
use std::io::Write;
use tokio::fs::File;

use crate::clone_cmd::InputArchive;
use crate::human_size;
use bitar::{
    archive_reader::{ArchiveReader, HttpReader, IoReader},
    chunker, Archive,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Options {
    /// Medatada key to get value for, or None to print regular archive info.
    pub metadata_key: Option<String>,
    /// Local file or URL to read archive from.
    pub input_archive: InputArchive,
}

pub async fn print_archive_reader<R>(reader: R) -> Result<()>
where
    R: ArchiveReader,
    R::Error: std::error::Error + Send + Sync + 'static,
{
    let archive = Archive::try_init(reader).await?;
    print_archive(&archive);
    Ok(())
}

fn print_rolling_hash_config(hc: &chunker::FilterConfig) {
    info!(
        "  Rolling hash window size: {}",
        human_size!(hc.window_size)
    );
    info!("  Chunk minimum size: {}", human_size!(hc.min_chunk_size));
    info!("  Chunk maximum size: {}", human_size!(hc.max_chunk_size));
    info!(
        "  Chunk average target size: {} (mask: {:#b})",
        human_size!(hc.filter_bits.chunk_target_average()),
        hc.filter_bits.mask(),
    );
}

pub fn print_chunker_config(config: &chunker::Config) {
    info!(
        "  Chunking algorithm: {}",
        match config {
            chunker::Config::BuzHash(_) => "BuzHash",
            chunker::Config::RollSum(_) => "RollSum",
            chunker::Config::FixedSize(_) => "Fixed Size",
        }
    );
    match config {
        chunker::Config::BuzHash(hc) => print_rolling_hash_config(hc),
        chunker::Config::RollSum(hc) => print_rolling_hash_config(hc),
        chunker::Config::FixedSize(chunk_size) => {
            info!("  Fixed chunk size: {}", human_size!(*chunk_size));
        }
    }
}

pub fn print_metadata_overview(metadata: &HashMap<String, Vec<u8>>) {
    if metadata.is_empty() {
        info!("  Metadata: None");
    } else {
        let display = metadata
            .iter()
            .map(|(key, value)| format!("{}({})", key, value.len()))
            .collect::<Vec<String>>()
            .join(", ");
        info!("  Metadata: {}", display);
    }
}

pub fn print_archive<R>(archive: &Archive<R>) {
    info!("Archive: ");
    info!("  Built with version: {}", archive.built_with_version());
    info!(
        "  Archive size: {}",
        human_size!(archive.compressed_size() + archive.header_size() as u64)
    );

    print_metadata_overview(archive.metadata());

    info!("  Header checksum: {}", archive.header_checksum());
    info!("  Chunk hash length: {} bytes", archive.chunk_hash_length());
    info!(
        "  Chunk compression: {}",
        match archive.chunk_compression() {
            None => "None".to_string(),
            Some(c) => format!("{}", c),
        }
    );

    print_chunker_config(archive.chunker_config());

    info!("Source:");
    info!("  Source checksum: {}", archive.source_checksum());
    info!(
        "  Chunks in source: {} (unique: {})",
        archive.total_chunks(),
        archive.unique_chunks()
    );
    info!(
        "  Average chunk size: {}",
        human_size!(
            archive
                .chunk_descriptors()
                .iter()
                .map(|cdesc| u64::from(cdesc.source_size))
                .sum::<u64>()
                / archive.chunk_descriptors().len() as u64
        )
    );
    info!(
        "  Source size: {}",
        human_size!(archive.total_source_size())
    );
}

async fn info_impl<R>(reader: R, metadata_key: Option<String>) -> Result<()>
where
    R: ArchiveReader,
    R::Error: std::error::Error + Send + Sync + 'static,
{
    if let Some(key) = metadata_key {
        let archive = Archive::try_init(reader).await?;
        if let Some(value) = archive.metadata_value(key.as_str()) {
            std::io::stdout().write_all(&value)?;
        } else {
            bail!("Metadata key not found: {}", key);
        }
        Ok(())
    } else {
        print_archive_reader(reader).await
    }
}

pub async fn info_cmd(options: Options) -> Result<()> {
    match options.input_archive {
        InputArchive::Local(path) => {
            info_impl(IoReader::new(File::open(path).await?), options.metadata_key).await
        }
        InputArchive::Remote(input) => {
            let mut request = reqwest::Client::new()
                .get(input.url.clone())
                .headers(input.headers.clone());
            if let Some(timeout) = input.receive_timeout {
                request = request.timeout(timeout);
            }
            let reader = HttpReader::from_request(request)
                .retries(input.retries)
                .retry_delay(input.retry_delay);
            info_impl(reader, options.metadata_key).await
        }
    }
}
