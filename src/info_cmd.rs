use log::*;
use std::path::Path;

use crate::string_utils::*;
use bitar::archive_reader::ArchiveReader;
use bitar::chunker::{ChunkerConfig, HashConfig};
use bitar::Error;
use bitar::ReaderBackend;

pub async fn print_archive_backend(reader_backend: ReaderBackend) -> Result<(), Error> {
    let archive = ArchiveReader::try_init(reader_backend).await?;
    print_archive(&archive);
    Ok(())
}

fn print_rolling_hash_config(hc: &HashConfig) {
    info!(
        "  Rolling hash window size: {}",
        size_to_str(hc.window_size)
    );
    info!("  Chunk minimum size: {}", size_to_str(hc.min_chunk_size));
    info!("  Chunk maximum size: {}", size_to_str(hc.max_chunk_size));
    info!(
        "  Chunk average target size: {} (mask: {:#b})",
        size_to_str(hc.filter_bits.chunk_target_average()),
        hc.filter_bits.mask(),
    );
}

pub fn print_chunker_config(config: &ChunkerConfig) {
    info!("  Chunking algorithm: {}", config);
    match config {
        ChunkerConfig::BuzHash(hc) => print_rolling_hash_config(hc),
        ChunkerConfig::RollSum(hc) => print_rolling_hash_config(hc),
        ChunkerConfig::FixedSize(chunk_size) => {
            info!("  Fixed chunk size: {}", size_to_str(*chunk_size));
        }
    }
}

pub fn print_archive(archive: &ArchiveReader) {
    info!("Archive: ");
    info!("  Version: {}", archive.built_with_version());
    info!(
        "  Archive size: {}",
        size_to_str(archive.compressed_size() + archive.header_size() as u64)
    );
    info!("  Header checksum: {}", archive.header_checksum());

    print_chunker_config(&archive.chunker_config());

    info!("Source:");
    info!("  Source checksum: {}", archive.source_checksum());
    info!(
        "  Chunks in source: {} (unique: {})",
        archive.total_chunks(),
        archive.unique_chunks()
    );
    info!(
        "  Average chunk size: {}",
        size_to_str(
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
        size_to_str(archive.total_source_size())
    );
}

#[derive(Debug, Clone)]
pub struct Command {
    pub input: String,
}

impl Command {
    pub async fn run(self) -> Result<(), Error> {
        let builder = if let Ok(uri) = self.input.parse() {
            ReaderBackend::new_remote(uri, 0, None, None)
        } else {
            ReaderBackend::new_local(&Path::new(&self.input))
        };
        print_archive_backend(builder).await?;
        Ok(())
    }
}
