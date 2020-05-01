use log::*;
use tokio::fs::File;

use crate::string_utils::*;
use bitar::{Archive, ChunkerConfig, ChunkerFilterConfig, Error, Reader, ReaderRemote};

pub async fn print_archive_reader(reader: &mut dyn Reader) -> Result<(), Error> {
    let archive = Archive::try_init(reader).await?;
    print_archive(&archive);
    Ok(())
}

fn print_rolling_hash_config(hc: &ChunkerFilterConfig) {
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

pub fn print_archive(archive: &Archive) {
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
        let mut reader: Box<dyn Reader> = if let Ok(uri) = self.input.parse::<reqwest::Url>() {
            let request = reqwest::Client::new().get(uri);
            Box::new(ReaderRemote::new(request, 0, None))
        } else {
            Box::new(File::open(&self.input).await?)
        };
        print_archive_reader(&mut *reader).await?;
        Ok(())
    }
}
