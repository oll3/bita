use anyhow::Result;
use log::*;
use tokio::fs::File;

use crate::string_utils::*;
use bitar::{chunker, Archive, Reader, ReaderRemote};

pub async fn print_archive_reader<R>(reader: &mut R) -> Result<()>
where
    R: Reader,
    R::Error: std::error::Error + Send + Sync + 'static,
{
    let archive = Archive::try_init(reader).await?;
    print_archive(&archive);
    Ok(())
}

fn print_rolling_hash_config(hc: &chunker::FilterConfig) {
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
            info!("  Fixed chunk size: {}", size_to_str(*chunk_size));
        }
    }
}

pub fn print_archive(archive: &Archive) {
    info!("Archive: ");
    info!("  Built with version: {}", archive.built_with_version());
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

pub async fn info_cmd(input: String) -> Result<()> {
    if let Ok(url) = input.parse::<reqwest::Url>() {
        print_archive_reader(&mut ReaderRemote::from_url(url)).await
    } else {
        let mut file = File::open(&input).await?;
        print_archive_reader(&mut file).await
    }
}
