use log::*;
use std::path::Path;

use crate::config;
use bita::archive_reader::ArchiveReader;
use bita::error::Error;
use bita::reader_backend;
use bita::string_utils::*;

pub async fn print_archive_backend(builder: reader_backend::Builder) -> Result<(), Error> {
    let archive = ArchiveReader::init(builder).await?;
    print_archive(&archive);
    Ok(())
}

pub fn print_archive(archive: &ArchiveReader) {
    info!("Archive: ");
    info!("  Version: {}", archive.created_by_app_version);
    info!(
        "  Chunk minimum size: {}",
        size_to_str(archive.chunker_params.min_chunk_size),
    );
    info!(
        "  Chunk maximum size: {}",
        size_to_str(archive.chunker_params.max_chunk_size),
    );
    info!(
        "  Chunk average target size: {} (mask: {:#b})",
        size_to_str(archive.chunker_params.chunk_target_average()),
        archive.chunker_params.filter_mask(),
    );
    info!("  Chunk compression: {}", archive.chunk_compression);
    info!("  Chunk hash length: {} bytes", archive.hash_length);
    info!(
        "  Hash window size: {}",
        size_to_str(archive.chunker_params.buzhash_window_size)
    );
    info!(
        "  Archive size: {}",
        size_to_str(archive.compressed_size() + archive.header_size as u64)
    );
    info!("  Header checksum: {}", archive.header_checksum);

    info!("Source:");
    info!("  Source checksum: {}", archive.source_checksum);
    info!(
        "  Chunks in source: {} (unique: {})",
        archive.total_chunks(),
        archive.unique_chunks()
    );
    info!(
        "  Average chunk size: {}",
        size_to_str(
            archive
                .chunk_descriptors
                .iter()
                .map(|cdesc| u64::from(cdesc.source_size))
                .sum::<u64>()
                / archive.chunk_descriptors.len() as u64
        )
    );
    info!("  Source size: {}", size_to_str(archive.source_total_size));
}

pub async fn run(config: config::InfoConfig) -> Result<(), Error> {
    let builder = if &config.input[0..7] == "http://" || &config.input[0..8] == "https://" {
        reader_backend::Builder::new_remote(config.input.parse().unwrap(), 0, None, None)
    } else {
        reader_backend::Builder::new_local(&Path::new(&config.input))
    };
    print_archive_backend(builder).await?;
    Ok(())
}
