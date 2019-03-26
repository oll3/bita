use std::fs::File;

use crate::archive_reader::*;
use crate::config;
use crate::errors::*;
use crate::remote_archive_backend::RemoteReader;
use crate::string_utils::*;

fn print_archive_info<T>(mut archive_backend: T) -> Result<()>
where
    T: ArchiveBackend,
{
    let archive = ArchiveReader::try_init(&mut archive_backend, &mut Vec::new())?;
    println!(
        "Created with bita version: {}",
        archive.created_by_app_version
    );
    println!(
        "Chunk target - min: {}, max: {}, filter mask: {:#b}, hash window size: {}",
        archive.chunker_params.min_chunk_size,
        archive.chunker_params.max_chunk_size,
        archive.chunker_params.filter_bits,
        archive.chunker_params.buzhash_window_size
    );
    println!(
        "Header checksum: {}",
        HexSlice::new(&archive.header_checksum)
    );
    println!(
        "Source checksum: {}",
        HexSlice::new(&archive.source_checksum)
    );
    println!(
        "Chunks in source: {} (unique: {})",
        archive.total_chunks(),
        archive.unique_chunks()
    );
    println!("Chunk compression: {}", archive.chunk_compression);
    //println!("Unique chunks: {}", archive.unique_chunks());
    println!(
        "Source total size: {}",
        size_to_str(archive.source_total_size)
    );
    println!(
        "Compressed size: {}",
        size_to_str(archive.compressed_size() + archive.header_size as u64)
    );

    Ok(())
}

pub fn run(config: &config::InfoConfig) -> Result<()> {
    if &config.input[0..7] == "http://" || &config.input[0..8] == "https://" {
        let remote_source = RemoteReader::new(&config.input);
        print_archive_info(remote_source)?;
    } else {
        let local_file =
            File::open(&config.input).chain_err(|| format!("unable to open {}", config.input))?;
        print_archive_info(local_file)?;
    }

    Ok(())
}
