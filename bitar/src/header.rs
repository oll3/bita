use blake2::{Blake2b, Digest};
use prost::Message;

use crate::chunk_dictionary::ChunkDictionary;
use crate::error::Error;

/// Bita archive file magic
pub const ARCHIVE_MAGIC: &[u8; 6] = b"BITA1\0";

/// Pre header is the file magic + the size of the dictionary length value (u64)
pub const PRE_HEADER_SIZE: usize = 6 + std::mem::size_of::<u64>();

/// Header structure looks like
/// Offset | Size | Description
/// 0        6      Archive file magic (BITA1\0).
/// 6        8      Dictionary size (u64 le).
/// 14       n      Protobuf encoded dictionary.
/// n        8      Chunk data offset in archive, absolute from archive start (u64 le).
/// n + 8    64     Full header checksum (blake2), from offset 0 to n + 8.

/// Build header from dictionary
pub fn build_header(
    dictionary: &ChunkDictionary,
    chunk_data_offset: Option<u64>,
) -> Result<Vec<u8>, Error> {
    let mut header: Vec<u8> = vec![];
    let mut hasher = Blake2b::new();
    let mut dictionary_buf: Vec<u8> = Vec::new();

    dictionary.encode(&mut dictionary_buf)?;

    // File magic indicating bita archive version 1
    header.extend(ARCHIVE_MAGIC);

    // Chunk dictionary size
    header.extend(&(dictionary_buf.len() as u64).to_le_bytes());

    // The chunk dictionary
    header.extend(dictionary_buf);

    // Start of archive chunk data, absolute to the archive start
    let offset = match chunk_data_offset {
        Some(o) => o,
        None => header.len() as u64 + 8 + 64,
    };
    header.extend(&(offset as u64).to_le_bytes());

    // Create and store hash of full header
    hasher.input(&header);
    header.extend(&hasher.result());

    Ok(header)
}
