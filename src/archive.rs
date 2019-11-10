use protobuf::Message;

use blake2::{Blake2b, Digest};
use std::fmt;
use std::mem;

use crate::chunk_dictionary;
use crate::compression::Compression;
use crate::error::Error;
use crate::string_utils::*;

pub const BUZHASH_SEED: u32 = 0x1032_4195;

pub type HashBuf = Vec<u8>;

// Bita archive file magic
pub const FILE_MAGIC: &[u8; 6] = b"BITA1\0";

// Pre header is the file magic + the size of the dictionary length value (u64)
pub const PRE_HEADER_SIZE: usize = 6 + mem::size_of::<u64>();

#[derive(Clone)]
pub struct ChunkDescriptor {
    pub checksum: HashBuf,
    pub archive_size: u32,
    pub archive_offset: u64,
    pub source_size: u32,
}

impl From<ChunkDescriptor> for chunk_dictionary::ChunkDescriptor {
    fn from(dict: ChunkDescriptor) -> Self {
        chunk_dictionary::ChunkDescriptor {
            checksum: dict.checksum,
            archive_size: dict.archive_size,
            archive_offset: dict.archive_offset,
            source_size: dict.source_size,
            unknown_fields: std::default::Default::default(),
            cached_size: std::default::Default::default(),
        }
    }
}

impl From<chunk_dictionary::ChunkDescriptor> for ChunkDescriptor {
    fn from(dict: chunk_dictionary::ChunkDescriptor) -> Self {
        ChunkDescriptor {
            checksum: dict.checksum,
            archive_size: dict.archive_size,
            archive_offset: dict.archive_offset,
            source_size: dict.source_size,
        }
    }
}

impl fmt::Display for chunk_dictionary::ChunkDictionary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "version: {}, chunks: {}, source hash: {}, source size: {}, compression: {}",
            self.application_version,
            self.chunk_descriptors.len(),
            HexSlice::new(&self.source_checksum),
            size_to_str(self.source_total_size),
            Compression::from(self.get_chunk_compression().clone())
        )
    }
}

pub fn u64_from_le_slice(v: &[u8]) -> u64 {
    let mut tmp: [u8; 8] = Default::default();
    tmp.copy_from_slice(v);
    u64::from_le_bytes(tmp)
}

pub fn build_header(
    dictionary: &chunk_dictionary::ChunkDictionary,
    chunk_data_offset: Option<u64>,
) -> Result<Vec<u8>, Error> {
    let mut header: Vec<u8> = vec![];
    let mut hasher = Blake2b::new();
    let mut dictionary_buf: Vec<u8> = Vec::new();

    dictionary
        .write_to_vec(&mut dictionary_buf)
        .map_err(|e| ("failed to serialize header", e))?;

    // File magic indicating bita archive version 1
    header.extend(FILE_MAGIC);

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
