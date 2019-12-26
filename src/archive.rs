use protobuf::Message;

use blake2::{Blake2b, Digest};
use std::mem;

use crate::chunk_dictionary;
use crate::chunker::{ChunkerConfig, HashConfig, HashFilterBits};
use crate::error::Error;
use crate::HashSum;

pub const BUZHASH_SEED: u32 = 0x1032_4195;

// Bita archive file magic
pub const FILE_MAGIC: &[u8; 6] = b"BITA1\0";

// Pre header is the file magic + the size of the dictionary length value (u64)
pub const PRE_HEADER_SIZE: usize = 6 + mem::size_of::<u64>();

#[derive(Clone)]
pub struct ChunkDescriptor {
    pub checksum: HashSum,
    pub archive_size: u32,
    pub archive_offset: u64,
    pub source_size: u32,
    pub source_offsets: Vec<u64>,
}

impl From<ChunkDescriptor> for chunk_dictionary::ChunkDescriptor {
    fn from(dict: ChunkDescriptor) -> Self {
        chunk_dictionary::ChunkDescriptor {
            checksum: dict.checksum.to_vec(),
            archive_size: dict.archive_size,
            archive_offset: dict.archive_offset,
            source_size: dict.source_size,
            unknown_fields: std::default::Default::default(),
            cached_size: std::default::Default::default(),
        }
    }
}

impl From<chunk_dictionary::ChunkerParameters> for ChunkerConfig {
    fn from(params: chunk_dictionary::ChunkerParameters) -> Self {
        match params.chunking_algorithm {
            chunk_dictionary::ChunkerParameters_ChunkingAlgorithm::BUZHASH => {
                ChunkerConfig::BuzHash(HashConfig {
                    filter_bits: HashFilterBits(params.chunk_filter_bits),
                    min_chunk_size: params.min_chunk_size as usize,
                    max_chunk_size: params.max_chunk_size as usize,
                    window_size: params.rolling_hash_window_size as usize,
                })
            }
            chunk_dictionary::ChunkerParameters_ChunkingAlgorithm::ROLLSUM => {
                ChunkerConfig::RollSum(HashConfig {
                    filter_bits: HashFilterBits(params.chunk_filter_bits),
                    min_chunk_size: params.min_chunk_size as usize,
                    max_chunk_size: params.max_chunk_size as usize,
                    window_size: params.rolling_hash_window_size as usize,
                })
            }
            chunk_dictionary::ChunkerParameters_ChunkingAlgorithm::FIXED_SIZE => {
                ChunkerConfig::FixedSize(params.max_chunk_size as usize)
            }
        }
    }
}

impl From<(chunk_dictionary::ChunkDescriptor, Vec<u64>)> for ChunkDescriptor {
    fn from((dict, source_offsets): (chunk_dictionary::ChunkDescriptor, Vec<u64>)) -> Self {
        ChunkDescriptor {
            checksum: dict.checksum.into(),
            archive_size: dict.archive_size,
            archive_offset: dict.archive_offset,
            source_size: dict.source_size,
            source_offsets,
        }
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
