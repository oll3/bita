use blake2::{Blake2b, Digest};
use prost::Message;
use std::mem;

use crate::chunk_dictionary::{
    chunker_parameters::ChunkingAlgorithm, ChunkDictionary, ChunkerParameters,
};
use crate::chunker::{ChunkerConfig, HashConfig, HashFilterBits};
use crate::error::Error;

pub const BUZHASH_SEED: u32 = 0x1032_4195;

// Bita archive file magic
pub const FILE_MAGIC: &[u8; 6] = b"BITA1\0";

// Pre header is the file magic + the size of the dictionary length value (u64)
pub const PRE_HEADER_SIZE: usize = 6 + mem::size_of::<u64>();

impl std::convert::TryFrom<ChunkerParameters> for ChunkerConfig {
    type Error = Error;
    fn try_from(p: ChunkerParameters) -> Result<Self, Self::Error> {
        match ChunkingAlgorithm::from_i32(p.chunking_algorithm) {
            Some(ChunkingAlgorithm::Buzhash) => Ok(ChunkerConfig::BuzHash(HashConfig {
                filter_bits: HashFilterBits(p.chunk_filter_bits),
                min_chunk_size: p.min_chunk_size as usize,
                max_chunk_size: p.max_chunk_size as usize,
                window_size: p.rolling_hash_window_size as usize,
            })),
            Some(ChunkingAlgorithm::Rollsum) => Ok(ChunkerConfig::RollSum(HashConfig {
                filter_bits: HashFilterBits(p.chunk_filter_bits),
                min_chunk_size: p.min_chunk_size as usize,
                max_chunk_size: p.max_chunk_size as usize,
                window_size: p.rolling_hash_window_size as usize,
            })),
            Some(ChunkingAlgorithm::FixedSize) => {
                Ok(ChunkerConfig::FixedSize(p.max_chunk_size as usize))
            }
            _ => Err(Error::UnknownChunkingAlgorithm),
        }
    }
}

pub fn u64_from_le_slice(v: &[u8]) -> u64 {
    let mut tmp: [u8; 8] = Default::default();
    tmp.copy_from_slice(v);
    u64::from_le_bytes(tmp)
}

pub fn build_header(
    dictionary: &ChunkDictionary,
    chunk_data_offset: Option<u64>,
) -> Result<Vec<u8>, Error> {
    let mut header: Vec<u8> = vec![];
    let mut hasher = Blake2b::new();
    let mut dictionary_buf: Vec<u8> = Vec::new();

    dictionary.encode(&mut dictionary_buf)?;

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
