use protobuf::Message;

use blake2::{Blake2b, Digest};
use std::fmt;

use crate::chunk_dictionary;
use crate::chunker_utils::HashBuf;
use crate::compression::Compression;
use crate::errors::*;
use crate::string_utils::*;

pub struct ChunkDescriptor {
    pub checksum: HashBuf,
    pub archive_size: u32,
    pub archive_offset: u64,
    pub source_size: u32,
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
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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

fn size_vec(s: u64) -> [u8; 8] {
    [
        ((s >> 56) & 0xff) as u8,
        ((s >> 48) & 0xff) as u8,
        ((s >> 40) & 0xff) as u8,
        ((s >> 32) & 0xff) as u8,
        ((s >> 24) & 0xff) as u8,
        ((s >> 16) & 0xff) as u8,
        ((s >> 8) & 0xff) as u8,
        (s & 0xff) as u8,
    ]
}

pub fn vec_to_size(sv: &[u8]) -> u64 {
    (u64::from(sv[0]) << 56)
        | (u64::from(sv[1]) << 48)
        | (u64::from(sv[2]) << 40)
        | (u64::from(sv[3]) << 32)
        | (u64::from(sv[4]) << 24)
        | (u64::from(sv[5]) << 16)
        | (u64::from(sv[6]) << 8)
        | u64::from(sv[7])
}

pub fn build_header(dictionary: &chunk_dictionary::ChunkDictionary) -> Result<Vec<u8>> {
    let mut header: Vec<u8> = vec![];
    let mut hasher = Blake2b::new();
    let mut dictionary_buf: Vec<u8> = Vec::new();

    dictionary
        .write_to_vec(&mut dictionary_buf)
        .chain_err(|| "failed to serialize header")?;

    // header magic
    header.extend(b"bita");

    // Major archive version
    header.push(0);

    // Chunk dictionary size
    header.extend(&size_vec(dictionary_buf.len() as u64));

    // The chunk dictionary
    header.extend(dictionary_buf);

    // Chunk data offset. 0 if not used.
    // For now it will always start where the header hash ends, that is current size
    // of header buffer + 8 for this size value + 64 for header hash value
    {
        let offset = header.len() + 8 + 64;
        println!("Chunk data offset: {}", offset);
        header.extend(&size_vec(offset as u64));
    }

    // Create and set hash of full header
    hasher.input(&header);
    let hash = hasher.result().to_vec();
    println!("Header hash: {}", HexSlice::new(&hash));
    header.extend(hash);

    Ok(header)
}
