use protobuf::Message;

use blake2::{Blake2b, Digest};
use chunker_utils::HashBuf;
use std::fmt;

use chunk_dictionary;
use errors::*;
use string_utils::*;

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub enum Compression {
    None,
    LZMA(u32),
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct ChunkDescriptor {
    // Hash of (uncompressed) chunk
    pub hash: HashBuf,

    // Chunk data compression
    pub compression: Compression,

    // Offset in archive
    pub archive_offset: u64,
    pub archive_size: u64,

    // Chunk source size - That is the size of the uncompressed chunk
    pub source_size: u64,
    pub source_offsets: Vec<u64>,
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub enum ChunkDataLocation {
    // Chunk data is located inside the archive
    Internal,
    // TODO: Chunk data is located in a separate file (at path)
    // External(string),
    // TODO: Chunk data is separated per chunk and named according to the
    // chunk hash (casync style).
    // The string is a path to where chunks are located.
    //PerChunk(string),
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct HeaderV1 {
    // Hash of the source file
    pub source_hash: HashBuf,

    // Total size of the source file
    pub source_total_size: u64,

    // Array of chunk descriptors. In order of (first) occurrence in source file.
    pub chunk_descriptors: Vec<ChunkDescriptor>,

    // Where does archive chunk data live
    pub chunk_data_location: ChunkDataLocation,

    // Chunker parameters used to create the archive
    pub chunk_filter_bits: u32,
    pub min_chunk_size: usize,
    pub max_chunk_size: usize,
    pub hash_window_size: usize,
    pub hash_length: usize,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Version {
    V1(HeaderV1),
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Header {
    pub version: Version,
}

impl fmt::Display for chunk_dictionary::ChunkDictionary {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "app version: {}, chunks: {}, source hash: {}, source size: {}",
            self.application_version,
            self.chunk_descriptors.len(),
            HexSlice::new(&self.source_checksum),
            size_to_str(self.source_total_size)
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

impl fmt::Display for chunk_dictionary::ChunkDescriptor_oneof_compression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            chunk_dictionary::ChunkDescriptor_oneof_compression::LZMA(lvl) => {
                write!(f, "LZMA({})", lvl)
            }
        }
    }
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
