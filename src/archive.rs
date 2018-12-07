use bincode::serialize_into;
use blake2::{Blake2b, Digest};
use chunker_utils::HashBuf;
use std::fmt;
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
    // The strings is a path to where chunks are located.
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

impl fmt::Display for Header {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.version {
            Version::V1(ref v1) => write!(
                f,
                "chunks: {}, source hash: {}, source size: {}",
                v1.chunk_descriptors.len(),
                HexSlice::new(&v1.source_hash),
                size_to_str(v1.source_total_size)
            ),
        }
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
    ((sv[0] as u64) << 56)
        | ((sv[1] as u64) << 48)
        | ((sv[2] as u64) << 40)
        | ((sv[3] as u64) << 32)
        | ((sv[4] as u64) << 24)
        | ((sv[5] as u64) << 16)
        | ((sv[6] as u64) << 8)
        | ((sv[7] as u64) << 0)
}

pub fn build_header(header: &Header) -> Vec<u8> {
    // header magic
    let magic = "bita".as_bytes();
    let mut file_buf: Vec<u8> = vec![];
    let mut hasher = Blake2b::new();
    let mut header_buf: Vec<u8> = Vec::new();
    serialize_into(&mut header_buf, header).expect("serialize");
    hasher.input(&header_buf);
    println!("Header size: {}", header_buf.len());

    file_buf.extend(magic);
    file_buf.extend(&size_vec(header_buf.len() as u64));
    file_buf.extend(header_buf);
    file_buf.extend(hasher.result().to_vec());

    file_buf
}
