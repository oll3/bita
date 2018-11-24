use bincode::serialize_into;
use chunker_utils::HashBuf;
use sha2::{Digest, Sha512};
use std::fmt;
use string_utils::*;

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct ChunkDescriptor {
    // Hash of (uncompressed) chunk
    pub hash: HashBuf,

    // Is chunk compressed
    pub compressed: bool,

    // Offset in archive
    pub archive_offset: u64,
    pub archive_size: u64,

    // Chunk source size - That is the size of the uncompressed chunk
    pub source_size: u64,
    pub source_offsets: Vec<u64>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Compression {
    LZMA,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ArchiveHeaderV1 {
    // Hash of the source file
    pub source_hash: HashBuf,

    // Total size of the source file
    pub source_total_size: u64,

    // Array of chunk descriptors. In order of (first) occurrence in source file.
    pub chunk_descriptors: Vec<ChunkDescriptor>,

    // Compression used for compressed chunks in archive
    pub compression: Compression,

    // Chunker parameters used to create the archive
    pub avg_chunk_size: usize,
    pub min_chunk_size: usize,
    pub max_chunk_size: usize,
    pub hash_window_size: usize,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum ArchiveVersion {
    V1(ArchiveHeaderV1),
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ArchiveHeader {
    pub version: ArchiveVersion,
}

impl fmt::Display for ArchiveHeader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.version {
            ArchiveVersion::V1(ref v1) => write!(
                f,
                "chunks: {}, compression: {:?}, source hash: {}, source size: {}",
                v1.chunk_descriptors.len(),
                v1.compression,
                HexSlice::new(&v1.source_hash),
                v1.source_total_size
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

pub fn build_header(header: &ArchiveHeader) -> Vec<u8> {
    // header magic
    let magic = "bita".as_bytes();
    let mut file_buf: Vec<u8> = vec![];
    let mut hasher = Sha512::new();
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
