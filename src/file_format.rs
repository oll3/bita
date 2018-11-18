use bincode::serialize_into;
use sha2::{Digest, Sha512};

const FORMAT_VERSION: u8 = 0;

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ChunkDescriptor {
    pub hash: Vec<u8>,
    pub offset: u64,
    pub chunk_size: u64,
    pub compressed: bool,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Compression {
    LZMA,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct FileHeader {
    pub build_index: Vec<u64>,
    pub chunk_lookup: Vec<ChunkDescriptor>,
    pub compression: Compression,
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

pub fn build_header(header: &FileHeader) -> Vec<u8> {
    // header magic number + version
    let magic = vec![0x63, 0x10, 0x9b, 0x41];
    let mut file_buf: Vec<u8> = vec![];
    let mut hasher = Sha512::new();
    let mut header_buf: Vec<u8> = Vec::new();
    serialize_into(&mut header_buf, header).expect("serialize");
    hasher.input(&header_buf);

    file_buf.extend(magic);
    file_buf.push(FORMAT_VERSION);
    file_buf.extend(&size_vec(header_buf.len() as u64));
    file_buf.extend(header_buf);
    file_buf.extend(hasher.result().to_vec());

    file_buf
}
