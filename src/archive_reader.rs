use lzma::LzmaWriter;
use sha2::{Digest, Sha512};
use std::io::prelude::*;
use string_utils::*;

use chunker;
use file_format;

pub struct ArchiveReader<T> {
    input: T,
    header: file_format::ArchiveHeaderV1,
    chunk_index: usize,
}

impl<T> ArchiveReader<T>
where
    T: Read,
{
    pub fn verify_pre_header(pre_header: &[u8]) -> Result<usize, &'static str> {
        if pre_header.len() < 12 {
            return Err("Failed to read header of archive");
        }
        if &pre_header[0..4] != "bita".as_bytes() {
            return Err("Not an archive");
        }

        let header_size = file_format::vec_to_size(&pre_header[4..12]) as usize;
        return Ok(header_size);
    }

    pub fn new(mut input: T) -> Self {
        // Read the pre-header (file magic, version and size)
        let mut header_buf = vec![0; 12];
        input
            .read_exact(&mut header_buf)
            .expect("read archive pre-header");

        let header_size =
            Self::verify_pre_header(&header_buf[0..12]).expect("verify archive header");

        println!("archive header size: {}", header_size);

        // Read the header
        header_buf.resize(header_size, 0);
        input
            .read_exact(&mut header_buf)
            .expect("read archive header");

        // ...and deserialize it
        let header: file_format::ArchiveHeader =
            bincode::deserialize(&header_buf).expect("unpack header");

        // Verify the header against the header hash
        let mut hasher = Sha512::new();
        hasher.input(&header_buf);

        header_buf.resize(64, 0);
        input.read_exact(&mut header_buf).expect("read header hash");

        if header_buf != hasher.result().to_vec() {
            panic!("Corrupt archive header");
        }

        println!("{}", header);

        let header_v1 = match header.version {
            file_format::ArchiveVersion::V1(v1) => v1,
            _ => panic!("Unknown archive version"),
        };

        return ArchiveReader {
            input: input,
            header: header_v1,
            chunk_index: 0,
        };
    }

    pub fn source_total_size(&self) -> u64 {
        self.header.source_total_size
    }

    // Iterate over chunks. TODO: Use iterator.
    pub fn iter_chunks<F>(&mut self, mut result: F)
    where
        F: FnMut(&chunker::Chunk),
    {
        let mut chunk_buf = chunker::Chunk {
            offset: 0,
            data: vec![],
        };
        let mut hasher = Sha512::new();
        let mut comp_buf: Vec<u8> = vec![];

        // For each chunk
        while self.chunk_index < self.header.chunk_descriptors.len() {
            let cd = &self.header.chunk_descriptors[self.chunk_index];

            if cd.compressed {
                // Archived chunk is compressed.
                // Read compressed data and unpack into chunk buffer.
                chunk_buf.data.resize(0, 0);
                comp_buf.resize(cd.archive_size as usize, 0);
                let rc = self
                    .input
                    .read_exact(&mut comp_buf)
                    .expect("read compressed archive data");

                {
                    let mut f = LzmaWriter::new_decompressor(&mut chunk_buf.data)
                        .expect("new decompressor");
                    let mut wc = 0;
                    while wc < comp_buf.len() {
                        wc += f.write(&comp_buf[wc..]).expect("write decompressor");
                    }
                    f.finish().expect("finish decompressor");
                }
                println!(
                    "Chunk {} '{}', size {}, decompressed to {}, insert at {:?}",
                    self.chunk_index,
                    HexSlice::new(&cd.hash),
                    size_to_str(&(cd.archive_size as usize)),
                    size_to_str(&chunk_buf.data.len()),
                    cd.source_offsets
                );
            } else {
                // Archived chunk is NOT compressed.
                chunk_buf.data.resize(cd.source_size as usize, 0);
                self.input
                    .read_exact(&mut chunk_buf.data)
                    .expect("read archive data");

                println!(
                    "Chunk {} '{}', size {}, uncompressed, insert at {:?}",
                    self.chunk_index,
                    HexSlice::new(&cd.hash),
                    size_to_str(&chunk_buf.data.len()),
                    cd.source_offsets
                );
            }

            // Chunk buffer is filled. Verify chunk data by chunk hash.
            hasher.input(&chunk_buf.data);
            let hash = hasher.result_reset().to_vec();
            if hash[0..cd.hash.len()] != cd.hash[0..] {
                panic!("Hash mismatch at chunk index {}", self.chunk_index);
            }

            for offset in &cd.source_offsets {
                chunk_buf.offset = *offset as usize;
                result(&chunk_buf);
            }
            self.chunk_index += 1;
        }
    }
}
