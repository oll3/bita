use chunker_utils::HashBuf;
use lzma::LzmaWriter;
use sha2::{Digest, Sha512};
use std::collections::{HashMap, HashSet};
use std::io::prelude::*;
use std::io::SeekFrom;
use string_utils::*;

use chunker;
use file_format;

pub struct ArchiveReader<T> {
    input: T,

    // Go from chunk hash to archive chunk index (chunks vector)
    chunk_map: HashMap<HashBuf, usize>,

    // List of chunk descriptors
    chunks: Vec<file_format::ChunkDescriptor>,

    pub archive_chunks_offset: u64,

    // Size of the original source file
    pub source_total_size: u64,

    // Chunker parameters used when this archive was created
    pub avg_chunk_size: usize,
    pub min_chunk_size: usize,
    pub max_chunk_size: usize,
    pub hash_window_size: usize,
    pub hash_length: usize,

    // Statistics - Total bytes read from archive
    pub total_read: u64,
}

impl<T> ArchiveReader<T>
where
    T: Read + Seek,
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
        };

        // Extract and store parameters from file header
        let source_total_size = header_v1.source_total_size;
        let avg_chunk_size = header_v1.avg_chunk_size;
        let min_chunk_size = header_v1.min_chunk_size;
        let max_chunk_size = header_v1.max_chunk_size;
        let hash_window_size = header_v1.hash_window_size;
        let hash_length = header_v1.hash_length;

        let mut chunk_order: Vec<file_format::ChunkDescriptor> = Vec::new();
        let mut chunk_map: HashMap<HashBuf, usize> = HashMap::new();
        {
            let mut index = 0;
            for desc in header_v1.chunk_descriptors {
                chunk_map.insert(desc.hash.clone(), index);
                chunk_order.push(desc);
                index += 1;
            }
        }

        // The input file's offset should be where the actual chunk data
        // starts now. Store it.
        let archive_chunks_offset = input.seek(SeekFrom::Current(0)).expect("seek");

        return ArchiveReader {
            input: input,
            chunk_map: chunk_map,
            chunks: chunk_order,
            source_total_size: source_total_size,
            archive_chunks_offset: archive_chunks_offset,
            avg_chunk_size: avg_chunk_size,
            min_chunk_size: min_chunk_size,
            max_chunk_size: max_chunk_size,
            hash_window_size: hash_window_size,
            hash_length: hash_length,
            total_read: 0,
        };
    }

    // Get a set of all chunks present in archive
    pub fn chunk_hash_set(&self) -> HashSet<HashBuf> {
        self.chunk_map.iter().map(|x| x.0.clone()).collect()
    }

    // Get source offsets of a chunk
    pub fn chunk_source_offsets(&self, hash: &HashBuf) -> Vec<u64> {
        if let Some(index) = self.chunk_map.get(hash) {
            return self.chunks[*index].source_offsets.clone();
        } else {
            vec![]
        }
    }

    // Get chunk data for listed chunks if present in archive
    pub fn read_chunk_data<F>(&mut self, chunks: &HashSet<HashBuf>, mut result: F)
    where
        F: FnMut(&chunker::Chunk),
    {
        let mut index = 0;
        let mut descriptors: Vec<(usize, &file_format::ChunkDescriptor)> = Vec::new();
        for chunk in &self.chunks {
            // Create list of all chunks which we need to read from archive
            if chunks.contains(&chunk.hash) {
                descriptors.push((index, &chunk));
            }
            index += 1;
        }

        // TODO: Merge chunks which are in exaxt sequence in archive so that we can
        // do as few reads/request from source file.
        let mut chunk_buf = chunker::Chunk {
            offset: 0,
            data: vec![],
        };
        let mut comp_buf: Vec<u8> = vec![];
        let mut hasher = Sha512::new();

        for (index, cd) in descriptors {
            // Set input file pointer to archive offset
            self.input
                .seek(SeekFrom::Start(
                    self.archive_chunks_offset + cd.archive_offset,
                )).expect("seek offset");

            if cd.compressed {
                // Archived chunk is compressed.
                // Read compressed data and unpack into chunk buffer.
                chunk_buf.data.resize(0, 0);
                comp_buf.resize(cd.archive_size as usize, 0);
                self.input
                    .read_exact(&mut comp_buf)
                    .expect("read compressed archive data");

                {
                    // Decompress data
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
                    index,
                    HexSlice::new(&cd.hash),
                    size_to_str(&(cd.archive_size as usize)),
                    size_to_str(&chunk_buf.data.len()),
                    cd.source_offsets
                );

                self.total_read += cd.archive_size;
            } else {
                // Archived chunk is NOT compressed.
                chunk_buf.data.resize(cd.archive_size as usize, 0);
                self.input
                    .read_exact(&mut chunk_buf.data)
                    .expect("read archive data");
                println!(
                    "Chunk {} '{}', size {}, uncompressed, insert at {:?}",
                    index,
                    HexSlice::new(&cd.hash),
                    size_to_str(&chunk_buf.data.len()),
                    cd.source_offsets
                );

                self.total_read += cd.archive_size;
            }

            // Chunk buffer is filled. Verify chunk data by chunk hash.
            hasher.input(&chunk_buf.data);
            let hash = hasher.result_reset().to_vec();
            if hash[0..self.hash_length] != cd.hash[0..self.hash_length] {
                panic!(
                    "Chunk hash mismatch (expected: {}, got: {})",
                    HexSlice::new(&cd.hash[0..self.hash_length]),
                    HexSlice::new(&hash[0..self.hash_length])
                );
            }

            // For each offset where this chunk was found in source
            for offset in &cd.source_offsets {
                chunk_buf.offset = *offset as usize;
                result(&chunk_buf);
            }
        }
    }
}
