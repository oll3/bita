use chunker_utils::HashBuf;
use lzma::LzmaWriter;
use sha2::{Digest, Sha512};
use std::collections::{HashMap, HashSet};
use std::io;
use std::io::prelude::*;
use string_utils::*;

use archive;
use chunker;

pub struct ArchiveReader<T> {
    input: T,

    // Go from chunk hash to archive chunk index (chunks vector)
    chunk_map: HashMap<HashBuf, usize>,

    // List of chunk descriptors
    chunks: Vec<archive::ChunkDescriptor>,

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

// Trait to implement for archive backends
pub trait ArchiveBackend {
    // Read from archive into the given buffer.
    // Should read the exact number of bytes of the given buffer and start read at
    // given offset.
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> io::Result<()>;

    // Read and return chunked data
    fn read_in_chunks<F: FnMut(Vec<u8>)>(
        &mut self,
        start_offset: u64,
        chunk_sizes: &Vec<u64>,
        chunk_callback: F,
    ) -> io::Result<()>;
}

impl<T> ArchiveReader<T>
where
    T: ArchiveBackend,
{
    pub fn verify_pre_header(pre_header: &[u8]) -> Result<usize, &'static str> {
        if pre_header.len() < 12 {
            return Err("Failed to read header of archive");
        }
        if &pre_header[0..4] != "bita".as_bytes() {
            return Err("Not an archive");
        }

        let header_size = archive::vec_to_size(&pre_header[4..12]) as usize;
        return Ok(header_size);
    }

    pub fn new(mut input: T) -> Self {
        // Read the pre-header (file magic, version and size)
        let mut input_offset: u64 = 0;
        let mut header_buf = vec![0; 12];
        input
            .read_at(0, &mut header_buf)
            .expect("read archive pre-header");

        input_offset += 12;

        let header_size =
            Self::verify_pre_header(&header_buf[0..12]).expect("verify archive header");

        println!("archive header size: {}", header_size);

        // Read the header
        header_buf.resize(header_size, 0);
        input
            .read_at(input_offset, &mut header_buf)
            .expect("read archive header");
        input_offset += header_size as u64;

        // ...and deserialize it
        let header: archive::Header = bincode::deserialize(&header_buf).expect("unpack header");

        // Verify the header against the header hash
        let mut hasher = Sha512::new();
        hasher.input(&header_buf);

        header_buf.resize(64, 0);
        input
            .read_at(input_offset, &mut header_buf)
            .expect("read header hash");
        input_offset += 64;

        if header_buf != hasher.result().to_vec() {
            panic!("Corrupt archive header");
        }

        println!("{}", header);

        let header_v1 = match header.version {
            archive::Version::V1(v1) => v1,
        };

        // Extract and store parameters from file header
        let source_total_size = header_v1.source_total_size;
        let avg_chunk_size = header_v1.avg_chunk_size;
        let min_chunk_size = header_v1.min_chunk_size;
        let max_chunk_size = header_v1.max_chunk_size;
        let hash_window_size = header_v1.hash_window_size;
        let hash_length = header_v1.hash_length;

        let mut chunk_order: Vec<archive::ChunkDescriptor> = Vec::new();
        let mut chunk_map: HashMap<HashBuf, usize> = HashMap::new();
        {
            let mut index = 0;
            for desc in header_v1.chunk_descriptors {
                chunk_map.insert(desc.hash.clone(), index);
                chunk_order.push(desc);
                index += 1;
            }
        }

        return ArchiveReader {
            input: input,
            chunk_map: chunk_map,
            chunks: chunk_order,
            source_total_size: source_total_size,
            archive_chunks_offset: input_offset,
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

    // Group chunks which are placed in sequence inside archive
    fn group_chunks_in_sequence(
        mut chunks: Vec<&archive::ChunkDescriptor>,
    ) -> Vec<Vec<&archive::ChunkDescriptor>> {
        let mut group_list = vec![];
        if chunks.len() == 0 {
            return group_list;
        }
        let mut group = vec![chunks.remove(0)];
        while chunks.len() > 0 {
            let chunk = chunks.remove(0);

            let prev_chunk_end =
                group.last().unwrap().archive_offset + group.last().unwrap().archive_size;

            if prev_chunk_end == chunk.archive_offset {
                // Chunk is placed right next to the previous chunk
                group.push(chunk);
            } else {
                group_list.push(group);
                group = vec![chunk];
            }
        }
        group_list.push(group);
        return group_list;
    }

    // Get chunk data for all listed chunks if present in archive
    pub fn read_chunk_data<F>(&mut self, chunks: &HashSet<HashBuf>, mut result: F)
    where
        F: FnMut(&chunker::Chunk),
    {
        // Create list of chunks which are in archive. The order of the list should
        // be the same otder as the chunk data in archive.
        let descriptors: Vec<&archive::ChunkDescriptor> = self
            .chunks
            .iter()
            .filter(|chunk| chunks.contains(&chunk.hash))
            .collect();

        // Create groups of chunks so that we can make a single request for all chunks
        // which are placed in sequence in archive.
        let grouped_chunks = Self::group_chunks_in_sequence(descriptors.clone());

        let mut hasher = Sha512::new();
        let mut chunk_buf = chunker::Chunk {
            offset: 0,
            data: vec![],
        };

        let hash_length = self.hash_length;
        let mut total_read = 0;

        for group in grouped_chunks {
            let start_offset = self.archive_chunks_offset + group[0].archive_offset;
            let chunk_sizes = group.iter().map(|c| c.archive_size).collect();

            let mut chunk_index = 0;
            self.input
                .read_in_chunks(start_offset, &chunk_sizes, |archive_data| {
                    let cd = &group[chunk_index];
                    if cd.compressed {
                        // Archived chunk is compressed
                        chunk_buf.data.resize(0, 0);
                        {
                            // Decompress archive data
                            let mut f = LzmaWriter::new_decompressor(&mut chunk_buf.data)
                                .expect("new decompressor");
                            let mut wc = 0;
                            while wc < archive_data.len() {
                                wc += f.write(&archive_data[wc..]).expect("write decompressor");
                            }
                            f.finish().expect("finish decompressor");
                        }
                        println!(
                            "Chunk '{}', size {}, decompressed to {}, insert at {:?}",
                            HexSlice::new(&cd.hash),
                            size_to_str(&(cd.archive_size as usize)),
                            size_to_str(&chunk_buf.data.len()),
                            cd.source_offsets
                        );
                        total_read += cd.archive_size;
                    } else {
                        // Archived chunk is NOT compressed
                        chunk_buf.data = archive_data;
                        println!(
                            "Chunk '{}', size {}, uncompressed, insert at {:?}",
                            HexSlice::new(&cd.hash),
                            size_to_str(&chunk_buf.data.len()),
                            cd.source_offsets
                        );
                        total_read += cd.archive_size;
                    }

                    // Verify data by hash
                    hasher.input(&chunk_buf.data);
                    let hash = hasher.result_reset().to_vec();
                    if hash[0..hash_length] != cd.hash[0..hash_length] {
                        panic!(
                            "Chunk hash mismatch (expected: {}, got: {})",
                            HexSlice::new(&cd.hash[0..hash_length]),
                            HexSlice::new(&hash[0..hash_length])
                        );
                    }

                    // For each offset where this chunk was found in source
                    for offset in &cd.source_offsets {
                        chunk_buf.offset = *offset as usize;
                        result(&chunk_buf);
                    }

                    chunk_index += 1;
                }).expect("read chunks");
        }
        self.total_read = total_read;
        return;
    }
}
