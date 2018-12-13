use blake2::{Blake2b, Digest};
use chunker_utils::HashBuf;
use lzma::LzmaWriter;
use std::collections::{HashMap, HashSet};
use std::io::prelude::*;
use std::time::{Duration, Instant};
use string_utils::*;

use archive;
use chunk_dictionary;
use chunker;
use errors::*;

pub struct ArchiveReader<T> {
    input: T,

    // Go from chunk hash to archive chunk index (chunks vector)
    chunk_map: HashMap<HashBuf, usize>,

    // List of chunk descriptors
    chunks: Vec<chunk_dictionary::ChunkDescriptor>,

    pub archive_chunks_offset: u64,

    // Size of the original source file
    pub source_total_size: u64,

    // Chunker parameters used when this archive was created
    pub chunk_filter_bits: u32,
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
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<()>;

    // Read and return chunked data
    fn read_in_chunks<F: FnMut(Vec<u8>) -> Result<()>>(
        &mut self,
        start_offset: u64,
        chunk_sizes: &[u64],
        chunk_callback: F,
    ) -> Result<()>;
}

impl<T> ArchiveReader<T>
where
    T: ArchiveBackend,
{
    pub fn verify_pre_header(pre_header: &[u8]) -> Result<()> {
        if pre_header.len() < 5 {
            bail!("failed to read header of archive")
        }
        if &pre_header[0..4] != b"bita" {
            bail!("not an archive")
        }
        if pre_header[4] != 0 {
            bail!("unknown archive version");
        }
        Ok(())
    }

    pub fn init(mut input: T) -> Result<Self> {
        // Read the pre-header (file magic, version and size)
        let mut header_buf = vec![0; 13];
        input
            .read_at(0, &mut header_buf)
            .chain_err(|| "unable to read archive")?;

        Self::verify_pre_header(&header_buf[0..13])?;

        let dictionary_size = archive::vec_to_size(&header_buf[5..13]) as usize;
        println!("Dictionary size: {}", size_to_str(dictionary_size));

        // Read the dictionary, chunk data offset and header hash
        header_buf.resize(13 + dictionary_size + 8 + 64, 0);
        input
            .read_at(13, &mut header_buf[13..])
            .chain_err(|| "unable to read archive")?;

        // Verify the header against the header hash
        let mut hasher = Blake2b::new();
        let offs = 13 + dictionary_size + 8;
        hasher.input(&header_buf[..offs]);
        if header_buf[offs..(offs + 64)] != hasher.result().to_vec()[..] {
            bail!("Corrupt archive header")
        }

        // Deserialize the chunk dictionary
        let offs = 13;
        let dictionary: chunk_dictionary::ChunkDictionary =
            protobuf::parse_from_bytes(&header_buf[offs..(offs + dictionary_size)])
                .chain_err(|| "unable to unpack archive header")?;

        // Get chunk data offset
        let offs = 13 + dictionary_size;
        let chunk_data_offset = archive::vec_to_size(&header_buf[offs..(offs + 8)]) as usize;

        println!("{}", dictionary);

        // Extract and store parameters from file header
        let chunker_params = dictionary.chunker_params.unwrap();
        let source_total_size = dictionary.source_total_size;
        let chunk_filter_bits = chunker_params.chunk_filter_bits;
        let min_chunk_size = chunker_params.min_chunk_size;
        let max_chunk_size = chunker_params.max_chunk_size;
        let hash_window_size = chunker_params.hash_window_size;
        let hash_length = chunker_params.chunk_hash_length;

        let mut chunk_order: Vec<chunk_dictionary::ChunkDescriptor> = Vec::new();
        let mut chunk_map: HashMap<HashBuf, usize> = HashMap::new();

        for (index, desc) in dictionary.chunk_descriptors.into_iter().enumerate() {
            chunk_map.insert(desc.checksum.clone(), index);
            chunk_order.push(desc);
        }

        Ok(ArchiveReader {
            input,
            chunk_map,
            chunks: chunk_order,
            source_total_size,
            archive_chunks_offset: chunk_data_offset as u64,
            chunk_filter_bits,
            min_chunk_size: min_chunk_size as usize,
            max_chunk_size: max_chunk_size as usize,
            hash_window_size: hash_window_size as usize,
            hash_length: hash_length as usize,
            total_read: 0,
        })
    }

    // Get a set of all chunks present in archive
    pub fn chunk_hash_set(&self) -> HashSet<HashBuf> {
        self.chunk_map.iter().map(|x| x.0.clone()).collect()
    }

    // Get source offsets of a chunk
    pub fn chunk_source_offsets(&self, hash: &[u8]) -> Vec<u64> {
        if let Some(index) = self.chunk_map.get(hash) {
            self.chunks[*index].source_offsets.clone()
        } else {
            vec![]
        }
    }

    // Group chunks which are placed in sequence inside archive
    fn group_chunks_in_sequence(
        mut chunks: Vec<&chunk_dictionary::ChunkDescriptor>,
    ) -> Vec<Vec<&chunk_dictionary::ChunkDescriptor>> {
        let mut group_list = vec![];
        if chunks.is_empty() {
            return group_list;
        }
        let mut group = vec![chunks.remove(0)];
        while !chunks.is_empty() {
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
        group_list
    }

    // Get chunk data for all listed chunks if present in archive
    pub fn read_chunk_data<F>(&mut self, chunks: &HashSet<HashBuf>, mut result: F) -> Result<()>
    where
        F: FnMut(&chunker::Chunk) -> Result<()>,
    {
        // Create list of chunks which are in archive. The order of the list should
        // be the same otder as the chunk data in archive.
        let descriptors: Vec<&chunk_dictionary::ChunkDescriptor> = self
            .chunks
            .iter()
            .filter(|chunk| chunks.contains(&chunk.checksum))
            .collect();

        // Create groups of chunks so that we can make a single request for all chunks
        // which are placed in sequence in archive.
        let grouped_chunks = Self::group_chunks_in_sequence(descriptors.clone());

        let mut hasher = Blake2b::new();
        let mut chunk_buf = chunker::Chunk {
            offset: 0,
            data: vec![],
        };

        let hash_length = self.hash_length;
        let mut total_read = 0;

        let mut decompress_time = Duration::new(0, 0);
        let mut verify_time = Duration::new(0, 0);

        for group in grouped_chunks {
            let start_offset = self.archive_chunks_offset + group[0].archive_offset;

            let chunk_sizes: Vec<u64> = group.iter().map(|c| c.archive_size).collect();

            let mut chunk_index = 0;
            self.input
                .read_in_chunks(start_offset, &chunk_sizes, |archive_data| {
                    let cd = &group[chunk_index];
                    match cd.compression {
                        Some(chunk_dictionary::ChunkDescriptor_oneof_compression::LZMA(_level)) => {
                            // Archived chunk is compressed
                            chunk_buf.data.resize(0, 0);
                            {
                                // Decompress archive data
                                //let decomp_start_time = Instant::now();
                                let mut f = LzmaWriter::new_decompressor(&mut chunk_buf.data)
                                    .expect("new decompressor");
                                let mut wc = 0;
                                while wc < archive_data.len() {
                                    let decomp_start_time = Instant::now();
                                    wc += f.write(&archive_data[wc..]).expect("write decompressor");
                                    decompress_time += decomp_start_time.elapsed();
                                }
                                f.finish().expect("finish decompressor");
                            }
                            println!(
                                "Chunk '{}', size {}, decompressed to {}, insert at {:?}",
                                HexSlice::new(&cd.checksum),
                                size_to_str(cd.archive_size),
                                size_to_str(chunk_buf.data.len()),
                                cd.source_offsets
                            );
                            total_read += cd.archive_size;
                        }
                        None => {
                            // Archived chunk is NOT compressed
                            chunk_buf.data = archive_data;
                            println!(
                                "Chunk '{}', size {}, uncompressed, insert at {:?}",
                                HexSlice::new(&cd.checksum),
                                size_to_str(chunk_buf.data.len()),
                                cd.source_offsets
                            );
                            total_read += cd.archive_size;
                        }
                    }

                    // Verify data by hash
                    let verify_start_time = Instant::now();
                    hasher.input(&chunk_buf.data);
                    let checksum = hasher.result_reset().to_vec();
                    if checksum[..hash_length] != cd.checksum[..hash_length] {
                        panic!(
                            "Chunk hash mismatch (expected: {}, got: {})",
                            HexSlice::new(&cd.checksum[0..hash_length]),
                            HexSlice::new(&checksum[0..hash_length])
                        );
                    }
                    verify_time += verify_start_time.elapsed();

                    // For each offset where this chunk was found in source
                    for offset in &cd.source_offsets {
                        chunk_buf.offset = *offset as usize;
                        result(&chunk_buf)?;
                    }

                    chunk_index += 1;

                    Ok(())
                })
                .expect("read chunks");
        }
        println!(
            "Decompression time: {}.{:03} s, verify time: {}.{:03} s",
            decompress_time.as_secs(),
            decompress_time.subsec_millis(),
            verify_time.as_secs(),
            verify_time.subsec_millis()
        );
        self.total_read = total_read;

        Ok(())
    }
}
