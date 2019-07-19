use blake2::{Blake2b, Digest};
use threadpool::ThreadPool;

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io::prelude::*;

use crate::archive;
use crate::chunk_dictionary;
use crate::chunker::ChunkerParams;
use crate::chunker_utils::HashBuf;
use crate::compression::Compression;
use crate::error::Error;
use crate::para_pipe::ParaPipe;
use crate::string_utils::*;

pub struct ArchiveReader {
    // Go from chunk hash to archive chunk index (chunks vector)
    chunk_map: HashMap<HashBuf, usize>,

    // Array of chunk descriptors
    pub chunk_descriptors: Vec<archive::ChunkDescriptor>,

    // Go from archive chunk index to array of source offsets
    chunk_offsets: Vec<Vec<u64>>,

    // The order of chunks in source
    rebuild_order: Vec<usize>,

    // The total archive header size
    pub header_size: usize,

    // Checksum (blake2) of header.
    pub header_checksum: Vec<u8>,

    // Compression used for all chunks
    pub chunk_compression: Compression,

    pub created_by_app_version: String,
    pub archive_chunks_offset: u64,

    // Size of the original source file
    pub source_total_size: u64,
    pub source_checksum: HashBuf,

    // Chunker parameters used when this archive was created
    pub chunker_params: ChunkerParams,
    pub hash_length: usize,
}

impl fmt::Display for ArchiveReader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "build version: {}, chunks: {} (unique: {}), compression: {}, decompressed size: {}, source checksum: {}",
            self.created_by_app_version,
            self.total_chunks(),
            self.unique_chunks(),
            self.chunk_compression,
            size_to_str(self.source_total_size),
            HexSlice::new(&self.source_checksum),
        )
    }
}

// Trait to implement for archive backends.
pub trait ArchiveBackend
where
    Self: Read,
{
    // Read from archive into the given buffer.
    // Should read the exact number of bytes of the given buffer and start read at
    // given offset.
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), Error>;

    // Read and return chunked data
    fn read_in_chunks<F: FnMut(Vec<u8>) -> Result<(), Error>>(
        &mut self,
        start_offset: u64,
        chunk_sizes: &[u64],
        chunk_callback: F,
    ) -> Result<(), Error>;
}

impl ArchiveReader {
    pub fn verify_pre_header(pre_header: &[u8]) -> Result<(), Error> {
        if pre_header.len() < archive::FILE_MAGIC.len() {
            return Err(Error::NotAnArchive(
                "failed to read header of archive".to_owned(),
            ));
        }
        // Allow both leagacy type file magic (prefixed with \0 but no null
        // termination) and new type 'BITA\0'.
        if &pre_header[0..archive::FILE_MAGIC.len()] != archive::FILE_MAGIC
            && &pre_header[0..archive::FILE_MAGIC.len()] != b"\0BITA1"
        {
            return Err(Error::NotAnArchive("invalid file magic".to_owned()));
        }
        Ok(())
    }

    pub fn try_init<R>(input: &mut R, header_buf: &mut Vec<u8>) -> Result<Self, Error>
    where
        R: Read,
    {
        // Read the pre-header (file magic and size)
        header_buf.resize(archive::PRE_HEADER_SIZE, 0);
        input.read_exact(header_buf).or_else(|e| {
            header_buf.clear();
            Err(("unable to read archive", e))
        })?;

        Self::verify_pre_header(&header_buf[0..archive::PRE_HEADER_SIZE])?;

        let dictionary_size = archive::u64_from_le_slice(
            &header_buf[archive::FILE_MAGIC.len()..archive::PRE_HEADER_SIZE],
        ) as usize;

        // Read the dictionary, chunk data offset and header hash
        header_buf.resize(archive::PRE_HEADER_SIZE + dictionary_size + 8 + 64, 0);
        input
            .read_exact(&mut header_buf[archive::PRE_HEADER_SIZE..])
            .map_err(|e| ("unable to read archive", e))?;

        // Verify the header against the header hash
        let mut hasher = Blake2b::new();
        let offs = archive::PRE_HEADER_SIZE + dictionary_size + 8;
        hasher.input(&header_buf[..offs]);
        let header_checksum = header_buf[offs..(offs + 64)].to_vec();
        if header_checksum != hasher.result().to_vec() {
            return Err(Error::NotAnArchive("corrupt archive header".to_owned()));
        }

        // Deserialize the chunk dictionary
        let offs = archive::PRE_HEADER_SIZE;
        let dictionary: chunk_dictionary::ChunkDictionary =
            protobuf::parse_from_bytes(&header_buf[offs..(offs + dictionary_size)])
                .map_err(|e| ("unable to unpack archive header", e))?;

        // Get chunk data offset
        let offs = archive::PRE_HEADER_SIZE + dictionary_size;
        let chunk_data_offset = archive::u64_from_le_slice(&header_buf[offs..(offs + 8)]) as usize;

        // Create map to go from chunk hash to descriptor index
        let mut chunk_descriptors: Vec<archive::ChunkDescriptor> = Vec::new();
        let mut chunk_map: HashMap<HashBuf, usize> = HashMap::new();
        for (index, desc) in dictionary.chunk_descriptors.into_iter().enumerate() {
            chunk_map.insert(desc.checksum.clone(), index);
            chunk_descriptors.push(desc.into());
        }

        // Create chunk offset vector, to go from chunk index to source file offsets
        let mut chunk_offsets = vec![vec![]; chunk_descriptors.len()];
        let mut current_offset: u64 = 0;
        dictionary
            .rebuild_order
            .iter()
            .for_each(|descriptor_index| {
                let descriptor_index = *descriptor_index as usize;
                let chunk_size = chunk_descriptors[descriptor_index].source_size;
                chunk_offsets[descriptor_index].push(current_offset);
                current_offset += u64::from(chunk_size);
            });

        let chunker_params = dictionary.chunker_params.unwrap();
        Ok(ArchiveReader {
            chunk_map,
            chunk_descriptors,
            chunk_offsets,
            header_checksum,
            header_size: header_buf.len(),
            source_total_size: dictionary.source_total_size,
            source_checksum: dictionary.source_checksum,
            created_by_app_version: dictionary.application_version,
            chunk_compression: dictionary.chunk_compression.unwrap().into(),
            rebuild_order: dictionary
                .rebuild_order
                .into_iter()
                .map(|s| s as usize)
                .collect(),
            archive_chunks_offset: chunk_data_offset as u64,
            chunker_params: ChunkerParams::new(
                chunker_params.chunk_filter_bits,
                chunker_params.min_chunk_size as usize,
                chunker_params.max_chunk_size as usize,
                chunker_params.hash_window_size as usize,
                archive::BUZHASH_SEED,
            ),
            hash_length: chunker_params.chunk_hash_length as usize,
        })
    }

    pub fn total_chunks(&self) -> usize {
        self.rebuild_order.len()
    }

    pub fn unique_chunks(&self) -> usize {
        self.chunk_descriptors.len()
    }

    pub fn compressed_size(&self) -> u64 {
        self.chunk_descriptors
            .iter()
            .map(|c| u64::from(c.archive_size))
            .sum()
    }

    // Get a set of all chunks present in archive
    pub fn chunk_hash_set(&self) -> HashSet<HashBuf> {
        self.chunk_map.iter().map(|x| x.0.clone()).collect()
    }

    // Get source offsets of a chunk
    pub fn chunk_source_offsets(&self, hash: &[u8]) -> Vec<u64> {
        if let Some(index) = self.chunk_map.get(hash) {
            self.chunk_offsets[*index].clone()
        } else {
            vec![]
        }
    }

    // Group chunks which are placed in sequence inside archive
    fn group_chunks_in_sequence(
        mut chunks: Vec<&archive::ChunkDescriptor>,
    ) -> Vec<Vec<&archive::ChunkDescriptor>> {
        let mut group_list = vec![];
        if chunks.is_empty() {
            return group_list;
        }
        let mut group = vec![chunks.remove(0)];
        while !chunks.is_empty() {
            let chunk = chunks.remove(0);

            let prev_chunk_end = group.last().unwrap().archive_offset
                + u64::from(group.last().unwrap().archive_size);

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

    pub fn decompress_and_verify(
        hash_length: usize,
        compression: Compression,
        archive_checksum: &[u8],
        source_size: usize,
        archive_data: Vec<u8>,
    ) -> Result<Vec<u8>, Error> {
        let mut hasher = Blake2b::new();
        let chunk_data = if archive_data.len() == source_size {
            // Archive data is not compressed
            archive_data
        } else {
            let mut decompress_buf = vec![];
            compression.decompress(archive_data, &mut decompress_buf)?;
            decompress_buf
        };

        // Verify data by hash
        hasher.input(&chunk_data);
        let checksum = hasher.result().to_vec();
        if checksum[..hash_length] != archive_checksum[..hash_length] {
            panic!(
                "Chunk hash mismatch (expected: {}, got: {})",
                HexSlice::new(&checksum[0..hash_length]),
                HexSlice::new(&archive_checksum[0..hash_length])
            );
        }

        Ok(chunk_data)
    }

    // Get chunk data for all listed chunks if present in archive
    pub fn read_chunk_data<T, F>(
        &self,
        pool: &ThreadPool,
        mut input: T,
        chunks: &HashSet<HashBuf>,
        mut chunk_callback: F,
    ) -> Result<u64, Error>
    where
        T: ArchiveBackend,
        F: FnMut(HashBuf, &[u8]) -> Result<(), Error>,
    {
        // Create list of chunks which are in archive. The order of the list should
        // be the same order as the chunk data in archive.
        let descriptors: Vec<&archive::ChunkDescriptor> = self
            .chunk_descriptors
            .iter()
            .filter(|chunk| chunks.contains(&chunk.checksum))
            .collect();

        let mut total_read = 0;

        // Setup a parallel pipe for decompression and verify chunk data
        let hash_length = self.hash_length;
        let chunk_compression = self.chunk_compression;
        let mut pipe = ParaPipe::new_output(pool, |(chunk_data, checksum): (Vec<u8>, HashBuf)| {
            // For each offset where this chunk was found in source
            chunk_callback(checksum, &chunk_data).expect("forward chunk");
        });

        // Create groups of chunks so that we can make a single request for all chunks
        // which are placed in sequence in archive.
        let grouped_chunks = Self::group_chunks_in_sequence(descriptors);

        for group in grouped_chunks {
            // For each group of chunks
            let start_offset = self.archive_chunks_offset + group[0].archive_offset;
            let chunk_sizes: Vec<u64> = group.iter().map(|c| u64::from(c.archive_size)).collect();
            let mut chunk_index = 0;

            input
                .read_in_chunks(start_offset, &chunk_sizes, |archive_data| {
                    // For each chunk read from archive
                    let chunk_descriptor = &group[chunk_index];
                    total_read += u64::from(chunk_descriptor.archive_size);
                    pipe.input(
                        (
                            chunk_descriptor.checksum.clone(),
                            chunk_descriptor.source_size as usize,
                            archive_data,
                        ),
                        move |(checksum, source_size, archive_data): (HashBuf, usize, Vec<u8>)| {
                            (
                                Self::decompress_and_verify(
                                    hash_length,
                                    chunk_compression,
                                    &checksum,
                                    source_size,
                                    archive_data,
                                )
                                .expect("decompression failed"),
                                checksum,
                            )
                        },
                    );

                    chunk_index += 1;

                    Ok(())
                })
                .expect("read chunks");
        }

        Ok(total_read)
    }
}
