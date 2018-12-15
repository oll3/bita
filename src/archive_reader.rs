use blake2::{Blake2b, Digest};
use chunker_utils::HashBuf;
use lzma::LzmaWriter;
use std::collections::{HashMap, HashSet};
use std::io::prelude::*;
use string_utils::*;

use archive;
use chunk_dictionary;
use errors::*;

// Skip forward in a file/stream which is non seekable
fn skip_bytes<T>(input: &mut T, skip_bytes: u64, skip_buffer: &mut [u8]) -> Result<()>
where
    T: Read,
{
    let mut total_read = 0;
    while total_read < skip_bytes {
        let read_bytes = std::cmp::min(skip_buffer.len(), (skip_bytes - total_read) as usize);
        input
            .read_exact(&mut skip_buffer[0..read_bytes])
            .chain_err(|| "failed to skip")?;
        total_read += read_bytes as u64;
    }
    Ok(())
}

pub struct ArchiveReader {
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

// Trait to implement for archive backends.
pub trait ArchiveBackend
where
    Self: Read,
{
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

impl ArchiveReader {
    pub fn verify_pre_header(pre_header: &[u8]) -> Result<()> {
        if pre_header.len() < 5 {
            bail!("failed to read header of archive")
        }
        if &pre_header[0..4] != b"bita" {
            return Err(Error::from_kind(ErrorKind::NotAnArchive(
                "missing magic".to_string(),
            )));
        }
        if pre_header[4] != 0 {
            return Err(Error::from_kind(ErrorKind::NotAnArchive(
                "unknown archive version".to_string(),
            )));
        }
        Ok(())
    }

    pub fn try_init<R>(input: &mut R, header_buf: &mut Vec<u8>) -> Result<Self>
    where
        R: Read,
    {
        // Read the pre-header (file magic, version and size)
        //let mut header_buf = vec![0; 13];
        header_buf.resize(13, 0);
        input
            .read_exact(header_buf)
            .or_else(|err| {
                header_buf.clear();
                Err(err)
            })
            .chain_err(|| "unable to read archive")?;

        Self::verify_pre_header(&header_buf[0..13])?;

        let dictionary_size = archive::vec_to_size(&header_buf[5..13]) as usize;

        // Read the dictionary, chunk data offset and header hash
        header_buf.resize(13 + dictionary_size + 8 + 64, 0);
        input
            .read_exact(&mut header_buf[13..])
            .chain_err(|| "unable to read archive")?;

        // Verify the header against the header hash
        let mut hasher = Blake2b::new();
        let offs = 13 + dictionary_size + 8;
        hasher.input(&header_buf[..offs]);
        if header_buf[offs..(offs + 64)] != hasher.result().to_vec()[..] {
            //bail!("Corrupt archive header")
            return Err(Error::from_kind(ErrorKind::NotAnArchive(
                "corrupt archive header".to_string(),
            )));
        }

        // Deserialize the chunk dictionary
        let offs = 13;
        let dictionary: chunk_dictionary::ChunkDictionary =
            protobuf::parse_from_bytes(&header_buf[offs..(offs + dictionary_size)])
                .chain_err(|| "unable to unpack archive header")?;

        // Get chunk data offset
        let offs = 13 + dictionary_size;
        let chunk_data_offset = archive::vec_to_size(&header_buf[offs..(offs + 8)]) as usize;

        // println!("{}", dictionary);

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

    // Decompress chunk data, if compressed
    fn decompress_chunk(
        compression: &Option<chunk_dictionary::ChunkDescriptor_oneof_compression>,
        archive_data: Vec<u8>,
        chunk_data: &mut Vec<u8>,
    ) -> Result<()> {
        match compression {
            Some(chunk_dictionary::ChunkDescriptor_oneof_compression::LZMA(_level)) => {
                // Archived chunk is compressed
                chunk_data.clear();
                let mut f = LzmaWriter::new_decompressor(chunk_data).expect("new decompressor");
                f.write_all(&archive_data).expect("write decompressor");
                f.finish().expect("finish decompressor");
            }
            None => {
                // Archived chunk is NOT compressed
                *chunk_data = archive_data;
            }
        }

        Ok(())
    }

    // Get chunk data for all listed chunks if present in archive
    pub fn read_chunk_data<T, F>(
        &mut self,
        mut input: T,
        chunks: &HashSet<HashBuf>,
        mut result: F,
    ) -> Result<()>
    where
        T: ArchiveBackend,
        F: FnMut(&chunk_dictionary::ChunkDescriptor, &[u8]) -> Result<()>,
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
        let grouped_chunks = Self::group_chunks_in_sequence(descriptors);

        let mut hasher = Blake2b::new();
        let mut total_read = 0;
        let mut chunk_data = vec![];

        for group in grouped_chunks {
            // For each group of chunks
            let start_offset = self.archive_chunks_offset + group[0].archive_offset;
            let chunk_sizes: Vec<u64> = group.iter().map(|c| c.archive_size).collect();
            let mut chunk_index = 0;

            input
                .read_in_chunks(start_offset, &chunk_sizes, |archive_data| {
                    // For each chunk read from archive
                    let chunk_descriptor = &group[chunk_index];
                    Self::decompress_chunk(
                        &chunk_descriptor.compression,
                        archive_data,
                        &mut chunk_data,
                    )?;

                    total_read += chunk_descriptor.archive_size;

                    // Verify data by hash
                    hasher.input(&chunk_data);
                    let checksum = hasher.result_reset().to_vec();
                    if checksum[..self.hash_length] != chunk_descriptor.checksum[..self.hash_length]
                    {
                        bail!(
                            "Chunk hash mismatch (expected: {}, got: {})",
                            HexSlice::new(&chunk_descriptor.checksum[0..self.hash_length]),
                            HexSlice::new(&checksum[0..self.hash_length])
                        );
                    }

                    // For each offset where this chunk was found in source
                    result(chunk_descriptor, &chunk_data)?;

                    chunk_index += 1;

                    Ok(())
                })
                .expect("read chunks");
        }
        self.total_read = total_read;

        Ok(())
    }

    // Get chunk data for all listed chunks if present in archive
    pub fn read_chunk_stream<T, F>(
        &mut self,
        mut input: T,
        mut current_input_offset: u64,
        chunks: &HashSet<HashBuf>,
        mut result: F,
    ) -> Result<()>
    where
        T: Read,
        F: FnMut(&chunk_dictionary::ChunkDescriptor, &[u8]) -> Result<()>,
    {
        let mut hasher = Blake2b::new();
        let mut skip_buffer = vec![0; 1024 * 1024];
        let mut chunk_data = vec![];

        // Sort list of chunks to be read in order of occurence in stream.
        let descriptors = self
            .chunks
            .iter()
            .filter(|chunk| chunks.contains(&chunk.checksum));

        for chunk_descriptor in descriptors {
            // Read through the stream and pick the chunk data requsted.

            let mut archive_data = vec![0; chunk_descriptor.archive_size as usize];
            let abs_chunk_offset = self.archive_chunks_offset + chunk_descriptor.archive_offset;

            // Skip until chunk
            let skip = abs_chunk_offset - current_input_offset;
            skip_bytes(&mut input, skip, &mut skip_buffer)?;

            // Read chunk data
            input
                .read_exact(&mut archive_data)
                .chain_err(|| "failed to read from archive")?;

            Self::decompress_chunk(&chunk_descriptor.compression, archive_data, &mut chunk_data)?;

            // Verify data by hash
            hasher.input(&chunk_data);
            let checksum = hasher.result_reset().to_vec();
            if checksum[..self.hash_length] != chunk_descriptor.checksum[..self.hash_length] {
                bail!(
                    "Chunk hash mismatch (expected: {}, got: {})",
                    HexSlice::new(&chunk_descriptor.checksum[0..self.hash_length]),
                    HexSlice::new(&checksum[0..self.hash_length])
                );
            }

            current_input_offset += skip;
            current_input_offset += chunk_descriptor.archive_size;
            self.total_read += chunk_descriptor.archive_size;

            // For each offset where this chunk was found in source
            result(chunk_descriptor, &chunk_data)?;
        }

        Ok(())
    }
}
