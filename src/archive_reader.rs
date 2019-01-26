use blake2::{Blake2b, Digest};

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io::prelude::*;

use crate::archive;
use crate::chunk_dictionary;
use crate::chunker_utils::HashBuf;
use crate::compression::Compression;
use crate::errors::*;
use crate::string_utils::*;

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

    // Array of chunk descriptors
    chunk_descriptors: Vec<archive::ChunkDescriptor>,

    // Go from archive chunk index to array of source offsets
    chunk_offsets: Vec<Vec<u64>>,

    // The order of chunks in source
    rebuild_order: Vec<usize>,

    // Compression used for all chunks
    pub chunk_compression: Compression,

    pub created_by_app_version: String,
    pub archive_chunks_offset: u64,

    // Size of the original source file
    pub source_total_size: u64,
    pub source_checksum: HashBuf,

    // Chunker parameters used when this archive was created
    pub chunk_filter_bits: u32,
    pub min_chunk_size: usize,
    pub max_chunk_size: usize,
    pub hash_window_size: usize,
    pub hash_length: usize,
}

impl fmt::Display for ArchiveReader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "build version: {}, chunks: {} (unique: {}), compression: {}, decompressed size: {}, source checksum: {}",
            self.created_by_app_version,
            self.rebuild_order.len(),
            self.chunk_descriptors.len(),
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
        let source_total_size = dictionary.source_total_size;
        let source_checksum = dictionary.source_checksum;
        let created_by_app_version = dictionary.application_version;
        let chunker_params = dictionary.chunker_params.unwrap();
        let chunk_compression = dictionary.chunk_compression.unwrap().into();

        let mut chunk_descriptors: Vec<archive::ChunkDescriptor> = Vec::new();
        let mut chunk_map: HashMap<HashBuf, usize> = HashMap::new();

        // Create map to go from chunk hash to descriptor index
        for (index, desc) in dictionary.chunk_descriptors.into_iter().enumerate() {
            chunk_map.insert(desc.checksum.clone(), index);
            chunk_descriptors.push(desc.into());
        }

        // Create chunk offset vector, to go from chunk index to source file offsets
        let mut chunk_offsets = vec![vec![]; chunk_descriptors.len()];
        {
            let mut current_offset: u64 = 0;
            for descriptor_index in dictionary.rebuild_order.iter() {
                let descriptor_index = *descriptor_index as usize;
                let chunk_size = chunk_descriptors[descriptor_index].source_size;
                chunk_offsets[descriptor_index].push(current_offset);
                current_offset += u64::from(chunk_size);
            }
        }

        Ok(ArchiveReader {
            chunk_map,
            chunk_descriptors,
            chunk_offsets,
            source_total_size,
            source_checksum,
            created_by_app_version,
            chunk_compression,
            rebuild_order: dictionary
                .rebuild_order
                .into_iter()
                .map(|s| s as usize)
                .collect(),
            archive_chunks_offset: chunk_data_offset as u64,
            chunk_filter_bits: chunker_params.chunk_filter_bits,
            min_chunk_size: chunker_params.min_chunk_size as usize,
            max_chunk_size: chunker_params.max_chunk_size as usize,
            hash_window_size: chunker_params.hash_window_size as usize,
            hash_length: chunker_params.chunk_hash_length as usize,
        })
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
        chunk_descriptor: &archive::ChunkDescriptor,
        archive_data: Vec<u8>,
    ) -> Result<Vec<u8>> {
        let mut hasher = Blake2b::new();
        let chunk_data = if chunk_descriptor.archive_size == chunk_descriptor.source_size {
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
        if checksum[..hash_length] != chunk_descriptor.checksum[..hash_length] {
            bail!(
                "Chunk hash mismatch (expected: {}, got: {})",
                HexSlice::new(&chunk_descriptor.checksum[0..hash_length]),
                HexSlice::new(&checksum[0..hash_length])
            );
        }

        Ok(chunk_data)
    }

    // Get chunk data for all listed chunks if present in archive
    pub fn read_chunk_data<T, F>(
        &self,
        mut input: T,
        chunks: &HashSet<HashBuf>,
        mut chunk_callback: F,
    ) -> Result<u64>
    where
        T: ArchiveBackend,
        F: FnMut(&archive::ChunkDescriptor, &[u8]) -> Result<()>,
    {
        // Create list of chunks which are in archive. The order of the list should
        // be the same otder as the chunk data in archive.
        let descriptors: Vec<&archive::ChunkDescriptor> = self
            .chunk_descriptors
            .iter()
            .filter(|chunk| chunks.contains(&chunk.checksum))
            .collect();

        // Create groups of chunks so that we can make a single request for all chunks
        // which are placed in sequence in archive.
        let grouped_chunks = Self::group_chunks_in_sequence(descriptors);

        let mut total_read = 0;

        for group in grouped_chunks {
            // For each group of chunks
            let start_offset = self.archive_chunks_offset + group[0].archive_offset;
            let chunk_sizes: Vec<u64> = group.iter().map(|c| u64::from(c.archive_size)).collect();
            let mut chunk_index = 0;

            input
                .read_in_chunks(start_offset, &chunk_sizes, |archive_data| {
                    // For each chunk read from archive
                    let chunk_descriptor = &group[chunk_index];

                    let chunk_data = Self::decompress_and_verify(
                        self.hash_length,
                        self.chunk_compression,
                        chunk_descriptor,
                        archive_data,
                    )?;

                    // For each offset where this chunk was found in source
                    chunk_callback(chunk_descriptor, &chunk_data)?;

                    chunk_index += 1;
                    total_read += u64::from(chunk_descriptor.archive_size);

                    Ok(())
                })
                .expect("read chunks");
        }

        Ok(total_read)
    }

    // Get chunk data for all listed chunks if present in archive
    pub fn read_chunk_stream<T, F>(
        &mut self,
        mut input: T,
        mut current_input_offset: u64,
        chunks: &HashSet<HashBuf>,
        mut chunk_callback: F,
    ) -> Result<u64>
    where
        T: Read,
        F: FnMut(&archive::ChunkDescriptor, &[u8]) -> Result<()>,
    {
        let mut skip_buffer = vec![0; 1024 * 1024];
        let mut total_read = 0;

        // Sort list of chunks to be read in order of occurence in stream.
        let descriptors = self
            .chunk_descriptors
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

            let chunk_data = Self::decompress_and_verify(
                self.hash_length,
                self.chunk_compression,
                chunk_descriptor,
                archive_data,
            )?;

            current_input_offset += skip;
            current_input_offset += u64::from(chunk_descriptor.archive_size);
            total_read += u64::from(chunk_descriptor.archive_size);

            // For each offset where this chunk was found in source
            chunk_callback(chunk_descriptor, &chunk_data)?;
        }

        Ok(total_read)
    }
}
