use blake2::{Blake2b, Digest};

use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::archive;
use crate::chunk_dictionary;
use crate::chunker::ChunkerParams;
use crate::compression::Compression;
use crate::error::Error;
use crate::reader_backend;
use crate::string_utils::*;

pub struct ArchiveReader {
    // Go from chunk hash to archive chunk index (chunks vector)
    chunk_map: HashMap<archive::HashBuf, usize>,

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
    pub source_checksum: archive::HashBuf,

    // Chunker parameters used when this archive was created
    pub chunker_params: ChunkerParams,
    pub hash_length: usize,
}

impl fmt::Display for ArchiveReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

    pub async fn init(mut builder: reader_backend::Builder) -> Result<Self, Error> {
        // Read the pre-header (file magic and size)
        let mut header = builder
            .read_at(0, archive::PRE_HEADER_SIZE)?
            .await
            .map_err(|err| err.wrap("unable to read archive"))?;

        Self::verify_pre_header(&header)?;

        let dictionary_size = archive::u64_from_le_slice(
            &header[archive::FILE_MAGIC.len()..archive::PRE_HEADER_SIZE],
        ) as usize;

        // Read the dictionary, chunk data offset and header hash
        header.append(
            &mut builder
                .read_at(archive::PRE_HEADER_SIZE as u64, dictionary_size + 8 + 64)?
                .await
                .map_err(|err| err.wrap("unable to read archive"))?,
        );

        // Verify the header against the header hash
        let mut hasher = Blake2b::new();
        let offs = archive::PRE_HEADER_SIZE + dictionary_size + 8;
        hasher.input(&header[..offs]);
        let header_checksum = header[offs..(offs + 64)].to_vec();
        if header_checksum != hasher.result().to_vec() {
            return Err(Error::NotAnArchive("corrupt archive header".to_owned()));
        }

        // Deserialize the chunk dictionary
        let offs = archive::PRE_HEADER_SIZE;
        let dictionary: chunk_dictionary::ChunkDictionary =
            protobuf::parse_from_bytes(&header[offs..(offs + dictionary_size)])
                .map_err(|e| ("unable to unpack archive header", e))?;

        // Get chunk data offset
        let offs = archive::PRE_HEADER_SIZE + dictionary_size;
        let chunk_data_offset = archive::u64_from_le_slice(&header[offs..(offs + 8)]) as usize;

        // Create map to go from chunk hash to descriptor index
        let mut chunk_descriptors: Vec<archive::ChunkDescriptor> = Vec::new();
        let mut chunk_map: HashMap<archive::HashBuf, usize> = HashMap::new();
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
        Ok(Self {
            chunk_map,
            chunk_descriptors,
            chunk_offsets,
            header_checksum,
            header_size: header.len(),
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
    pub fn chunk_hash_set(&self) -> HashSet<archive::HashBuf> {
        self.chunk_map.iter().map(|x| x.0.clone()).collect()
    }

    // Get source offsets of a chunk
    pub fn chunk_source_offsets(&self, hash: &[u8]) -> &[u64] {
        if let Some(&index) = self.chunk_map.get(hash) {
            &self.chunk_offsets[index]
        } else {
            &[]
        }
    }

    // Group chunks which are placed in sequence inside archive
    pub fn grouped_chunks(
        &self,
        chunks: &HashSet<archive::HashBuf>,
    ) -> Vec<Vec<&archive::ChunkDescriptor>> {
        let mut descriptors: Vec<&archive::ChunkDescriptor> = self
            .chunk_descriptors
            .iter()
            .filter(|chunk| chunks.contains(&chunk.checksum))
            .collect();

        let mut group_list = vec![];
        if descriptors.is_empty() {
            return group_list;
        }
        let mut group = vec![descriptors.remove(0)];
        while !descriptors.is_empty() {
            let descriptors = descriptors.remove(0);

            let prev_chunk_end = group.last().unwrap().archive_offset
                + u64::from(group.last().unwrap().archive_size);

            if prev_chunk_end == descriptors.archive_offset {
                // Chunk is placed right next to the previous chunk
                group.push(descriptors);
            } else {
                group_list.push(group);
                group = vec![descriptors];
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
}
