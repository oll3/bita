use blake2::{Blake2b, Digest};
use std::convert::TryFrom;

use crate::archive;
use crate::chunk_dictionary as dict;
use crate::chunk_index::ChunkIndex;
use crate::chunker::ChunkerConfig;
use crate::compression::Compression;
use crate::error::Error;
use crate::reader_backend::ReaderBackend;
use crate::HashSum;

#[derive(Clone)]
pub struct ChunkDescriptor {
    pub checksum: HashSum,
    pub archive_size: u32,
    pub archive_offset: u64,
    pub source_size: u32,
}

impl From<dict::ChunkDescriptor> for ChunkDescriptor {
    fn from(dict: dict::ChunkDescriptor) -> Self {
        ChunkDescriptor {
            checksum: dict.checksum.into(),
            archive_size: dict.archive_size,
            archive_offset: dict.archive_offset,
            source_size: dict.source_size,
        }
    }
}

pub struct ArchiveReader {
    // Array representing the order of chunks in archive
    chunk_order: Vec<ChunkDescriptor>,

    // Chunk index describing the source file construct
    source_index: ChunkIndex,

    total_chunks: usize,
    header_size: usize,
    header_checksum: HashSum,
    chunk_compression: Compression,
    created_by_app_version: String,

    // Where the chunk data starts inside archive
    chunk_data_offset: u64,

    source_total_size: u64,
    source_checksum: HashSum,

    chunker_config: ChunkerConfig,
    chunk_hash_length: usize,
}

impl ArchiveReader {
    fn verify_pre_header(pre_header: &[u8]) -> Result<(), Error> {
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
            return Err(Error::NotAnArchive("invalid bita archive magic".to_owned()));
        }
        Ok(())
    }

    fn map_chunks(dictionary: &dict::ChunkDictionary) -> Vec<ChunkDescriptor> {
        let mut chunk_descriptors: Vec<ChunkDescriptor> = Vec::new();
        for desc in dictionary.chunk_descriptors.iter() {
            let desc: ChunkDescriptor = desc.clone().into();
            chunk_descriptors.push(desc);
        }
        chunk_descriptors
    }

    pub async fn try_init(mut backend: ReaderBackend) -> Result<Self, Error> {
        // Read the pre-header (file magic and size)
        let mut header = backend
            .read_at(0, archive::PRE_HEADER_SIZE)
            .await
            .map_err(|err| err.wrap("unable to read archive"))?;

        Self::verify_pre_header(&header)?;

        let dictionary_size = archive::u64_from_le_slice(
            &header[archive::FILE_MAGIC.len()..archive::PRE_HEADER_SIZE],
        ) as usize;

        // Read the dictionary, chunk data offset and header hash
        header.append(
            &mut backend
                .read_at(archive::PRE_HEADER_SIZE as u64, dictionary_size + 8 + 64)
                .await
                .map_err(|err| err.wrap("unable to read archive"))?,
        );

        // Verify the header against the header checksum
        let header_checksum = {
            let mut hasher = Blake2b::new();
            let offs = archive::PRE_HEADER_SIZE + dictionary_size + 8;
            hasher.input(&header[..offs]);
            let header_checksum = HashSum::from_slice(&header[offs..(offs + 64)]);
            if header_checksum != &hasher.result()[..] {
                return Err(Error::NotAnArchive("corrupt archive header".to_owned()));
            }
            header_checksum
        };

        // Deserialize the chunk dictionary
        let dictionary: dict::ChunkDictionary = {
            let offs = archive::PRE_HEADER_SIZE;
            prost::Message::decode(&header[offs..(offs + dictionary_size)])
                .map_err(|e| ("unable to unpack archive header", e))?
        };

        // Get chunk data offset
        let chunk_data_offset = {
            let offs = archive::PRE_HEADER_SIZE + dictionary_size;
            archive::u64_from_le_slice(&header[offs..(offs + 8)])
        };

        let source_index = ChunkIndex::from_dictionary(&dictionary);

        let chunk_order = Self::map_chunks(&dictionary);
        let chunker_params = dictionary
            .chunker_params
            .ok_or("missing chunker parameters")?;
        Ok(Self {
            chunk_order,
            header_checksum,
            header_size: header.len(),
            source_total_size: dictionary.source_total_size,
            source_checksum: dictionary.source_checksum.into(),
            created_by_app_version: dictionary.application_version.clone(),
            chunk_compression: Compression::try_from(
                dictionary.chunk_compression.ok_or("missing compression")?,
            )?,
            total_chunks: dictionary.rebuild_order.iter().count(),
            chunk_data_offset,
            chunk_hash_length: chunker_params.chunk_hash_length as usize,
            chunker_config: ChunkerConfig::try_from(chunker_params)?,
            source_index,
        })
    }

    pub fn total_chunks(&self) -> usize {
        self.total_chunks
    }
    pub fn unique_chunks(&self) -> usize {
        self.chunk_order.len()
    }
    pub fn compressed_size(&self) -> u64 {
        self.chunk_order
            .iter()
            .map(|c| u64::from(c.archive_size))
            .sum()
    }
    pub fn chunk_data_offset(&self) -> u64 {
        self.chunk_data_offset
    }
    pub fn chunk_descriptors(&self) -> &[ChunkDescriptor] {
        &self.chunk_order
    }
    pub fn total_source_size(&self) -> u64 {
        self.source_total_size
    }
    pub fn chunker_config(&self) -> &ChunkerConfig {
        &self.chunker_config
    }
    pub fn header_checksum(&self) -> &HashSum {
        &self.header_checksum
    }
    pub fn source_checksum(&self) -> &HashSum {
        &self.source_checksum
    }
    pub fn header_size(&self) -> usize {
        self.header_size
    }
    pub fn chunk_hash_length(&self) -> usize {
        self.chunk_hash_length
    }
    pub fn chunk_compression(&self) -> Compression {
        self.chunk_compression
    }
    pub fn built_with_version(&self) -> &str {
        &self.created_by_app_version
    }
    pub fn source_index(&self) -> &ChunkIndex {
        &self.source_index
    }

    // Group chunks which are placed in sequence inside archive
    pub fn grouped_chunks(&self, chunks: &ChunkIndex) -> Vec<Vec<ChunkDescriptor>> {
        let mut descriptors: Vec<ChunkDescriptor> = self
            .chunk_order
            .iter()
            .filter(|chunk| chunks.contains(&chunk.checksum))
            .cloned()
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
        compression: Compression,
        archive_checksum: &HashSum,
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
        let checksum = HashSum::from_slice(&hasher.result()[..archive_checksum.len()]);
        if checksum != *archive_checksum {
            panic!(
                "Chunk hash mismatch (expected: {}, got: {})",
                checksum, archive_checksum,
            );
        }

        Ok(chunk_data)
    }
}
