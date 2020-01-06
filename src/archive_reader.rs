use blake2::{Blake2b, Digest};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use crate::archive;
use crate::chunk_dictionary;
use crate::chunker::ChunkerConfig;
use crate::compression::Compression;
use crate::error::Error;
use crate::reader_backend;
use crate::HashSum;

#[derive(Clone)]
pub struct ChunkDescriptor {
    pub checksum: HashSum,
    pub archive_size: u32,
    pub archive_offset: u64,
    pub source_size: u32,
    pub source_offsets: Vec<u64>,
}

impl From<(chunk_dictionary::ChunkDescriptor, Vec<u64>)> for ChunkDescriptor {
    fn from((dict, source_offsets): (chunk_dictionary::ChunkDescriptor, Vec<u64>)) -> Self {
        ChunkDescriptor {
            checksum: dict.checksum.into(),
            archive_size: dict.archive_size,
            archive_offset: dict.archive_offset,
            source_size: dict.source_size,
            source_offsets,
        }
    }
}

pub struct ArchiveReader {
    // Go from chunk hash to archive chunk index (chunks vector)
    chunk_map: HashMap<HashSum, Rc<ChunkDescriptor>>,

    // Array representing the order chunks in source
    chunk_descriptors: Vec<Rc<ChunkDescriptor>>,

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

    fn map_chunks(
        dictionary: &chunk_dictionary::ChunkDictionary,
    ) -> (
        Vec<Rc<ChunkDescriptor>>,
        HashMap<HashSum, Rc<ChunkDescriptor>>,
    ) {
        // Create chunk offset vector, to go from chunk index to source file offsets
        let source_offsets = {
            let mut offsets: Vec<Vec<u64>> = vec![vec![]; dictionary.chunk_descriptors.len()];
            let mut offset: u64 = 0;
            dictionary.rebuild_order.iter().for_each(|&i| {
                let i = i as usize;
                let chunk_size = dictionary.chunk_descriptors[i].source_size;
                offsets[i].push(offset);
                offset += u64::from(chunk_size);
            });
            offsets
        };

        // Create map to go from chunk hash to descriptor index
        let mut chunk_descriptors: Vec<Rc<ChunkDescriptor>> = Vec::new();
        let mut chunk_map: HashMap<HashSum, Rc<ChunkDescriptor>> = HashMap::new();

        for (desc, chunk_source_offsets) in dictionary
            .chunk_descriptors
            .iter()
            .zip(source_offsets.into_iter())
        {
            let desc: Rc<ChunkDescriptor> = Rc::new((desc.clone(), chunk_source_offsets).into());
            chunk_map.insert(desc.checksum.clone(), desc.clone());
            chunk_descriptors.push(desc);
        }
        (chunk_descriptors, chunk_map)
    }

    pub async fn try_init(mut builder: reader_backend::Builder) -> Result<Self, Error> {
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
        let dictionary: chunk_dictionary::ChunkDictionary = {
            let offs = archive::PRE_HEADER_SIZE;
            protobuf::parse_from_bytes(&header[offs..(offs + dictionary_size)])
                .map_err(|e| ("unable to unpack archive header", e))?
        };

        // Get chunk data offset
        let chunk_data_offset = {
            let offs = archive::PRE_HEADER_SIZE + dictionary_size;
            archive::u64_from_le_slice(&header[offs..(offs + 8)])
        };

        let (chunk_descriptors, chunk_map) = Self::map_chunks(&dictionary);
        let chunker_params = dictionary.chunker_params.unwrap();
        Ok(Self {
            chunk_map,
            chunk_descriptors,
            header_checksum,
            header_size: header.len(),
            source_total_size: dictionary.source_total_size,
            source_checksum: dictionary.source_checksum.into(),
            created_by_app_version: dictionary.application_version,
            chunk_compression: dictionary.chunk_compression.unwrap().into(),
            total_chunks: dictionary.rebuild_order.into_iter().count(),
            chunk_data_offset,
            chunk_hash_length: chunker_params.chunk_hash_length as usize,
            chunker_config: chunker_params.into(),
        })
    }

    pub fn total_chunks(&self) -> usize {
        self.total_chunks
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
    pub fn chunk_data_offset(&self) -> u64 {
        self.chunk_data_offset
    }
    pub fn chunk_descriptors(&self) -> &[Rc<ChunkDescriptor>] {
        &self.chunk_descriptors
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

    // Get a set of all chunks present in archive
    pub fn chunk_hash_set(&self) -> HashSet<HashSum> {
        self.chunk_map.iter().map(|x| x.0.clone()).collect()
    }

    // Get source offsets of a chunk
    pub fn chunk_source_offsets(&self, hash: &HashSum) -> &[u64] {
        if let Some(descriptor) = self.chunk_map.get(hash) {
            &descriptor.source_offsets
        } else {
            &[]
        }
    }

    // Group chunks which are placed in sequence inside archive
    pub fn grouped_chunks(&self, chunks: &HashSet<HashSum>) -> Vec<Vec<Rc<ChunkDescriptor>>> {
        let mut descriptors: Vec<Rc<ChunkDescriptor>> = self
            .chunk_descriptors
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
