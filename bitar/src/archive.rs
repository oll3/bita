use blake2::{Blake2b, Digest};

use crate::{
    chunk_dictionary as dict,
    chunk_dictionary::{
        chunk_compression::CompressionType, chunker_parameters::ChunkingAlgorithm,
        ChunkCompression, ChunkerParameters,
    },
    header, ChunkIndex, ChunkerConfig, Compression, CompressionError, HashSum, Reader,
};

#[derive(Debug)]
pub enum ArchiveError<R> {
    NotAnArchive,
    InvalidHeaderChecksum,
    CorruptArchive,
    ReaderError(R),
    UnknownChunkingAlgorithm,
    DictionaryDecode(prost::DecodeError),
    UnknownCompression,
    CompressionError(CompressionError),
}

impl<R> std::error::Error for ArchiveError<R> where R: std::error::Error {}

impl<R> std::fmt::Display for ArchiveError<R>
where
    R: std::error::Error,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotAnArchive => write!(f, "not an archive"),
            Self::InvalidHeaderChecksum => write!(f, "invalid archive header"),
            Self::CorruptArchive => write!(f, "corrupt archive"),
            Self::ReaderError(err) => write!(f, "reader error: {}", err),
            Self::UnknownChunkingAlgorithm => write!(f, "unknown chunking algorithm"),
            Self::DictionaryDecode(err) => write!(f, "dictionary decode error: {}", err),
            Self::UnknownCompression => write!(f, "unknown chunk compression"),
            Self::CompressionError(err) => write!(f, "compression error: {}", err),
        }
    }
}

impl<E> From<prost::DecodeError> for ArchiveError<E> {
    fn from(err: prost::DecodeError) -> Self {
        Self::DictionaryDecode(err)
    }
}

impl<E> From<CompressionError> for ArchiveError<E> {
    fn from(err: CompressionError) -> Self {
        Self::CompressionError(err)
    }
}

/// Description of a chunk within an archive.
#[derive(Clone, Debug)]
pub struct ChunkDescriptor {
    /// Chunk checksum.
    pub checksum: HashSum,
    /// Actual size of chunk data in the archive (may be compressed).
    pub archive_size: u32,
    /// Byte offset of chunk in the archive.
    pub archive_offset: u64,
    /// Size of the chunk data in source (uncompressed).
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

/// A readable archive.
pub struct Archive {
    // Array representing the order of chunks in archive
    chunk_order: Vec<ChunkDescriptor>,

    // Chunk index describing the source file construct
    source_index: ChunkIndex,

    total_chunks: usize,
    header_size: usize,
    header_checksum: HashSum,
    chunk_compression: Compression,
    created_by_app_version: String,
    chunk_data_offset: u64,
    source_total_size: u64,
    source_checksum: HashSum,
    chunker_config: ChunkerConfig,
    chunk_hash_length: usize,
}

impl Archive {
    fn verify_pre_header<E>(pre_header: &[u8]) -> Result<(), ArchiveError<E>> {
        if pre_header.len() < header::ARCHIVE_MAGIC.len() {
            return Err(ArchiveError::NotAnArchive);
        }
        // Allow both legacy type file magic (prefixed with \0 but no null
        // termination) and 'BITA\0'.
        if &pre_header[0..header::ARCHIVE_MAGIC.len()] != header::ARCHIVE_MAGIC
            && &pre_header[0..header::ARCHIVE_MAGIC.len()] != b"\0BITA1"
        {
            return Err(ArchiveError::NotAnArchive);
        }
        Ok(())
    }
    /// Try to initialize an archive from a reader.
    pub async fn try_init<R>(reader: &mut R) -> Result<Self, ArchiveError<R::Error>>
    where
        R: Reader,
    {
        // Read the pre-header (file magic and size)
        let mut header: Vec<u8> = reader
            .read_at(0, header::PRE_HEADER_SIZE)
            .await
            .map_err(ArchiveError::ReaderError)?
            .to_vec();
        Self::verify_pre_header(&header)?;

        let dictionary_size =
            u64_from_le_slice(&header[header::ARCHIVE_MAGIC.len()..header::PRE_HEADER_SIZE])
                as usize;

        // Read the dictionary, chunk data offset and header hash
        header.extend_from_slice(
            &reader
                .read_at(header::PRE_HEADER_SIZE as u64, dictionary_size + 8 + 64)
                .await
                .map_err(ArchiveError::ReaderError)?,
        );

        // Verify the header against the header checksum
        let header_checksum = {
            let mut hasher = Blake2b::new();
            let offs = header::PRE_HEADER_SIZE + dictionary_size + 8;
            hasher.input(&header[..offs]);
            let header_checksum = HashSum::from_slice(&header[offs..(offs + 64)]);
            if header_checksum != &hasher.result()[..] {
                return Err(ArchiveError::InvalidHeaderChecksum);
            }
            header_checksum
        };

        // Deserialize the chunk dictionary
        let dictionary: dict::ChunkDictionary = {
            let offs = header::PRE_HEADER_SIZE;
            prost::Message::decode(&header[offs..(offs + dictionary_size)])?
        };

        // Get chunk data offset
        let chunk_data_offset = {
            let offs = header::PRE_HEADER_SIZE + dictionary_size;
            u64_from_le_slice(&header[offs..(offs + 8)])
        };

        let source_index = ChunkIndex::from_dictionary(&dictionary);
        let chunk_order = dictionary
            .chunk_descriptors
            .into_iter()
            .map(ChunkDescriptor::from)
            .collect();
        let chunker_params = dictionary
            .chunker_params
            .ok_or(ArchiveError::CorruptArchive)?;
        let chunk_hash_length = chunker_params.chunk_hash_length as usize;
        Ok(Self {
            chunk_order,
            header_checksum,
            header_size: header.len(),
            source_total_size: dictionary.source_total_size,
            source_checksum: dictionary.source_checksum.into(),
            created_by_app_version: dictionary.application_version.clone(),
            chunk_compression: compression_from_dictionary(
                dictionary
                    .chunk_compression
                    .ok_or(ArchiveError::CorruptArchive)?,
            )?,
            total_chunks: dictionary.rebuild_order.iter().count(),
            chunk_data_offset,
            chunk_hash_length,
            chunker_config: chunker_config_from_params(chunker_params)?,
            source_index,
        })
    }
    /// Total number of chunks in archive (including duplicates).
    pub fn total_chunks(&self) -> usize {
        self.total_chunks
    }
    /// Total number of unique chunks in archive (no duplicates).
    pub fn unique_chunks(&self) -> usize {
        self.chunk_order.len()
    }
    /// Total size of chunks in archive when compressed.
    pub fn compressed_size(&self) -> u64 {
        self.chunk_order
            .iter()
            .map(|c| u64::from(c.archive_size))
            .sum()
    }
    /// On which offset in the archive the chunk data starts at.
    pub fn chunk_data_offset(&self) -> u64 {
        self.chunk_data_offset
    }
    /// Get archive chunk descriptors.
    pub fn chunk_descriptors(&self) -> &[ChunkDescriptor] {
        &self.chunk_order
    }
    /// Total size of the original source file.
    pub fn total_source_size(&self) -> u64 {
        self.source_total_size
    }
    /// Checksum of the original source file (Blake2).
    pub fn source_checksum(&self) -> &HashSum {
        &self.source_checksum
    }
    /// Get the chunker configuration used when building the archive.
    pub fn chunker_config(&self) -> &ChunkerConfig {
        &self.chunker_config
    }
    /// Get the checksum of the archive header.
    pub fn header_checksum(&self) -> &HashSum {
        &self.header_checksum
    }
    /// Get the size of the archive header.
    pub fn header_size(&self) -> usize {
        self.header_size
    }
    /// Get the hash length used for identifying chunks when building the archive.
    pub fn chunk_hash_length(&self) -> usize {
        self.chunk_hash_length
    }
    /// Get the compression used for chunks in the archive.
    pub fn chunk_compression(&self) -> Compression {
        self.chunk_compression
    }
    /// Get the version of crate used when building the archive.
    pub fn built_with_version(&self) -> &str {
        &self.created_by_app_version
    }
    /// Get the ChunkIndex of the archive.
    pub fn source_index(&self) -> &ChunkIndex {
        &self.source_index
    }
    /// Returns chunks at adjacent location in archive grouped together.
    pub fn grouped_chunks(&self, chunks: &ChunkIndex) -> Vec<Vec<ChunkDescriptor>> {
        let mut group_list = Vec::new();
        let mut group: Vec<ChunkDescriptor> = Vec::new();
        for descriptor in self
            .chunk_order
            .iter()
            .filter(|chunk| chunks.contains(&chunk.checksum))
            .cloned()
        {
            if let Some(prev) = group.last() {
                let prev_chunk_end = prev.archive_offset + u64::from(prev.archive_size);
                if prev_chunk_end == descriptor.archive_offset {
                    // Chunk is placed right next to the previous chunk
                    group.push(descriptor);
                } else {
                    group_list.push(group);
                    group = vec![descriptor];
                }
            } else {
                group.push(descriptor);
            }
        }
        group_list
    }
}

fn chunker_config_from_params<R>(p: ChunkerParameters) -> Result<ChunkerConfig, ArchiveError<R>> {
    match ChunkingAlgorithm::from_i32(p.chunking_algorithm) {
        Some(ChunkingAlgorithm::Buzhash) => {
            Ok(ChunkerConfig::BuzHash(crate::ChunkerFilterConfig {
                filter_bits: crate::ChunkerFilterBits::from_bits(p.chunk_filter_bits),
                min_chunk_size: p.min_chunk_size as usize,
                max_chunk_size: p.max_chunk_size as usize,
                window_size: p.rolling_hash_window_size as usize,
            }))
        }
        Some(ChunkingAlgorithm::Rollsum) => {
            Ok(ChunkerConfig::RollSum(crate::ChunkerFilterConfig {
                filter_bits: crate::ChunkerFilterBits::from_bits(p.chunk_filter_bits),
                min_chunk_size: p.min_chunk_size as usize,
                max_chunk_size: p.max_chunk_size as usize,
                window_size: p.rolling_hash_window_size as usize,
            }))
        }
        Some(ChunkingAlgorithm::FixedSize) => {
            Ok(ChunkerConfig::FixedSize(p.max_chunk_size as usize))
        }
        _ => Err(ArchiveError::UnknownChunkingAlgorithm),
    }
}

fn compression_from_dictionary<R>(c: ChunkCompression) -> Result<Compression, ArchiveError<R>> {
    match CompressionType::from_i32(c.compression) {
        #[cfg(feature = "lzma-compression")]
        Some(CompressionType::Lzma) => Ok(Compression::LZMA(c.compression_level)),
        #[cfg(not(feature = "lzma-compression"))]
        Some(CompressionType::Lzma) => panic!("LZMA compression not enabled"),
        #[cfg(feature = "zstd-compression")]
        Some(CompressionType::Zstd) => Ok(Compression::ZSTD(c.compression_level)),
        #[cfg(not(feature = "zstd-compression"))]
        Some(CompressionType::Zstd) => panic!("ZSTD compression not enabled"),
        Some(CompressionType::Brotli) => Ok(Compression::Brotli(c.compression_level)),
        Some(CompressionType::None) => Ok(Compression::None),
        None => Err(ArchiveError::UnknownCompression),
    }
}

fn u64_from_le_slice(v: &[u8]) -> u64 {
    let mut tmp: [u8; 8] = Default::default();
    tmp.copy_from_slice(v);
    u64::from_le_bytes(tmp)
}
