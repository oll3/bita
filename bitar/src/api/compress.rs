use std::collections::{BTreeMap, HashMap};
use std::default::Default;
use std::error;
use std::fmt;
use std::path::PathBuf;

use blake2::{Blake2b512, Digest};
use futures_util::{future, StreamExt};
use tokio::fs;
use tokio::io;
use tokio::io::AsyncSeekExt;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::task::JoinError;

use crate::chunk_dictionary;
use crate::chunker;
use crate::Compression;
use crate::CompressionAlgorithm;

pub const PKG_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Options for the `create_archive` function
#[derive(Clone, Debug)]
pub struct CreateArchiveOptions {
    /// The configuration to use when creating a chunk stream from the input
    pub chunker_config: chunker::Config,

    /// Number of parallel buffers to use when manipulating chunks
    pub num_chunk_buffers: usize,

    /// The length that the chunk hash should be truncated to for the output
    pub chunk_hash_length: usize,

    /// A temporary file is used to write intermediate chunk data. Setting this
    /// option forces this file to be used instead of a randomly generated one
    pub temporary_file_override: Option<PathBuf>,

    /// The type of compression to use when compressing a chunk
    pub compression: Option<Compression>,

    /// Custom string/bytes key-value pair metadata to be stored in the archive header
    pub metadata: BTreeMap<String, Vec<u8>>,
}

impl Default for CreateArchiveOptions {
    fn default() -> CreateArchiveOptions {
        let num_buffers = match num_cpus::get() {
            0 | 1 => 1,
            n => n * 2,
        };

        CreateArchiveOptions {
            chunker_config: chunker::Config::RollSum(chunker::FilterConfig::default()),
            num_chunk_buffers: num_buffers,
            chunk_hash_length: 64,
            temporary_file_override: None,
            compression: Some(Compression {
                algorithm: CompressionAlgorithm::Brotli,
                level: 6,
            }),
            metadata: BTreeMap::new(),
        }
    }
}

/// Output from the `create_archive` function
#[derive(Default)]
pub struct CreateArchiveResult {
    pub source_hash: Vec<u8>,
    pub source_length: usize,
    pub header: chunk_dictionary::ChunkDictionary,
}

/// Error from the `create_archive` function
#[derive(Debug)]
pub enum CreateArchiveError {
    /// Error that occurred while operatin on the temp file used to creat the archive
    TempFileError(io::Error),
    /// Failed to chunk the input file
    ChunkerError(JoinError),
    /// Failed to write to the output file
    OutputWriteError(io::Error),
}

impl fmt::Display for CreateArchiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CreateArchiveError::TempFileError(_) => {
                write!(f, "Error occurred while operating on the temp file")
            }
            CreateArchiveError::ChunkerError(_) => write!(f, "Error chunking the input file"),
            CreateArchiveError::OutputWriteError(_) => write!(f, "Error writing to output file"),
        }
    }
}

impl error::Error for CreateArchiveError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CreateArchiveError::TempFileError(e) => Some(e),
            CreateArchiveError::ChunkerError(e) => Some(e),
            CreateArchiveError::OutputWriteError(e) => Some(e),
        }
    }
}

/// Compress the input into the output as a bita archive
pub async fn create_archive<R: AsyncRead + Unpin + Send, W: AsyncWrite + Unpin>(
    mut input: R,
    mut output: W,
    options: &CreateArchiveOptions,
) -> Result<CreateArchiveResult, CreateArchiveError> {
    let mut source_hasher = Blake2b512::new();
    let mut source_length: usize = 0;

    let mut chunk_order = Vec::new();
    let mut unique_chunks = HashMap::new();
    let mut unique_chunk_index: usize = 0;

    let chunker = options.chunker_config.new_chunker(&mut input);
    let mut chunk_stream = chunker
        .map(|result| {
            let (offset, chunk) = result.expect("Error chunking");

            // Create some metadata of the input source
            source_hasher.update(chunk.data());
            source_length += chunk.len();

            // Convert each chunk into a `VerifiedChunk`
            tokio::task::spawn_blocking(move || (offset, chunk.verify()))
        })
        .buffered(options.num_chunk_buffers)
        .filter_map(|result| {
            // Create a lookup table of unique chunks by hash
            let (offset, verified) = result.expect("error while hashing chunk");
            let (unique, chunk_index) = if unique_chunks.contains_key(verified.hash()) {
                (false, *unique_chunks.get(verified.hash()).unwrap())
            } else {
                let chunk_index = unique_chunk_index;
                unique_chunks.insert(verified.hash().clone(), chunk_index);
                unique_chunk_index += 1;
                (true, chunk_index)
            };

            // Store a pointer (as index) to unique chunk index for each chunk
            chunk_order.push(chunk_index);
            future::ready(if unique {
                Some((chunk_index, offset, verified))
            } else {
                None
            })
        })
        .map(|(chunk_index, offset, verified)| {
            let _opt = options.clone();

            tokio::task::spawn_blocking(move || {
                let compressed = verified
                    .chunk()
                    .clone()
                    .compress(_opt.compression)
                    .expect("compress chunk");
                let (_compression, bytes) = compressed.into_inner();
                (chunk_index, offset, verified, bytes)
            })
        })
        .buffered(options.num_chunk_buffers);

    let mut archive_offset: u64 = 0;
    let mut archive_chunks = Vec::new();

    let mut temp_file = if let Some(p) = &options.temporary_file_override {
        fs::File::create(p).await
    } else {
        tempfile::tempfile().map(tokio::fs::File::from_std)
    }
    .map_err(CreateArchiveError::TempFileError)?;

    while let Some(result) = chunk_stream.next().await {
        let (_chunk_index, _offset, verified, compressed_bytes) =
            result.map_err(CreateArchiveError::ChunkerError)?;

        let use_data = {
            if compressed_bytes.len() < verified.chunk().len() {
                &compressed_bytes
            } else {
                verified.chunk().data()
            }
        };

        let mut hash = verified.hash().clone();
        hash.truncate(options.chunk_hash_length);

        // Write the compressed chunks to the file. This is not the final output
        // as we need to calculate the header and prepend it
        temp_file
            .write_all(use_data)
            .await
            .map_err(CreateArchiveError::TempFileError)?;

        // Store a descriptor which refers to the compressed data
        archive_chunks.push(chunk_dictionary::ChunkDescriptor {
            checksum: hash.to_vec(),
            source_size: verified.len() as u32,
            archive_offset,
            archive_size: use_data.len() as u32,
        });
        archive_offset += use_data.len() as u64;
    }

    let chunker_params = match &options.chunker_config {
        chunker::Config::BuzHash(hash_config) => chunk_dictionary::ChunkerParameters {
            chunk_filter_bits: hash_config.filter_bits.bits(),
            min_chunk_size: hash_config.min_chunk_size as u32,
            max_chunk_size: hash_config.max_chunk_size as u32,
            rolling_hash_window_size: hash_config.window_size as u32,
            chunk_hash_length: options.chunk_hash_length as u32,
            chunking_algorithm: chunk_dictionary::chunker_parameters::ChunkingAlgorithm::Buzhash
                as i32,
        },
        chunker::Config::RollSum(hash_config) => chunk_dictionary::ChunkerParameters {
            chunk_filter_bits: hash_config.filter_bits.bits(),
            min_chunk_size: hash_config.min_chunk_size as u32,
            max_chunk_size: hash_config.max_chunk_size as u32,
            rolling_hash_window_size: hash_config.window_size as u32,
            chunk_hash_length: options.chunk_hash_length as u32,
            chunking_algorithm: chunk_dictionary::chunker_parameters::ChunkingAlgorithm::Rollsum
                as i32,
        },
        chunker::Config::FixedSize(chunk_size) => chunk_dictionary::ChunkerParameters {
            min_chunk_size: 0,
            chunk_filter_bits: 0,
            rolling_hash_window_size: 0,
            max_chunk_size: *chunk_size as u32,
            chunk_hash_length: options.chunk_hash_length as u32,
            chunking_algorithm: chunk_dictionary::chunker_parameters::ChunkingAlgorithm::FixedSize
                as i32,
        },
    };

    // Explicity drop the stream so we can reduce the lifetime of references to
    // variables we need to create the result object.
    drop(chunk_stream);

    let source_hash = source_hasher.finalize().to_vec();

    let file_header = chunk_dictionary::ChunkDictionary {
        rebuild_order: chunk_order.iter().map(|&index| index as u32).collect(),
        application_version: PKG_VERSION.to_string(),
        chunk_descriptors: archive_chunks,
        source_checksum: source_hash.clone(),
        source_total_size: source_length as u64,
        chunker_params: Some(chunker_params),
        chunk_compression: Some(options.compression.into()),
        metadata: options.metadata.clone(),
    };

    let header_buf = crate::header::build(&file_header, None).expect("Failed to create header");

    output
        .write_all(&header_buf)
        .await
        .map_err(CreateArchiveError::OutputWriteError)?;

    temp_file
        .rewind()
        .await
        .map_err(CreateArchiveError::TempFileError)?;

    io::copy(&mut temp_file, &mut output)
        .await
        .map_err(CreateArchiveError::OutputWriteError)?;

    Ok(CreateArchiveResult {
        source_length,
        source_hash,
        header: file_header.clone(),
    })
}
