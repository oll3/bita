use brotli::enc::backward_references::BrotliEncoderParams;
use bytes::Bytes;
use std::io::Write;

use crate::{
    chunk_dictionary::{chunk_compression::CompressionType, ChunkCompression},
    Chunk, CompressedChunk,
};

#[derive(Debug)]
pub enum CompressionError {
    IO(std::io::Error),
    #[cfg(feature = "lzma-compression")]
    LZMA(lzma::LzmaError),
}
impl std::error::Error for CompressionError {}
impl std::fmt::Display for CompressionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IO(err) => write!(f, "i/o error: {}", err),
            #[cfg(feature = "lzma-compression")]
            Self::LZMA(err) => write!(f, "LZMA error: {}", err),
        }
    }
}
impl From<std::io::Error> for CompressionError {
    fn from(e: std::io::Error) -> Self {
        Self::IO(e)
    }
}
#[cfg(feature = "lzma-compression")]
impl From<lzma::LzmaError> for CompressionError {
    fn from(e: lzma::LzmaError) -> Self {
        Self::LZMA(e)
    }
}

/// Compression helper type.
#[derive(Debug, Clone, Copy)]
pub enum Compression {
    None,
    #[cfg(feature = "lzma-compression")]
    LZMA(u32),
    #[cfg(feature = "zstd-compression")]
    ZSTD(u32),
    Brotli(u32),
}

impl std::fmt::Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(feature = "lzma-compression")]
            Compression::LZMA(ref level) => write!(f, "LZMA({})", level),
            #[cfg(feature = "zstd-compression")]
            Compression::ZSTD(ref level) => write!(f, "ZSTD({})", level),
            Compression::Brotli(ref level) => write!(f, "Brotli({})", level),
            Compression::None => write!(f, "None"),
        }
    }
}

impl From<Compression> for ChunkCompression {
    fn from(c: Compression) -> Self {
        let (chunk_compression, chunk_compression_level) = match c {
            #[cfg(feature = "lzma-compression")]
            Compression::LZMA(ref level) => (CompressionType::Lzma, *level),
            #[cfg(feature = "zstd-compression")]
            Compression::ZSTD(ref level) => (CompressionType::Zstd, *level),
            Compression::Brotli(ref level) => (CompressionType::Brotli, *level),
            Compression::None => (CompressionType::None, 0),
        };
        Self {
            compression: chunk_compression as i32,
            compression_level: chunk_compression_level,
        }
    }
}

impl Compression {
    /// Compress a block of data with set compression.
    pub(crate) fn compress(self, chunk: Chunk) -> Result<CompressedChunk, CompressionError> {
        let source_size = chunk.len();
        let result = match self {
            #[cfg(feature = "lzma-compression")]
            Compression::LZMA(ref level) => {
                use lzma::LzmaWriter;
                use std::io::prelude::*;
                let mut result = Vec::with_capacity(chunk.len());
                {
                    let mut f = LzmaWriter::new_compressor(&mut result, *level)?;
                    f.write_all(chunk.data())?;
                    f.finish()?;
                }
                Bytes::from(result)
            }
            #[cfg(feature = "zstd-compression")]
            Compression::ZSTD(ref level) => {
                let mut result = Vec::with_capacity(chunk.len());
                zstd::stream::copy_encode(chunk.data(), &mut result, *level as i32)?;
                Bytes::from(result)
            }
            Compression::Brotli(ref level) => {
                let mut result = Vec::with_capacity(chunk.len());
                let params = BrotliEncoderParams {
                    quality: *level as i32,
                    magic_number: false,
                    ..Default::default()
                };
                {
                    let mut writer =
                        brotli::CompressorWriter::with_params(&mut result, 1024 * 1024, &params);
                    writer.write_all(&chunk.data())?;
                }
                Bytes::from(result)
            }
            Compression::None => chunk.into_inner(),
        };
        Ok(CompressedChunk {
            data: result,
            source_size,
            compression: self,
        })
    }
    /// Decompress a block of data using the set compression.
    pub(crate) fn decompress(compressed: CompressedChunk) -> Result<Chunk, CompressionError> {
        match compressed.compression() {
            #[cfg(feature = "lzma-compression")]
            Compression::LZMA(_) => {
                use lzma::LzmaWriter;
                use std::io::prelude::*;
                let mut output = Vec::with_capacity(compressed.source_size);
                let mut f = LzmaWriter::new_decompressor(&mut output)?;
                f.write_all(compressed.data())?;
                f.finish()?;
                Ok(Chunk::from(output))
            }
            #[cfg(feature = "zstd-compression")]
            Compression::ZSTD(_) => {
                let mut output = Vec::with_capacity(compressed.source_size);
                zstd::stream::copy_decode(compressed.data(), &mut output)?;
                Ok(Chunk::from(output))
            }
            Compression::Brotli(_) => {
                let mut output = Vec::with_capacity(compressed.source_size);
                let mut input_slice: &[u8] = compressed.data();
                brotli::BrotliDecompress(&mut input_slice, &mut output)?;
                Ok(Chunk::from(output))
            }
            Compression::None => {
                // Archived chunk is NOT compressed
                Ok(Chunk::from(compressed.into_inner().1))
            }
        }
    }
}
