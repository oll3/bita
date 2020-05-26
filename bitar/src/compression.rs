use brotli::enc::backward_references::BrotliEncoderParams;
use bytes::Bytes;
use std::io::Write;

use crate::chunk_dictionary::{chunk_compression::CompressionType, ChunkCompression};

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
    pub fn compress(self, input: Bytes) -> Result<Bytes, CompressionError> {
        match self {
            #[cfg(feature = "lzma-compression")]
            Compression::LZMA(ref level) => {
                use lzma::LzmaWriter;
                use std::io::prelude::*;
                let mut result = Vec::with_capacity(input.len());
                {
                    let mut f = LzmaWriter::new_compressor(&mut result, *level)?;
                    f.write_all(&input[..])?;
                    f.finish()?;
                }
                Ok(Bytes::from(result))
            }
            #[cfg(feature = "zstd-compression")]
            Compression::ZSTD(ref level) => {
                let mut result = Vec::with_capacity(input.len());
                zstd::stream::copy_encode(&input[..], &mut result, *level as i32)?;
                Ok(Bytes::from(result))
            }
            Compression::Brotli(ref level) => {
                let mut result = Vec::with_capacity(input.len());
                let params = BrotliEncoderParams {
                    quality: *level as i32,
                    magic_number: false,
                    ..Default::default()
                };
                {
                    let mut writer =
                        brotli::CompressorWriter::with_params(&mut result, 1024 * 1024, &params);
                    writer.write_all(&input)?;
                }
                Ok(Bytes::from(result))
            }
            Compression::None => Ok(input),
        }
    }
    /// Decompress a block of data using the set compression.
    pub fn decompress(self, input: Bytes, size_hint: usize) -> Result<Bytes, CompressionError> {
        match self {
            #[cfg(feature = "lzma-compression")]
            Compression::LZMA(_) => {
                use lzma::LzmaWriter;
                use std::io::prelude::*;
                let mut output = Vec::with_capacity(size_hint);
                let mut f = LzmaWriter::new_decompressor(&mut output)?;
                f.write_all(&input)?;
                f.finish()?;
                Ok(Bytes::from(output))
            }
            #[cfg(feature = "zstd-compression")]
            Compression::ZSTD(_) => {
                let mut output = Vec::with_capacity(size_hint);
                zstd::stream::copy_decode(&input[..], &mut output)?;
                Ok(Bytes::from(output))
            }
            Compression::Brotli(_) => {
                let mut output = Vec::with_capacity(size_hint);
                {
                    let mut decompressor =
                        brotli::DecompressorWriter::new(&mut output, 1024 * 1024);
                    decompressor.write_all(&input[..])?;
                }
                Ok(Bytes::from(output))
            }
            Compression::None => {
                // Archived chunk is NOT compressed
                Ok(input)
            }
        }
    }
}
