use brotli::enc::backward_references::BrotliEncoderParams;
use bytes::Bytes;
use std::io::Write;

use crate::chunk_dictionary::{chunk_compression::CompressionType, ChunkCompression};
use crate::Error;

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

impl std::convert::TryFrom<ChunkCompression> for Compression {
    type Error = Error;
    fn try_from(c: ChunkCompression) -> Result<Self, Self::Error> {
        match CompressionType::from_i32(c.compression) {
            #[cfg(feature = "lzma-compression")]
            Some(CompressionType::Lzma) => Ok(Self::LZMA(c.compression_level)),
            #[cfg(not(feature = "lzma-compression"))]
            Some(CompressionType::Lzma) => panic!("LZMA compression not enabled"),
            #[cfg(feature = "zstd-compression")]
            Some(CompressionType::Zstd) => Ok(Self::ZSTD(c.compression_level)),
            #[cfg(not(feature = "zstd-compression"))]
            Some(CompressionType::Zstd) => panic!("ZSTD compression not enabled"),
            Some(CompressionType::Brotli) => Ok(Self::Brotli(c.compression_level)),
            Some(CompressionType::None) => Ok(Self::None),
            None => Err(Error::UnknownCompression),
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
    // Compress a block of data with set compression
    pub fn compress(self, input: Bytes) -> Result<Bytes, Error> {
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

    // Decompress a block of data using the set compression
    pub fn decompress(self, input: Bytes, size_hint: usize) -> Result<Bytes, Error> {
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
