use brotli::enc::backward_references::BrotliEncoderParams;
use std::io::Write;

use crate::chunk_dictionary::{chunk_compression::CompressionType, ChunkCompression};
use crate::error::Error;

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
            None => Err(Error::Other("unknown compression type".to_string())),
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
    pub fn compress(self, data: &[u8]) -> Result<Vec<u8>, Error> {
        match self {
            #[cfg(feature = "lzma-compression")]
            Compression::LZMA(ref level) => {
                use lzma::LzmaWriter;
                use std::io::prelude::*;
                let mut result = vec![];
                {
                    let mut f = LzmaWriter::new_compressor(&mut result, *level)
                        .map_err(|e| ("failed to create lzma compressor", e))?;
                    f.write_all(data)
                        .map_err(|e| ("failed compress with lzma", e))?;
                    f.finish()
                        .map_err(|e| ("failed to finish lzma compression", e))?;
                }
                Ok(result)
            }
            #[cfg(feature = "zstd-compression")]
            Compression::ZSTD(ref level) => {
                let mut result = vec![];
                let data = data.to_vec();
                zstd::stream::copy_encode(&data[..], &mut result, *level as i32)
                    .map_err(|e| ("failed compress with zstd", e))?;
                Ok(result)
            }
            Compression::Brotli(ref level) => {
                let mut result = vec![];
                let params = BrotliEncoderParams {
                    quality: *level as i32,
                    magic_number: false,
                    ..Default::default()
                };
                {
                    let mut writer =
                        brotli::CompressorWriter::with_params(&mut result, 1024 * 1024, &params);
                    writer
                        .write_all(&data)
                        .map_err(|e| ("failed compress with brotli", e))?;
                }
                Ok(result)
            }
            Compression::None => Ok(data.to_vec()),
        }
    }

    // Decompress a block of data using the set compression
    pub fn decompress(self, input: Vec<u8>, output: &mut Vec<u8>) -> Result<(), Error> {
        match self {
            #[cfg(feature = "lzma-compression")]
            Compression::LZMA(_) => {
                use lzma::LzmaWriter;
                use std::io::prelude::*;
                output.clear();
                let mut f = LzmaWriter::new_decompressor(output)
                    .map_err(|e| ("failed to create lzma decompressor", e))?;
                f.write_all(&input)
                    .map_err(|e| ("failed to decompress using lzma", e))?;
                f.finish()
                    .map_err(|e| ("failed to finish lzma decompression", e))?;
            }
            #[cfg(feature = "zstd-compression")]
            Compression::ZSTD(_) => {
                output.clear();
                zstd::stream::copy_decode(&input[..], output)
                    .map_err(|e| ("failed to decompress using zstd", e))?;
            }
            Compression::Brotli(_) => {
                output.clear();
                let mut decompressor = brotli::DecompressorWriter::new(output, 1024 * 1024);
                decompressor
                    .write_all(&input)
                    .map_err(|e| ("failed to decompress using brotli", e))?;
            }
            Compression::None => {
                // Archived chunk is NOT compressed
                *output = input;
            }
        }

        Ok(())
    }
}
