use brotli;
use brotli::enc::backward_references::BrotliEncoderParams;
use std::io::Write;

use crate::chunk_dictionary;
use crate::chunk_dictionary::ChunkCompression_CompressionType;
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

impl From<chunk_dictionary::ChunkCompression> for Compression {
    fn from(compression: chunk_dictionary::ChunkCompression) -> Self {
        match compression.compression {
            #[cfg(feature = "lzma-compression")]
            ChunkCompression_CompressionType::LZMA => {
                Compression::LZMA(compression.compression_level)
            }
            #[cfg(not(feature = "lzma-compression"))]
            ChunkCompression_CompressionType::LZMA => panic!("LZMA compression not enabled"),
            #[cfg(feature = "zstd-compression")]
            ChunkCompression_CompressionType::ZSTD => {
                Compression::ZSTD(compression.compression_level)
            }
            #[cfg(not(feature = "zstd-compression"))]
            ChunkCompression_CompressionType::ZSTD => panic!("ZSTD compression not enabled"),
            ChunkCompression_CompressionType::BROTLI => {
                Compression::Brotli(compression.compression_level)
            }
            ChunkCompression_CompressionType::NONE => Compression::None,
        }
    }
}

impl From<Compression> for chunk_dictionary::ChunkCompression {
    fn from(compression: Compression) -> Self {
        let (chunk_compression, chunk_compression_level) = match compression {
            #[cfg(feature = "lzma-compression")]
            Compression::LZMA(ref level) => (ChunkCompression_CompressionType::LZMA, *level),
            #[cfg(feature = "zstd-compression")]
            Compression::ZSTD(ref level) => (ChunkCompression_CompressionType::ZSTD, *level),
            Compression::Brotli(ref level) => (ChunkCompression_CompressionType::BROTLI, *level),
            Compression::None => (ChunkCompression_CompressionType::NONE, 0),
        };
        chunk_dictionary::ChunkCompression {
            compression: chunk_compression,
            compression_level: chunk_compression_level,
            unknown_fields: std::default::Default::default(),
            cached_size: std::default::Default::default(),
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
