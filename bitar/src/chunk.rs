#![allow(clippy::len_without_is_empty)]
use bytes::Bytes;
use std::fmt;

#[cfg(feature = "compress")]
use crate::Compression;
use crate::{CompressionAlgorithm, CompressionError, HashSum};

/// A single chunk.
///
/// Represents a single chunk of a file. Is not compressed.
#[derive(Debug, Clone, PartialEq)]
pub struct Chunk(pub(crate) Bytes);

impl<T> From<T> for Chunk
where
    T: Into<bytes::Bytes>,
{
    fn from(b: T) -> Self {
        Self(b.into())
    }
}

impl Chunk {
    /// Chunk data.
    #[inline]
    pub fn data(&self) -> &[u8] {
        &self.0[..]
    }
    /// Size of chunk.
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }
    /// Create a verified chunk by calculating a hash sum for it.
    #[inline]
    pub fn verify(self) -> VerifiedChunk {
        VerifiedChunk::from(self)
    }
    #[cfg(feature = "compress")]
    /// Create a compressed chunk.
    #[inline]
    pub fn compress(
        self,
        compression: Option<Compression>,
    ) -> Result<CompressedChunk, CompressionError> {
        CompressedChunk::try_compress(compression, self)
    }
    #[inline]
    pub fn into_inner(self) -> Bytes {
        self.0
    }
}

/// A chunk with verified hash sum.
#[derive(Debug, Clone)]
pub struct VerifiedChunk {
    pub(crate) chunk: Chunk,
    pub(crate) hash_sum: HashSum,
}

impl From<Chunk> for VerifiedChunk {
    fn from(chunk: Chunk) -> Self {
        Self::new(chunk)
    }
}

impl VerifiedChunk {
    /// Create a new verified chunk by calculating a hash of it.
    pub fn new(chunk: Chunk) -> Self {
        Self {
            hash_sum: HashSum::b2_digest(chunk.data()),
            chunk,
        }
    }
    /// Size of chunk.
    #[inline]
    pub fn len(&self) -> usize {
        self.chunk.len()
    }
    /// Get chunk.
    #[inline]
    pub fn chunk(&self) -> &Chunk {
        &self.chunk
    }
    /// Get chunk data.
    #[inline]
    pub fn data(&self) -> &[u8] {
        self.chunk.data()
    }
    /// Get hash sum of chunk.
    #[inline]
    pub fn hash(&self) -> &HashSum {
        &self.hash_sum
    }
    /// Split into hash and chunk.
    #[inline]
    pub fn into_parts(self) -> (HashSum, Chunk) {
        (self.hash_sum, self.chunk)
    }
}

/// A compressed chunk.
#[derive(Debug, Clone)]
pub struct CompressedChunk {
    pub(crate) data: Bytes,
    pub(crate) source_size: usize,
    pub(crate) compression: Option<CompressionAlgorithm>,
}

impl CompressedChunk {
    /// Create a compressed chunk.
    #[cfg(feature = "compress")]
    pub fn try_compress(
        compression: Option<Compression>,
        chunk: Chunk,
    ) -> Result<CompressedChunk, CompressionError> {
        if let Some(compression) = compression {
            Ok(CompressedChunk {
                source_size: chunk.len(),
                data: compression.compress(chunk.0)?,
                compression: Some(compression.algorithm),
            })
        } else {
            Ok(CompressedChunk {
                source_size: chunk.len(),
                data: chunk.0,
                compression: None,
            })
        }
    }
    /// Chunk data.
    #[inline]
    pub fn data(&self) -> &[u8] {
        &self.data[..]
    }
    /// Size of chunk.
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }
    /// Decompress the chunk.
    pub fn decompress(self) -> Result<Chunk, CompressionError> {
        Ok(match self.compression {
            Some(compression) => Chunk::from(compression.decompress(self.data, self.source_size)?),
            // Chunk not compressed.
            None => Chunk::from(self.data),
        })
    }
    /// Compression used for chunk.
    #[inline]
    pub fn compression(&self) -> Option<CompressionAlgorithm> {
        self.compression
    }
    #[inline]
    pub fn into_inner(self) -> (Option<CompressionAlgorithm>, Bytes) {
        (self.compression, self.data)
    }
}

/// A possibly compressed chunk fetched from archive.
///
/// Chunk might be compressed and needs to be decompressed before being verified.
#[derive(Debug, Clone)]
pub struct CompressedArchiveChunk {
    pub(crate) chunk: CompressedChunk,
    pub(crate) expected_hash: HashSum,
}

impl CompressedArchiveChunk {
    /// Size of chunk.
    pub fn len(&self) -> usize {
        self.chunk.len()
    }
    /// Decompress the chunk.
    pub fn decompress(self) -> Result<ArchiveChunk, CompressionError> {
        Ok(ArchiveChunk {
            chunk: self.chunk.decompress()?,
            expected_hash: self.expected_hash,
        })
    }
}

#[derive(Debug)]
pub struct HashSumMismatchError {
    expected: HashSum,
    got: HashSum,
    pub invalid_chunk: Chunk,
}
impl std::error::Error for HashSumMismatchError {}
impl fmt::Display for HashSumMismatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "expected hash {} but got {}", self.expected, self.got)
    }
}

/// An unverified chunk fetched from archive.
#[derive(Debug, Clone)]
pub struct ArchiveChunk {
    pub(crate) chunk: Chunk,
    pub(crate) expected_hash: HashSum,
}

impl ArchiveChunk {
    /// Size of chunk.
    pub fn len(&self) -> usize {
        self.chunk.len()
    }
    /// Verify an unverified chunk.
    ///
    /// Results in a verified chunk or an error if the chunk hash sum doesn't
    /// match with the expected one.
    pub fn verify(self) -> Result<VerifiedChunk, HashSumMismatchError> {
        let hash_sum = HashSum::b2_digest(self.chunk.data());
        if hash_sum != self.expected_hash {
            Err(HashSumMismatchError {
                expected: self.expected_hash,
                got: hash_sum,
                invalid_chunk: self.chunk,
            })
        } else {
            Ok(VerifiedChunk {
                chunk: self.chunk,
                hash_sum,
            })
        }
    }
}
