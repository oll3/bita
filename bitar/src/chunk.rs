#![allow(clippy::len_without_is_empty)]
use bytes::Bytes;

use crate::{Compression, CompressionError, HashSum};

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
    /// Create a compressed chunk.
    #[inline]
    pub fn compress(self, compression: Compression) -> Result<CompressedChunk, CompressionError> {
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
            hash_sum: HashSum::b2_digest(&chunk.data()),
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
    pub(crate) compression: Compression,
}

impl CompressedChunk {
    /// Create a compressed chunk.
    pub fn try_compress(
        compression: Compression,
        chunk: Chunk,
    ) -> Result<CompressedChunk, CompressionError> {
        Ok(compression.compress(chunk)?)
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
        Ok(Compression::decompress(self)?)
    }
    /// Compression used for chunk.
    #[inline]
    pub fn compression(&self) -> Compression {
        self.compression
    }
    #[inline]
    pub fn into_inner(self) -> (Compression, Bytes) {
        (self.compression, self.data)
    }
}
