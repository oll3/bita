use std::io;

use futures_util::Stream;
use tokio::io::AsyncRead;

use super::{fixed_size::FixedSizeChunker, rolling_hash::RollingHashChunker, StreamingChunker};
use crate::{
    rolling_hash::{BuzHash, RollSum},
    Chunk,
};

/// Helper type for creating a bit mask to use while scanning for chunk boundaries.
///
/// The bit mask given from the filter is used to match against the rolling hash sum.
/// When `sum | filter_mask == sum` then we have found a chunk boundary.
/// That is, with a mask set to 0b1 a chunk will be found every 2nd byte on average.
/// With a mask set to 0b11 a chunk will be found every 4th byte on average.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]

pub struct FilterBits(pub u32);

impl FilterBits {
    /// Create new filter mask with an average target size of the given value.
    ///
    /// The actual target size will be the given size rounded down to the closest power of 2 value.
    pub fn from_size(size: u32) -> Self {
        Self(30 - size.leading_zeros())
    }
    /// Create new filter mask from a number of bits.
    ///
    /// Eg 1 => 0b1, 2 => 0b11, 3 => 0b111 etc.
    pub fn from_bits(bits: u32) -> Self {
        Self(bits)
    }
    /// Get the bit mask value of the filter.
    pub fn mask(self) -> u32 {
        !0 >> (32 - self.0)
    }
    /// Get the average target size from the filter.
    pub fn chunk_target_average(self) -> u32 {
        1 << (self.0 + 1)
    }
    /// Get number of bits set in the filter.
    pub fn bits(self) -> u32 {
        self.0
    }
}

/// Filter configuration to use while scanning for chunk boundaries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FilterConfig {
    /// Bit mask filter resulting in an average chunk size.
    pub filter_bits: FilterBits,
    /// No chunks smaller than `min_chunk_size`.
    pub min_chunk_size: usize,
    /// No chunks bigger than `max_chunk_size`.
    pub max_chunk_size: usize,
    /// Number of bytes kept in the rolling hash window while scanning.
    pub window_size: usize,
}

impl Default for FilterConfig {
    fn default() -> FilterConfig {
        FilterConfig {
            filter_bits: FilterBits::from_size(64 * 1024),
            min_chunk_size: 16 * 1024,
            max_chunk_size: 16 * 1024 * 1024,
            window_size: 64,
        }
    }
}

/// Algorithm and configuration to use while scanning for chunk boundaries.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Config {
    BuzHash(FilterConfig),
    RollSum(FilterConfig),
    FixedSize(usize),
}

impl Config {
    /// Create an (async) stream of chunks from the given source using config.
    pub fn new_chunker<'r, R>(
        &self,
        source: R,
    ) -> Box<dyn Stream<Item = io::Result<(u64, Chunk)>> + Unpin + Send + 'r>
    where
        R: AsyncRead + Unpin + Send + 'r,
    {
        match self {
            Config::BuzHash(filter) => Box::new(StreamingChunker::new(
                RollingHashChunker::new(BuzHash::new(filter.window_size), filter),
                source,
            )),
            Config::RollSum(filter) => Box::new(StreamingChunker::new(
                RollingHashChunker::new(RollSum::new(filter.window_size), filter),
                source,
            )),
            Config::FixedSize(fixed_size) => Box::new(StreamingChunker::new(
                FixedSizeChunker::new(*fixed_size),
                source,
            )),
        }
    }
}
