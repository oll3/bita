mod archive;
mod chunk;
mod chunk_index;
mod chunk_location_map;
mod chunk_offset;
mod clone_output;
mod compression;
mod hashsum;
mod rolling_hash;

pub mod api;

pub mod archive_reader;
pub mod chunker;
pub mod header;

pub use archive::{Archive, ArchiveError};
pub use chunk::{
    ArchiveChunk, Chunk, CompressedArchiveChunk, CompressedChunk, HashSumMismatchError,
    VerifiedChunk,
};
pub use chunk_index::{ChunkIndex, ChunkLocation, ReorderOp};
pub use chunk_offset::ChunkOffset;
pub use clone_output::CloneOutput;
pub use compression::{
    Compression, CompressionAlgorithm, CompressionError, CompressionLevelOutOfRangeError,
};
pub use hashsum::HashSum;

pub mod chunk_dictionary {
    include!(concat!(env!("OUT_DIR"), "/chunk_dictionary.rs"));
}
