mod archive;
mod chunk;
mod chunk_index;
mod chunk_location_map;
mod compression;
mod hashsum;
mod http_range_request;
mod reader;
mod reader_remote;
mod rolling_hash;

pub mod chunker;
pub mod clone;
pub mod header;

pub use archive::{Archive, ArchiveError};
pub use chunk::{ArchiveChunk, Chunk, CompressedArchiveChunk, CompressedChunk, VerifiedChunk};
pub use chunk_index::{ChunkIndex, ChunkLocation, ReorderOp};
pub use compression::{Compression, CompressionError};
pub use hashsum::HashSum;
pub use reader::Reader;
pub use reader_remote::{ReaderRemote, ReaderRemoteError};

pub mod chunk_dictionary {
    include!(concat!(env!("OUT_DIR"), "/chunk_dictionary.rs"));
}
