#![recursion_limit = "256"]

mod archive;
mod chunk_index;
mod chunk_location_map;
mod chunker;
mod clone;
mod compression;
mod error;
mod hashsum;
mod header;
mod http_range_request;
mod output;
mod reader;
mod rolling_hash;
mod seed;

pub use archive::Archive;
pub use chunk_index::{ChunkIndex, ReorderOp};
pub use chunk_location_map::{ChunkLocation, ChunkLocationMap};
pub use chunker::{Chunker, ChunkerConfig, ChunkerFilterBits, ChunkerFilterConfig};
pub use clone::{clone_in_place, clone_using_archive};
pub use compression::Compression;
pub use error::Error;
pub use hashsum::HashSum;
pub use header::{build_header, FILE_MAGIC, PRE_HEADER_SIZE};
pub use output::{Output, OutputFile};
pub use reader::{Reader, ReaderLocal, ReaderRemote};
pub use rolling_hash::{BuzHash, RollSum, RollingHash};
pub use seed::Seed;

pub mod chunk_dictionary {
    include!(concat!(env!("OUT_DIR"), "/chunk_dictionary.rs"));
}
