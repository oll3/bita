#![recursion_limit = "256"]

pub mod archive;
pub mod archive_reader;
pub mod chunk_index;
pub mod chunk_location_map;
pub mod chunker;
pub mod compression;
mod error;
mod hashsum;
mod http_range_request;
mod reader_backend;
pub mod rolling_hash;

pub use error::Error;
pub use hashsum::HashSum;
pub use reader_backend::ReaderBackend;

pub mod chunk_dictionary {
    include!(concat!(env!("OUT_DIR"), "/chunk_dictionary.rs"));
}
