#[macro_use]
extern crate error_chain;
extern crate atty;
extern crate blake2;
extern crate crossbeam_channel;
extern crate curl;
extern crate lzma;
extern crate protobuf;
extern crate threadpool;
extern crate zstd;

mod archive;
mod archive_reader;
mod buzhash;
mod chunk_dictionary;
mod chunker;
mod chunker_utils;
mod file_archive_backend;
mod para_pipe;
mod remote_archive_backend;
mod string_utils;

pub mod clone_cmd;
pub mod compress_cmd;
pub mod compression;
pub mod config;
pub mod errors;
