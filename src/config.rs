use bita::archive::HashBuf;
use bita::compression::Compression;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct ChunkerConfig {
    pub chunk_filter_bits: u32,
    pub min_chunk_size: usize,
    pub max_chunk_size: usize,
    pub hash_window_size: usize,
    pub compression_level: u32,
    pub compression: Compression,
}

#[derive(Debug, Clone)]
pub struct CompressConfig {
    pub force_create: bool,

    // Use stdin if input not given
    pub input: Option<PathBuf>,
    pub output: PathBuf,
    pub temp_file: PathBuf,
    pub hash_length: usize,
    pub chunker_config: ChunkerConfig,
}

#[derive(Debug, Clone)]
pub struct CloneConfig {
    pub force_create: bool,
    pub input: String,
    pub output: PathBuf,
    pub seed_stdin: bool,
    pub seed_files: Vec<PathBuf>,
    pub header_checksum: Option<HashBuf>,
    pub http_retry_count: u32,
    pub http_retry_delay: Option<std::time::Duration>,
    pub http_timeout: Option<std::time::Duration>,
    pub verify_output: bool,
}

#[derive(Debug, Clone)]
pub struct InfoConfig {
    pub input: String,
}

#[derive(Debug, Clone)]
pub struct DiffConfig {
    pub input_a: PathBuf,
    pub input_b: PathBuf,
    pub chunker_config: ChunkerConfig,
}

#[derive(Debug, Clone)]
pub enum Config {
    Compress(CompressConfig),
    Clone(CloneConfig),
    Info(InfoConfig),
    Diff(DiffConfig),
}
