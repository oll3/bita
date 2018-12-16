use chunk_dictionary;

#[derive(Debug)]
pub struct BaseConfig {
    pub force_create: bool,
}

#[derive(Debug)]
pub struct CompressConfig {
    pub base: BaseConfig,

    // Use stdin if input not given
    pub input: String,
    pub output: String,
    pub temp_file: String,
    pub hash_length: usize,
    pub chunk_filter_bits: u32,
    pub min_chunk_size: usize,
    pub max_chunk_size: usize,
    pub hash_window_size: usize,
    pub compression_level: u32,
    pub compression: chunk_dictionary::ChunkCompression_CompressionType,
}
#[derive(Debug)]
pub struct UnpackConfig {
    pub base: BaseConfig,

    pub input: String,
    pub output: String,
    pub seed_files: Vec<String>,
    pub seed_stdin: bool,
}

#[derive(Debug)]
pub enum Config {
    Compress(CompressConfig),
    Unpack(UnpackConfig),
}
