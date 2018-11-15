#[derive(Debug)]
pub struct BaseConfig {
    pub force_create: bool,
}

#[derive(Debug)]
pub struct CompressConfig {
    pub base: BaseConfig,

    // Use stdin if input not given
    pub input: Option<String>,
    pub output: String,
    pub truncate_hash: Option<usize>,
}
#[derive(Debug)]
pub struct UnpackConfig {
    pub base: BaseConfig,
}

#[derive(Debug)]
pub enum Config {
    Compress(CompressConfig),
    Unpack(UnpackConfig),
}
