pub enum Error {
    NotAnArchive,
    ChecksumMismatch,
    UnexpectedEnd,
    CorruptArchive,
    UnknownChunkingAlgorithm,
    UnknownCompression,
    IO(std::io::Error),
    DictionaryDecode(prost::DecodeError),
    DictionaryEncode(prost::EncodeError),
    #[cfg(feature = "lzma-compression")]
    LZMA(lzma::LzmaError),
    Http(reqwest::Error),
    ThreadJoin(tokio::task::JoinError),
}

impl std::error::Error for Error {}

#[cfg(feature = "lzma-compression")]
impl From<lzma::LzmaError> for Error {
    fn from(e: lzma::LzmaError) -> Self {
        Self::LZMA(e)
    }
}
impl From<prost::DecodeError> for Error {
    fn from(e: prost::DecodeError) -> Self {
        Self::DictionaryDecode(e)
    }
}

impl From<prost::EncodeError> for Error {
    fn from(e: prost::EncodeError) -> Self {
        Self::DictionaryEncode(e)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::IO(e)
    }
}

impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Self {
        Self::Http(e)
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(e: tokio::task::JoinError) -> Self {
        Self::ThreadJoin(e)
    }
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NotAnArchive => write!(f, "NotAnArchive"),
            Error::ChecksumMismatch => write!(f, "ChecksumMismatch"),
            Error::UnexpectedEnd => write!(f, "UnexpectedEnd"),
            Error::CorruptArchive => write!(f, "CorruptArchive"),
            Error::UnknownChunkingAlgorithm => write!(f, "UnknownChunkingAlgorithm"),
            Error::UnknownCompression => write!(f, "UnknownCompression"),
            Error::IO(e) => write!(f, "IO({:?})", e),
            Error::DictionaryEncode(e) => write!(f, "DictionaryEncode({:?})", e),
            Error::DictionaryDecode(e) => write!(f, "DictionaryDecode({:?})", e),
            #[cfg(feature = "lzma-compression")]
            Error::LZMA(e) => write!(f, "LZMA({:?})", e),
            Error::Http(e) => write!(f, "Http({:?})", e),
            Error::ThreadJoin(e) => write!(f, "ThreadJoin({:?})", e),
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NotAnArchive => write!(f, "is not an archive"),
            Error::ChecksumMismatch => write!(f, "checksum mismatch"),
            Error::UnexpectedEnd => write!(f, "unexpected end"),
            Error::CorruptArchive => write!(f, "corrupt archive"),
            Error::UnknownChunkingAlgorithm => write!(f, "unknown chunking algorithm"),
            Error::UnknownCompression => write!(f, "unknown compression"),
            Error::IO(e) => write!(f, "i/o error: {}", e),
            Error::DictionaryEncode(e) => write!(f, "failed to encode dictionary: {}", e),
            Error::DictionaryDecode(e) => write!(f, "failed to decode dictionary: {}", e),
            #[cfg(feature = "lzma-compression")]
            Error::LZMA(e) => write!(f, "lzma compression error: {}", e),
            Error::Http(e) => write!(f, "http error: {}", e),
            Error::ThreadJoin(e) => write!(f, "error joining thread: {}", e),
        }
    }
}
