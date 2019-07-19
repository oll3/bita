pub enum Error {
    NotAnArchive(String),
    ChecksumMismatch(String),
    IO(String, std::io::Error),
    Protobuf(String, protobuf::ProtobufError),
    LZMA(String, lzma::LzmaError),
    CURL(String, curl::Error),
    Other(String),
}

impl From<(&str, lzma::LzmaError)> for Error {
    fn from((desc, e): (&str, lzma::LzmaError)) -> Self {
        Error::LZMA(desc.to_owned(), e)
    }
}

impl From<(&str, protobuf::ProtobufError)> for Error {
    fn from((desc, e): (&str, protobuf::ProtobufError)) -> Self {
        Error::Protobuf(desc.to_owned(), e)
    }
}

impl From<(&str, curl::Error)> for Error {
    fn from((desc, e): (&str, curl::Error)) -> Self {
        Error::CURL(desc.to_owned(), e)
    }
}

impl From<(&str, std::io::Error)> for Error {
    fn from((desc, e): (&str, std::io::Error)) -> Self {
        Error::IO(desc.to_owned(), e)
    }
}

impl From<(String, std::io::Error)> for Error {
    fn from((desc, e): (String, std::io::Error)) -> Self {
        Error::IO(desc, e)
    }
}

impl From<&str> for Error {
    fn from(desc: &str) -> Self {
        Error::Other(desc.to_owned())
    }
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NotAnArchive(ref desc) => write!(f, "{}", desc),
            Error::ChecksumMismatch(ref desc) => write!(f, "{}", desc),
            Error::IO(ref desc, ref e) => write!(f, "{}: {}", desc, e),
            Error::Protobuf(ref desc, ref e) => write!(f, "{}: {}", desc, e),
            Error::LZMA(ref desc, ref e) => write!(f, "{}: {}", desc, e),
            Error::CURL(ref desc, ref e) => write!(f, "{}: {}", desc, e),
            Error::Other(ref desc) => write!(f, "{}", desc),
        }
    }
}

impl std::error::Error for Error {}
