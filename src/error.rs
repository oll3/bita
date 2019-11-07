pub enum Error {
    NotAnArchive(String),
    ChecksumMismatch(String),
    IO(String, std::io::Error),
    Protobuf(String, protobuf::ProtobufError),
    #[cfg(feature = "lzma-compression")]
    LZMA(String, lzma::LzmaError),
    CURL(String, curl::Error),
    Hyper(String, hyper::Error),
    Http(String, hyper::http::Error),
    Tls(String, hyper_tls::Error),
    Other(String),
}

#[cfg(feature = "lzma-compression")]
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

impl From<(&str, hyper::Error)> for Error {
    fn from((desc, e): (&str, hyper::Error)) -> Self {
        Error::Hyper(desc.to_owned(), e)
    }
}

impl From<(&str, hyper::http::Error)> for Error {
    fn from((desc, e): (&str, hyper::http::Error)) -> Self {
        Error::Http(desc.to_owned(), e)
    }
}

impl From<(&str, hyper_tls::Error)> for Error {
    fn from((desc, e): (&str, hyper_tls::Error)) -> Self {
        Error::Tls(desc.to_owned(), e)
    }
}

impl From<&str> for Error {
    fn from(desc: &str) -> Self {
        Error::Other(desc.to_owned())
    }
}

impl From<String> for Error {
    fn from(desc: String) -> Self {
        Error::Other(desc)
    }
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NotAnArchive(desc) => write!(f, "{}", desc),
            Error::ChecksumMismatch(desc) => write!(f, "{}", desc),
            Error::IO(desc, e) => write!(f, "{}: {:?}", desc, e),
            Error::Protobuf(desc, e) => write!(f, "{}: {:?}", desc, e),
            #[cfg(feature = "lzma-compression")]
            Error::LZMA(desc, e) => write!(f, "{}: {:?}", desc, e),
            Error::CURL(desc, e) => write!(f, "{}: {:?}", desc, e),
            Error::Hyper(desc, e) => write!(f, "{}: {:?}", desc, e),
            Error::Http(desc, e) => write!(f, "{}: {:?}", desc, e),
            Error::Tls(desc, e) => write!(f, "{}: {:?}", desc, e),
            Error::Other(desc) => write!(f, "{}", desc),
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NotAnArchive(ref desc) => write!(f, "{}", desc),
            Error::ChecksumMismatch(ref desc) => write!(f, "{}", desc),
            Error::IO(ref desc, ref e) => write!(f, "{}: {}", desc, e),
            Error::Protobuf(ref desc, ref e) => write!(f, "{}: {}", desc, e),
            #[cfg(feature = "lzma-compression")]
            Error::LZMA(ref desc, ref e) => write!(f, "{}: {}", desc, e),
            Error::CURL(ref desc, ref e) => write!(f, "{}: {}", desc, e),
            Error::Hyper(ref desc, ref e) => write!(f, "{}: {}", desc, e),
            Error::Http(ref desc, ref e) => write!(f, "{}: {}", desc, e),
            Error::Tls(ref desc, ref e) => write!(f, "{}: {}", desc, e),
            Error::Other(ref desc) => write!(f, "{}", desc),
        }
    }
}

impl std::error::Error for Error {}
