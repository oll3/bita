use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use core::pin::Pin;
use core::task::{Context, Poll};
use futures_core::stream::Stream;
use futures_util::StreamExt;
use reqwest::{RequestBuilder, Url};
use std::time::Duration;

use crate::http_range_request;
use crate::reader::Reader;

/// ReaderRemote error.
#[derive(Debug)]
pub enum ReaderRemoteError {
    UnexpectedEnd,
    RequestNotClonable,
    Http(reqwest::Error),
}

impl std::error::Error for ReaderRemoteError {}

impl std::fmt::Display for ReaderRemoteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnexpectedEnd => write!(f, "unexpected end"),
            Self::RequestNotClonable => write!(f, "request is not clonable"),
            Self::Http(err) => write!(f, "http error: {}", err),
        }
    }
}

impl From<reqwest::Error> for ReaderRemoteError {
    fn from(e: reqwest::Error) -> Self {
        Self::Http(e)
    }
}

/// ReaderRemote is a helper for reading archives from a remote http location.
pub struct ReaderRemote {
    request: RequestBuilder,
    retries: u32,
    retry_delay: Duration,
}

impl ReaderRemote {
    /// Create a remote archive reader using RequestBuilder for the http request.
    pub fn from_request(request: RequestBuilder) -> Self {
        Self {
            request,
            retries: 0,
            retry_delay: Duration::from_secs(0),
        }
    }

    /// Create a remote archive reader using an URL and default parameters for the request.
    pub fn from_url(url: Url) -> Self {
        Self::from_request(reqwest::Client::new().get(url))
    }

    /// Set number of times to retry reading from the remote server if the request would fail
    /// for any reason.
    /// The reader will try to reconnect and continue download from where the failure occured.
    /// Any progress made so far should not be lost.
    pub fn retries(mut self, retries: u32) -> Self {
        self.retries = retries;
        self
    }

    /// Set a delay between attempts to reconnect to the remote server.
    /// On failure the reader will wait for the given time before trying to reconnect.
    pub fn retry_delay(mut self, retry_delay: Duration) -> Self {
        self.retry_delay = retry_delay;
        self
    }

    fn read_chunk_stream<'a>(
        &'a mut self,
        start_offset: u64,
        chunk_sizes: &'a [usize],
    ) -> impl Stream<Item = Result<Bytes, ReaderRemoteError>> + 'a {
        let total_size: u64 = chunk_sizes.iter().map(|v| *v as u64).sum();
        ChunkReader {
            request: http_range_request::Builder::new(&self.request, start_offset, total_size)
                .retry(self.retries, self.retry_delay),
            chunk_buf: BytesMut::with_capacity(*chunk_sizes.get(0).unwrap_or(&0)),
            chunk_index: 0,
            chunk_sizes,
        }
    }
}

struct ChunkReader<'a> {
    request: http_range_request::Builder<'a>,
    chunk_buf: BytesMut,
    chunk_index: usize,
    chunk_sizes: &'a [usize],
}

impl<'a> ChunkReader<'a>
where
    Self: Unpin,
{
    fn poll_read(&mut self, cx: &mut Context) -> Poll<Option<Result<Bytes, ReaderRemoteError>>> {
        while self.chunk_index < self.chunk_sizes.len() {
            let chunk_size = self.chunk_sizes[self.chunk_index];
            if self.chunk_buf.len() >= chunk_size {
                self.chunk_index += 1;
                let chunk = self.chunk_buf.split_to(chunk_size).freeze();
                return Poll::Ready(Some(Ok(chunk)));
            }
            match self.request.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(item))) => {
                    self.chunk_buf.extend_from_slice(&item[..]);
                }
                Poll::Ready(None) => {
                    return Poll::Ready(Some(Err(ReaderRemoteError::UnexpectedEnd)))
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                Poll::Pending => return Poll::Pending,
            }
        }
        Poll::Ready(None)
    }
}

impl<'a> Stream for ChunkReader<'a> {
    type Item = Result<Bytes, ReaderRemoteError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.poll_read(cx)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let chunks_left = self.chunk_sizes.len() - self.chunk_index;
        (chunks_left, Some(chunks_left))
    }
}

#[async_trait]
impl Reader for ReaderRemote {
    type Error = ReaderRemoteError;
    async fn read_at(&mut self, offset: u64, size: usize) -> Result<Bytes, ReaderRemoteError> {
        let request = http_range_request::Builder::new(&self.request, offset, size as u64)
            .retry(self.retries, self.retry_delay);

        let mut res = request.single().await?;
        if res.len() >= size {
            // Truncate the response if bigger than requested size
            Ok(res.split_to(size))
        } else if res.len() < size {
            Err(ReaderRemoteError::UnexpectedEnd)
        } else {
            Ok(res)
        }
    }
    fn read_chunks<'a>(
        &'a mut self,
        start_offset: u64,
        chunk_sizes: &'a [usize],
    ) -> Pin<Box<dyn Stream<Item = Result<Bytes, ReaderRemoteError>> + Send + 'a>> {
        Box::pin(self.read_chunk_stream(start_offset, chunk_sizes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::service::{make_service_fn, service_fn};

    async fn new_server(listener: std::net::TcpListener, data: Vec<u8>) {
        hyper::Server::from_tcp(listener)
            .unwrap()
            .serve(make_service_fn(move |_conn| {
                let data = data.clone();
                async move {
                    Ok::<_, std::convert::Infallible>(service_fn(move |req| {
                        // Only respond with the requested range of bytes
                        let range = req
                            .headers()
                            .get("range")
                            .expect("range header")
                            .to_str()
                            .unwrap()[6..]
                            .split('-')
                            .map(|s| s.parse::<u64>().unwrap())
                            .collect::<Vec<u64>>();
                        let start = range[0] as usize;
                        let end = std::cmp::min(range[1] as usize + 1, data.len());
                        let data = data[start..end].to_vec();
                        async move {
                            Ok::<_, hyper::Error>(hyper::Response::new(hyper::Body::from(data)))
                        }
                    }))
                }
            }))
            .await
            .unwrap();
    }
    fn new_reader(port: u16) -> ReaderRemote {
        ReaderRemote::from_url(Url::parse(&format!("http://127.0.0.1:{}", port)).unwrap())
    }
    fn new_listener() -> (std::net::TcpListener, u16) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        (listener, port)
    }

    #[test]
    fn builder() {
        let reader = ReaderRemote::from_url(Url::parse("http://localhost/file").unwrap())
            .retries(3)
            .retry_delay(Duration::from_secs(10));
        assert_eq!(reader.retry_delay, Duration::from_secs(10));
        assert_eq!(reader.retries, 3);
        let request = reader.request.build().unwrap();
        assert_eq!(request.url(), &Url::parse("http://localhost/file").unwrap());
        assert_eq!(request.method(), reqwest::Method::GET);
    }
    #[tokio::test]
    async fn read_single() {
        let expect = vec![1, 2, 3, 4, 5, 6];
        let (listener, port) = new_listener();
        let server = new_server(listener, expect.clone());
        let mut reader = new_reader(port);
        let read = reader.read_at(0, expect.len());
        tokio::select! {
            _ = server => panic!("server ended"),
            data = read => assert_eq!(data.unwrap(), expect),
        };
    }
    #[tokio::test]
    async fn read_single_offset() {
        let expect = vec![1, 2, 3, 4, 5, 6];
        let (listener, port) = new_listener();
        let server = new_server(listener, expect.clone());
        let mut reader = new_reader(port);
        let read = reader.read_at(1, expect.len() - 1);
        tokio::select! {
            _ = server => panic!("server ended"),
            data = read => assert_eq!(&data.unwrap()[..], &expect[1..]),
        };
    }
    #[tokio::test]
    async fn read_single_zero() {
        let (listener, port) = new_listener();
        let server = new_server(listener, vec![1, 2, 3, 4, 5, 6]);
        let mut reader = new_reader(port);
        let read = reader.read_at(1, 0);
        tokio::select! {
            _ = server => panic!("server ended"),
            data = read => assert_eq!(&data.unwrap()[..], &[]),
        };
    }
    #[tokio::test]
    async fn unexpected_end() {
        let (listener, port) = new_listener();
        let server = new_server(listener, vec![1, 2, 3, 4, 5, 6]);
        let mut reader = new_reader(port);
        let read = reader.read_at(0, 10);
        tokio::select! {
            _ = server => panic!("server ended"),
            data = read => match data.unwrap_err() { ReaderRemoteError::UnexpectedEnd => {} err=> panic!(err) },
        };
    }
    #[tokio::test]
    async fn read_chunks() {
        let expect = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
        ];
        let (listener, port) = new_listener();
        let server = new_server(listener, expect.clone());
        let mut reader = new_reader(port);
        let chunk_sizes = vec![6, 10, 4];
        let stream = reader
            .read_chunks(0, &chunk_sizes[..])
            .map(|v| v.expect("item"));
        tokio::select! {
            _ = server => panic!("server ended"),
            chunks = stream.collect::<Vec<Bytes>>() => assert_eq!(chunks, vec![
                Bytes::from(vec![1, 2, 3, 4, 5, 6]), Bytes::from(vec![7, 8, 9, 10, 11, 12, 13, 14, 15, 16]), Bytes::from(vec![17, 18, 19, 20]),
            ]),
        };
    }
    #[tokio::test]
    async fn read_chunks_with_offset() {
        let expect = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
        ];
        let (listener, port) = new_listener();
        let server = new_server(listener, expect.clone());
        let mut reader = new_reader(port);
        let chunk_sizes = vec![4, 10, 4];
        let stream = reader
            .read_chunks(2, &chunk_sizes[..])
            .map(|v| v.expect("item"));
        tokio::select! {
            _ = server => panic!("server ended"),
            chunks = stream.collect::<Vec<Bytes>>() => assert_eq!(chunks, vec![
                Bytes::from(vec![3, 4, 5, 6]), Bytes::from(vec![7, 8, 9, 10, 11, 12, 13, 14, 15, 16]), Bytes::from(vec![17, 18, 19, 20]),
            ]),
        };
    }
    #[tokio::test]
    async fn connection_timeout() {
        let (listener, port) = new_listener();
        let _server = new_server(listener, vec![]);
        let mut reader = ReaderRemote::from_request(
            reqwest::Client::new()
                .get(Url::parse(&format!("http://127.0.0.1:{}", port)).unwrap())
                .timeout(Duration::from_millis(5)),
        );
        let chunk_sizes = vec![1];
        match reader.read_chunks(0, &chunk_sizes[..]).next().await {
            Some(Err(ReaderRemoteError::Http(reqwest::Error { .. }))) => {}
            _ => panic!("unexpected result"),
        };
    }
}
