use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use core::pin::Pin;
use core::task::{Context, Poll};
use futures_util::{ready, stream::Stream, StreamExt};
use reqwest::{RequestBuilder, Url};
use std::{fmt, time::Duration};

use super::http_range_request::HttpRangeRequest;
use crate::archive_reader::{ArchiveReader, ChunkOffset};

/// Read a http/https hosted archive.
pub struct HttpReader {
    request_builder: RequestBuilder,
    retry_count: u32,
    retry_delay: Duration,
}

impl HttpReader {
    /// Create a remote archive reader using RequestBuilder for the http request.
    pub fn from_request(request_builder: RequestBuilder) -> Self {
        Self {
            request_builder,
            retry_count: 0,
            retry_delay: Duration::from_secs(0),
        }
    }

    /// Create a remote archive reader using an URL and default parameters for the request.
    pub fn from_url(url: Url) -> Self {
        Self::from_request(reqwest::Client::new().get(url))
    }

    /// Set number of times to retry on failure
    ///
    /// The reader will try to reconnect and continue download from where the failure occurred.
    /// Any progress made so far should not be lost.
    #[must_use]
    pub fn retries(mut self, retry_count: u32) -> Self {
        self.retry_count = retry_count;
        self
    }

    /// Set a delay between attempts to reconnect to the remote server.
    ///
    /// On failure the reader will wait for the given time before trying to reconnect.
    #[must_use]
    pub fn retry_delay(mut self, retry_delay: Duration) -> Self {
        self.retry_delay = retry_delay;
        self
    }

    fn read_chunk_stream(
        &mut self,
        chunks: Vec<ChunkOffset>,
    ) -> impl Stream<Item = Result<Bytes, HttpReaderError>> + '_ {
        ChunkReader {
            request_builder: &self.request_builder,
            chunk_buf: BytesMut::new(),
            chunk_index: 0,
            num_adjacent_reads: 0,
            chunks,
            retry_count: self.retry_count,
            retry_delay: self.retry_delay,
            request: None,
        }
    }
}

struct ChunkReader<'a> {
    request_builder: &'a RequestBuilder,
    chunk_buf: BytesMut,
    chunks: Vec<ChunkOffset>,
    chunk_index: usize,
    num_adjacent_reads: usize,
    retry_count: u32,
    retry_delay: Duration,
    request: Option<HttpRangeRequest>,
}

impl ChunkReader<'_>
where
    Self: Unpin,
{
    fn poll_read(&mut self, cx: &mut Context) -> Poll<Option<Result<Bytes, HttpReaderError>>> {
        loop {
            let chunks = &self.chunks[self.chunk_index..];
            if chunks.is_empty() {
                // No more chunks to fetch.
                return Poll::Ready(None);
            }
            let next = &chunks[0];
            if self.chunk_buf.len() >= next.size {
                self.chunk_index += 1;
                let chunk = self.chunk_buf.split_to(next.size).freeze();
                self.num_adjacent_reads -= 1;
                if self.num_adjacent_reads == 0 {
                    // Time to make another request.
                    self.request = None;
                }
                return Poll::Ready(Some(Ok(chunk)));
            }
            if self.request.is_none() {
                // Create a new range request.
                let request_builder = self
                    .request_builder
                    .try_clone()
                    .ok_or(HttpReaderError::RequestNotClonable)?;

                self.num_adjacent_reads = Self::adjacent_reads(chunks);
                let last_adjacent = &chunks[self.num_adjacent_reads - 1];
                let total_size = last_adjacent.end() - next.offset;
                self.chunk_buf.clear();
                self.request = Some(
                    HttpRangeRequest::new(request_builder, next.offset, total_size)
                        .retry(self.retry_count, self.retry_delay),
                );
            };

            // Poll for chunks.
            let request = self.request.as_mut().unwrap();
            match ready!(request.poll_next_unpin(cx)) {
                Some(Ok(chunk)) => {
                    self.chunk_buf.extend(chunk);
                }
                Some(Err(err)) => return Poll::Ready(Some(Err(err))),
                None => return Poll::Ready(Some(Err(HttpReaderError::UnexpectedEnd))),
            }
        }
    }

    fn adjacent_reads(chunks: &[ChunkOffset]) -> usize {
        chunks
            .windows(2)
            .take_while(|p| (p[0].offset + p[0].size as u64) == p[1].offset)
            .count()
            + 1
    }
}

impl Stream for ChunkReader<'_> {
    type Item = Result<Bytes, HttpReaderError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.poll_read(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let chunks_left = self.chunks.len() - self.chunk_index;
        (chunks_left, Some(chunks_left))
    }
}

#[async_trait]
impl ArchiveReader for HttpReader {
    type Error = HttpReaderError;

    async fn read_at(&mut self, offset: u64, size: usize) -> Result<Bytes, HttpReaderError> {
        let request = HttpRangeRequest::new(
            self.request_builder
                .try_clone()
                .ok_or(HttpReaderError::RequestNotClonable)?,
            offset,
            size as u64,
        )
        .retry(self.retry_count, self.retry_delay);

        let mut res = request.single().await?;
        if res.len() >= size {
            // Truncate the response if bigger than requested size
            Ok(res.split_to(size))
        } else if res.len() < size {
            Err(HttpReaderError::UnexpectedEnd)
        } else {
            Ok(res)
        }
    }

    fn read_chunks<'a>(
        &'a mut self,
        chunks: Vec<ChunkOffset>,
    ) -> Pin<Box<dyn Stream<Item = Result<Bytes, HttpReaderError>> + Send + 'a>> {
        Box::pin(self.read_chunk_stream(chunks))
    }
}

#[derive(Debug)]
pub enum HttpReaderError {
    UnexpectedEnd,
    RequestNotClonable,
    Http(reqwest::Error),
}

impl std::error::Error for HttpReaderError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            HttpReaderError::Http(err) => Some(err),
            HttpReaderError::UnexpectedEnd | HttpReaderError::RequestNotClonable => None,
        }
    }
}

impl fmt::Display for HttpReaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEnd => write!(f, "unexpected end"),
            Self::RequestNotClonable => write!(f, "request is not clonable"),
            Self::Http(_) => write!(f, "http error"),
        }
    }
}

impl From<reqwest::Error> for HttpReaderError {
    fn from(e: reqwest::Error) -> Self {
        Self::Http(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::Full;
    use hyper::{server::conn::http1, service::service_fn};
    use tokio::net::TcpListener;

    async fn new_server(listener: TcpListener, data: Vec<u8>) {
        let (stream, _) = listener.accept().await.unwrap();
        let io = hyper_util::rt::TokioIo::new(stream);
        http1::Builder::new()
            .serve_connection(
                io,
                service_fn(move |req| {
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
                        Ok::<_, hyper::Error>(hyper::Response::new(Full::new(
                            hyper::body::Bytes::from(data),
                        )))
                    }
                }),
            )
            .await
            .unwrap();
    }

    fn new_reader(port: u16) -> HttpReader {
        HttpReader::from_url(Url::parse(&format!("http://127.0.0.1:{}", port)).unwrap())
    }

    async fn new_listener() -> (TcpListener, u16) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        (listener, port)
    }

    #[test]
    fn one_adjacent_reads() {
        let chunks = [ChunkOffset::new(0, 1), ChunkOffset::new(10, 1)];
        assert_eq!(ChunkReader::adjacent_reads(&chunks[..]), 1);
    }

    #[test]
    fn two_adjacent_reads() {
        let chunks = [
            ChunkOffset::new(0, 1),
            ChunkOffset::new(1, 3),
            ChunkOffset::new(10, 3),
        ];
        assert_eq!(ChunkReader::adjacent_reads(&chunks[..]), 2);
    }

    #[test]
    fn multiple_adjacent_reads() {
        let chunks = [
            ChunkOffset::new(0, 1),
            ChunkOffset::new(1, 3),
            ChunkOffset::new(4, 3),
            ChunkOffset::new(7, 3),
            ChunkOffset::new(50, 3),
        ];
        assert_eq!(ChunkReader::adjacent_reads(&chunks[..]), 4);
    }

    #[test]
    fn builder() {
        let reader = HttpReader::from_url(Url::parse("http://localhost/file").unwrap())
            .retries(3)
            .retry_delay(Duration::from_secs(10));
        assert_eq!(reader.retry_delay, Duration::from_secs(10));
        assert_eq!(reader.retry_count, 3);
        let request = reader.request_builder.build().unwrap();
        assert_eq!(request.url(), &Url::parse("http://localhost/file").unwrap());
        assert_eq!(request.method(), reqwest::Method::GET);
    }

    #[tokio::test]
    async fn read_single() {
        let expect = vec![1, 2, 3, 4, 5, 6];
        let (listener, port) = new_listener().await;
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
        let (listener, port) = new_listener().await;
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
        let (listener, port) = new_listener().await;
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
        let (listener, port) = new_listener().await;
        let server = new_server(listener, vec![1, 2, 3, 4, 5, 6]);
        let mut reader = new_reader(port);
        let read = reader.read_at(0, 10);
        tokio::select! {
            _ = server => panic!("server ended"),
            data = read => match data.unwrap_err() { HttpReaderError::UnexpectedEnd => {} err=> panic!("{}", err) },
        };
    }

    #[tokio::test]
    async fn read_chunks() {
        let expect = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
        ];
        let (listener, port) = new_listener().await;
        let server = new_server(listener, expect.clone());
        let mut reader = new_reader(port);
        let chunks = vec![
            ChunkOffset { offset: 0, size: 6 },
            ChunkOffset {
                offset: 6,
                size: 10,
            },
            ChunkOffset {
                offset: 16,
                size: 4,
            },
        ];
        let stream = reader.read_chunks(chunks).map(|v| v.expect("item"));
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
        let (listener, port) = new_listener().await;
        let server = new_server(listener, expect.clone());
        let mut reader = new_reader(port);
        let chunks = vec![
            ChunkOffset { offset: 2, size: 4 },
            ChunkOffset {
                offset: 6,
                size: 10,
            },
            ChunkOffset {
                offset: 16,
                size: 4,
            },
        ];
        let stream = reader.read_chunks(chunks).map(|v| v.expect("item"));
        tokio::select! {
            _ = server => panic!("server ended"),
            chunks = stream.collect::<Vec<Bytes>>() => assert_eq!(chunks, vec![
                Bytes::from(vec![3, 4, 5, 6]), Bytes::from(vec![7, 8, 9, 10, 11, 12, 13, 14, 15, 16]), Bytes::from(vec![17, 18, 19, 20]),
            ]),
        };
    }

    #[tokio::test]
    async fn connection_timeout() {
        let (listener, port) = new_listener().await;
        let _server = new_server(listener, vec![]);
        let mut reader = HttpReader::from_request(
            reqwest::Client::new()
                .get(Url::parse(&format!("http://127.0.0.1:{}", port)).unwrap())
                .timeout(Duration::from_millis(5)),
        );
        let chunks = vec![ChunkOffset { offset: 0, size: 1 }];
        match reader.read_chunks(chunks).next().await {
            Some(Err(HttpReaderError::Http(reqwest::Error { .. }))) => {}
            _ => panic!("unexpected result"),
        };
    }
}
