use async_stream::try_stream;
use async_trait::async_trait;
use core::pin::Pin;
use futures_core::stream::Stream;
use futures_util::{pin_mut, StreamExt};
use reqwest::{RequestBuilder, Url};
use std::collections::VecDeque;
use std::time::Duration;

use crate::error::Error;
use crate::http_range_request;
use crate::reader::Reader;

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

    fn read_chunks<'a>(
        &'a mut self,
        start_offset: u64,
        mut chunk_sizes: VecDeque<usize>,
    ) -> impl Stream<Item = Result<Vec<u8>, Error>> + 'a {
        try_stream! {
            let total_size: u64 = chunk_sizes.iter().map(|v| *v as u64).sum();
            let request = http_range_request::Builder::new(
                    self.request.try_clone().ok_or(Error::RequestNotClonable)?,
                    start_offset,
                    total_size,
                )
                .retry(self.retries, self.retry_delay);

            let mut stream = request.stream();
            pin_mut!(stream);
            let mut chunk_buf: Vec<u8> = Vec::new();
            while let Some(chunk_size) = chunk_sizes.pop_front() {
                loop {
                    if chunk_buf.len() >= chunk_size {
                        yield chunk_buf.drain(..chunk_size).collect();
                        break;
                    }
                    match stream.next().await {
                        Some(Ok(tmp_buf)) => chunk_buf.extend_from_slice(&tmp_buf[..]),
                        Some(Err(err)) => Err(err)?,
                        None => {}
                    }
                }
            }
        }
    }
}

#[async_trait]
impl Reader for ReaderRemote {
    async fn read_at(&mut self, offset: u64, size: usize) -> Result<Vec<u8>, Error> {
        let request = http_range_request::Builder::new(
            self.request.try_clone().ok_or(Error::RequestNotClonable)?,
            offset,
            size as u64,
        )
        .retry(self.retries, self.retry_delay);

        let res = request.single().await?;
        if res.len() >= size {
            // Truncate the response if bigger than requested size
            Ok(res[..size].to_vec())
        } else if res.len() < size {
            Err(Error::UnexpectedEnd)
        } else {
            Ok(res[..].to_vec())
        }
    }
    fn read_chunks<'a>(
        &'a mut self,
        start_offset: u64,
        chunk_sizes: VecDeque<usize>,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<u8>, Error>> + Send + 'a>> {
        Box::pin(self.read_chunks(start_offset, chunk_sizes))
    }
}
