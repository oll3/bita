use async_stream::try_stream;
use async_trait::async_trait;
use core::pin::Pin;
use futures_core::stream::Stream;
use futures_util::{pin_mut, StreamExt};
use reqwest::Url;
use std::collections::VecDeque;
use std::io::SeekFrom;
use std::time::Duration;
use tokio::fs::File;
use tokio::prelude::*;

use crate::error::Error;
use crate::http_range_request;

#[async_trait]
pub trait ReaderBackend {
    async fn read_at(&mut self, offset: u64, size: usize) -> Result<Vec<u8>, Error>;
    fn read_chunks<'a>(
        &'a mut self,
        start_offset: u64,
        chunk_sizes: VecDeque<usize>,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<u8>, Error>> + Send + 'a>>;
}

pub struct ReaderBackendLocal {
    file: File,
}

impl ReaderBackendLocal {
    pub fn new(file: File) -> Self {
        Self { file }
    }

    fn read_chunks<'a>(
        &'a mut self,
        start_offset: u64,
        mut chunk_sizes: VecDeque<usize>,
    ) -> impl Stream<Item = Result<Vec<u8>, Error>> + 'a {
        try_stream! {
            self.file.seek(SeekFrom::Start(start_offset)).await?;
            while let Some(chunk_size) = chunk_sizes.pop_front() {
                let mut chunk_buf = vec![0; chunk_size];
                self.file.read_exact(&mut chunk_buf).await?;
                yield chunk_buf;
            }
        }
    }
}

#[async_trait]
impl ReaderBackend for ReaderBackendLocal {
    async fn read_at(&mut self, offset: u64, size: usize) -> Result<Vec<u8>, Error> {
        self.file.seek(SeekFrom::Start(offset)).await?;
        let mut res = vec![0; size];
        self.file.read_exact(&mut res).await?;
        Ok(res)
    }
    fn read_chunks<'a>(
        &'a mut self,
        start_offset: u64,
        chunk_sizes: VecDeque<usize>,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<u8>, Error>> + Send + 'a>> {
        Box::pin(self.read_chunks(start_offset, chunk_sizes))
    }
}

pub struct ReaderBackendRemote {
    url: Url,
    retries: u32,
    retry_delay: Option<Duration>,
    receive_timeout: Option<Duration>,
}

impl ReaderBackendRemote {
    pub fn new(
        url: Url,
        retries: u32,
        retry_delay: Option<Duration>,
        receive_timeout: Option<Duration>,
    ) -> Self {
        Self {
            url,
            retries,
            retry_delay,
            receive_timeout,
        }
    }

    fn read_chunks<'a>(
        &'a mut self,
        start_offset: u64,
        mut chunk_sizes: VecDeque<usize>,
    ) -> impl Stream<Item = Result<Vec<u8>, Error>> + 'a {
        try_stream! {
            let total_size: u64 = chunk_sizes.iter().map(|v| *v as u64).sum();
            let request = http_range_request::Builder::new(
                    self.url.clone(),
                    start_offset,
                    total_size,
                )
                .receive_timeout(self.receive_timeout)
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
impl ReaderBackend for ReaderBackendRemote {
    async fn read_at(&mut self, offset: u64, size: usize) -> Result<Vec<u8>, Error> {
        let request = http_range_request::Builder::new(self.url.clone(), offset, size as u64)
            .receive_timeout(self.receive_timeout)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn local_read_single_small() {
        let mut file = NamedTempFile::new().unwrap();
        let expected: Vec<u8> = b"hello file".to_vec();
        file.write_all(&expected).unwrap();
        let backend = ReaderBackendLocal::new(File::open(&file.path()).await.unwrap());
        pin_mut!(backend);
        let reader = backend.read_at(0, expected.len());
        let read_back = reader.await.unwrap();
        assert_eq!(read_back, expected);
    }
    #[tokio::test]
    async fn local_read_single_big() {
        let mut file = NamedTempFile::new().unwrap();
        let expected: Vec<u8> = (0..10 * 1024 * 1024).map(|v| v as u8).collect();
        file.write_all(&expected).unwrap();
        let backend = ReaderBackendLocal::new(File::open(&file.path()).await.unwrap());
        pin_mut!(backend);
        let reader = backend.read_at(0, expected.len());
        let read_back = reader.await.unwrap();
        assert_eq!(read_back, expected);
    }
    #[tokio::test]
    async fn local_read_chunks() {
        let mut file = NamedTempFile::new().unwrap();
        let expected: Vec<u8> = (0..10 * 1024 * 1024).map(|v| v as u8).collect();
        let chunk_sizes: VecDeque<usize> = vec![10, 20, 30, 100, 200, 400, 8 * 1024 * 1024].into();
        file.write_all(&expected).unwrap();
        let mut backend = ReaderBackendLocal::new(File::open(&file.path()).await.unwrap());
        let stream = backend.read_chunks(0, chunk_sizes.clone());
        {
            pin_mut!(stream);
            let mut chunk_offset = 0;
            let mut chunk_count = 0;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.unwrap();
                let chunk_size = chunk_sizes[chunk_count];
                assert_eq!(chunk, &expected[chunk_offset..chunk_offset + chunk_size]);
                chunk_offset += chunk_size;
                chunk_count += 1;
            }
            assert_eq!(chunk_count, chunk_sizes.len());
        }
    }
    // TODO: Add tests for remote backend
}
