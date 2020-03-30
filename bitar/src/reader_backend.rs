use async_stream::try_stream;
use futures_core::stream::Stream;
use futures_util::{pin_mut, StreamExt};
use reqwest::Url;
use std::collections::VecDeque;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::fs::File;
use tokio::prelude::*;

use crate::error::Error;
use crate::http_range_request;

#[derive(Clone, Debug)]
pub enum ReaderBackend {
    Remote {
        url: Url,
        retries: u32,
        retry_delay: Option<Duration>,
        receive_timeout: Option<Duration>,
    },
    Local(PathBuf),
}

impl ReaderBackend {
    /// Returns a backend for reading from a remote archive
    pub fn new_remote(
        url: Url,
        retries: u32,
        retry_delay: Option<Duration>,
        receive_timeout: Option<Duration>,
    ) -> Self {
        Self::Remote {
            url,
            retries,
            retry_delay,
            receive_timeout,
        }
    }

    /// Returns a backend for reading from a local file
    pub fn new_local(path: &Path) -> Self {
        Self::Local(path.to_path_buf())
    }

    /// Return a printable string of the source
    pub fn source(&self) -> String {
        match self {
            Self::Remote { url, .. } => url.to_string(),
            Self::Local(p) => p.display().to_string(),
        }
    }

    /// Read a single chunk at offset
    pub async fn read_at(&mut self, offset: u64, size: usize) -> Result<Vec<u8>, Error> {
        let request = match self {
            Self::Remote {
                url,
                retries,
                retry_delay,
                receive_timeout,
            } => Reader::Remote {
                request: http_range_request::Builder::new(url.clone(), offset, size as u64)
                    .receive_timeout(*receive_timeout)
                    .retry(*retries, *retry_delay),
            },
            Self::Local(path) => Reader::Local {
                start_offset: offset,
                file_path: path.clone(),
            },
        };
        request.single(size).await
    }

    /// Read a stream of sequential chunks
    ///
    /// Using this we can avoid making multiple request when chunks are in sequence.
    /// The first chunk returned will have the size of `chunk_size[0]` and the nth
    /// chunk `chunk_size[n]`.
    pub fn read_chunks(
        &self,
        start_offset: u64,
        chunk_sizes: &[usize],
    ) -> impl Stream<Item = Result<Vec<u8>, Error>> {
        match self {
            Self::Remote {
                url,
                retries,
                retry_delay,
                receive_timeout,
            } => {
                let total_size: u64 = chunk_sizes.iter().map(|v| *v as u64).sum();
                Reader::Remote {
                    request: http_range_request::Builder::new(
                        url.clone(),
                        start_offset,
                        total_size,
                    )
                    .receive_timeout(*receive_timeout)
                    .retry(*retries, *retry_delay),
                }
                .chunks(chunk_sizes.to_vec().into())
            }
            Self::Local(path) => Reader::Local {
                start_offset,
                file_path: path.clone(),
            }
            .chunks(chunk_sizes.to_vec().into()),
        }
    }
}

enum Reader {
    Remote {
        request: http_range_request::Builder,
    },
    Local {
        start_offset: u64,
        file_path: PathBuf,
    },
}

impl Reader {
    async fn single(self, size: usize) -> Result<Vec<u8>, Error> {
        match self {
            Self::Remote { request } => {
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
            Self::Local {
                file_path,
                start_offset,
            } => {
                let mut file = File::open(file_path).await?;
                file.seek(SeekFrom::Start(start_offset)).await?;
                let mut res = vec![0; size];
                file.read_exact(&mut res).await?;
                Ok(res)
            }
        }
    }
    fn chunks(
        self,
        mut chunk_sizes: VecDeque<usize>,
    ) -> impl Stream<Item = Result<Vec<u8>, Error>> {
        try_stream! {
            match self {
                Self::Remote {request} => {
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
                Self::Local {
                    file_path,
                    start_offset,
                } => {
                    let mut file = File::open(file_path).await?;
                    file.seek(SeekFrom::Start(start_offset)).await?;
                    while let Some(chunk_size) = chunk_sizes.pop_front() {
                        let mut chunk_buf = vec![0; chunk_size];
                        file.read_exact(&mut chunk_buf).await?;
                        yield chunk_buf;
                    }
                }
            }
        }
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
        let backend = ReaderBackend::new_local(file.path());
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
        let backend = ReaderBackend::new_local(file.path());
        pin_mut!(backend);
        let reader = backend.read_at(0, expected.len());
        let read_back = reader.await.unwrap();
        assert_eq!(read_back, expected);
    }
    #[tokio::test]
    async fn local_read_chunks() {
        let mut file = NamedTempFile::new().unwrap();
        let expected: Vec<u8> = (0..10 * 1024 * 1024).map(|v| v as u8).collect();
        let chunk_sizes: Vec<usize> = vec![10, 20, 30, 100, 200, 400, 8 * 1024 * 1024];
        file.write_all(&expected).unwrap();
        let backend = ReaderBackend::new_local(file.path()).read_chunks(0, &chunk_sizes);
        {
            pin_mut!(backend);
            let mut chunk_offset = 0;
            let mut chunk_count = 0;
            while let Some(chunk) = backend.next().await {
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
