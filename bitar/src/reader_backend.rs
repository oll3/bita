use async_stream::try_stream;
use futures_core::stream::Stream;
use futures_util::{pin_mut, StreamExt, TryFutureExt};
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
pub enum Builder {
    Remote {
        url: Url,
        retries: u32,
        retry_delay: Option<Duration>,
        receive_timeout: Option<Duration>,
    },
    Local(PathBuf),
}

impl Builder {
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
    pub fn new_local(path: &Path) -> Self {
        Self::Local(path.to_path_buf())
    }
    pub fn source(&self) -> String {
        match self {
            Builder::Remote { url, .. } => url.to_string(),
            Builder::Local(p) => p.display().to_string(),
        }
    }
    pub async fn read_at(&mut self, offset: u64, size: usize) -> Result<Vec<u8>, Error> {
        let request = match self {
            Self::Remote {
                url,
                retries,
                retry_delay,
                receive_timeout,
            } => ReaderBackend::Remote {
                request: http_range_request::Builder::new(url.clone(), offset, size as u64)
                    .receive_timeout(*receive_timeout)
                    .retry(*retries, *retry_delay),
            },
            Self::Local(path) => ReaderBackend::Local {
                start_offset: offset,
                file_path: path.clone(),
            },
        };
        request.single(size).await
    }
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
                ReaderBackend::Remote {
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
            Self::Local(path) => ReaderBackend::Local {
                start_offset,
                file_path: path.clone(),
            }
            .chunks(chunk_sizes.to_vec().into()),
        }
    }
}

enum ReaderBackend {
    Remote {
        request: http_range_request::Builder,
    },
    Local {
        start_offset: u64,
        file_path: PathBuf,
    },
}

impl ReaderBackend {
    pub async fn single(self, size: usize) -> Result<Vec<u8>, Error> {
        match self {
            Self::Remote { request } => {
                let res = request.single().await?;
                if res.len() >= size {
                    // Truncate the response if bigger than requested size
                    Ok(res[..size].to_vec())
                } else if res.len() < size {
                    Err("unexpected end of response".into())
                } else {
                    Ok(res[..].to_vec())
                }
            }
            Self::Local {
                file_path,
                start_offset,
            } => {
                let mut file = File::open(file_path)
                    .map_err(|err| ("failed to open file", err))
                    .await?;
                file.seek(SeekFrom::Start(start_offset))
                    .await
                    .map_err(|err| ("failed to seek file", err))?;
                let mut res = vec![0; size];
                match file.read_exact(&mut res).await {
                    Ok(_) => Ok(res),
                    Err(err) => Err(("failed to read from file", err).into()),
                }
            }
        }
    }
    pub fn chunks(
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
                            if let Some(result) = stream.next().await {
                                let tmp_buf = result?;
                                chunk_buf.extend_from_slice(&tmp_buf[..]);
                            }
                        }
                    }
                }
                Self::Local {
                    file_path,
                    start_offset,
                } => {
                    let mut file = File::open(file_path)
                        .map_err(|err| ("failed to open file", err))
                        .await?;
                    file.seek(SeekFrom::Start(start_offset))
                        .await
                        .map_err(|err| ("failed to seek file", err))?;
                    while let Some(chunk_size) = chunk_sizes.pop_front() {
                        let mut chunk_buf = vec![0; chunk_size];
                        file.read_exact(&mut chunk_buf).await.map_err(|err| ("failed to read from file", err))?;
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
    use futures_util::stream::StreamExt;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn local_read_single_small() {
        let mut file = NamedTempFile::new().unwrap();
        let expected: Vec<u8> = b"hello file".to_vec();
        file.write_all(&expected).unwrap();
        let builder = Builder::new_local(file.path());
        pin_mut!(builder);
        let reader = builder.read_at(0, expected.len());
        let read_back = reader.await.unwrap();
        assert_eq!(read_back, expected);
    }
    #[tokio::test]
    async fn local_read_single_big() {
        let mut file = NamedTempFile::new().unwrap();
        let expected: Vec<u8> = (0..10 * 1024 * 1024).map(|v| v as u8).collect();
        file.write_all(&expected).unwrap();
        let builder = Builder::new_local(file.path());
        pin_mut!(builder);
        let reader = builder.read_at(0, expected.len());
        let read_back = reader.await.unwrap();
        assert_eq!(read_back, expected);
    }
    #[tokio::test]
    async fn local_read_chunks() {
        let mut file = NamedTempFile::new().unwrap();
        let expected: Vec<u8> = (0..10 * 1024 * 1024).map(|v| v as u8).collect();
        let chunk_sizes: Vec<usize> = vec![10, 20, 30, 100, 200, 400, 8 * 1024 * 1024];
        file.write_all(&expected).unwrap();
        let reader = Builder::new_local(file.path()).read_chunks(0, &chunk_sizes);
        {
            pin_mut!(reader);
            let mut chunk_offset = 0;
            let mut chunk_count = 0;
            while let Some(chunk) = reader.next().await {
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
