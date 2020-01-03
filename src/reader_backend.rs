use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use futures_core::stream::Stream;
use hyper::Uri;
use std::collections::VecDeque;
use std::io::Seek;
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
        uri: Uri,
        retries: u32,
        retry_delay: Option<Duration>,
        receive_timeout: Option<Duration>,
    },
    Local(PathBuf),
}

impl Builder {
    pub fn new_remote(
        uri: Uri,
        retries: u32,
        retry_delay: Option<Duration>,
        receive_timeout: Option<Duration>,
    ) -> Self {
        Self::Remote {
            uri,
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
            Builder::Remote { uri, .. } => uri.to_string(),
            Builder::Local(p) => p.display().to_string(),
        }
    }
    pub fn read_at(&mut self, offset: u64, size: usize) -> Result<Single, Error> {
        match self {
            Self::Remote {
                uri,
                retries,
                retry_delay,
                receive_timeout,
            } => Ok(Single {
                size,
                backend: ReaderBackend::Remote {
                    buffer: Vec::new(),
                    request: http_range_request::Builder::new(uri.clone(), offset, size as u64)
                        .receive_timeout(*receive_timeout)
                        .retry(*retries, *retry_delay)
                        .build(),
                },
            }),
            Self::Local(path) => {
                let mut file =
                    std::fs::File::open(path).map_err(|err| ("failed to open file", err))?;
                file.seek(SeekFrom::Start(offset))
                    .map_err(|err| ("failed to seek file", err))?;
                Ok(Single {
                    size,
                    backend: ReaderBackend::Local {
                        buffer: Vec::new(),
                        read_count: 0,
                        file: File::from_std(file),
                    },
                })
            }
        }
    }
    pub fn read_chunks(&self, start_offset: u64, chunk_sizes: &[usize]) -> Result<Chunks, Error> {
        match self {
            Self::Remote {
                uri,
                retries,
                retry_delay,
                receive_timeout,
            } => {
                let total_size: u64 = chunk_sizes.iter().map(|v| *v as u64).sum();
                Ok(Chunks {
                    chunk_sizes: chunk_sizes.to_vec().into(),
                    backend: ReaderBackend::Remote {
                        buffer: Vec::new(),
                        request: http_range_request::Builder::new(
                            uri.clone(),
                            start_offset,
                            total_size,
                        )
                        .receive_timeout(*receive_timeout)
                        .retry(*retries, *retry_delay)
                        .build(),
                    },
                })
            }
            Self::Local(path) => {
                let mut file = std::fs::File::open(path).map_err(|e| ("failed to open file", e))?;
                file.seek(SeekFrom::Start(start_offset))
                    .map_err(|e| ("failed to seek file", e))?;
                Ok(Chunks {
                    chunk_sizes: chunk_sizes.to_vec().into(),
                    backend: ReaderBackend::Local {
                        read_count: 0,
                        buffer: Vec::new(),
                        file: File::from_std(file),
                    },
                })
            }
        }
    }
}

enum ReaderBackend {
    Remote {
        buffer: Vec<u8>,
        request: http_range_request::Request,
    },
    Local {
        read_count: usize,
        buffer: Vec<u8>,
        file: File,
    },
}

pub struct Single {
    size: usize,
    backend: ReaderBackend,
}

impl Future for Single {
    type Output = Result<Vec<u8>, Error>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let inner = self.get_mut();
        let backend = &mut inner.backend;
        let size = inner.size;
        loop {
            match backend {
                ReaderBackend::Remote { request, buffer } => {
                    match Pin::new(request).poll_next(cx) {
                        Poll::Ready(Some(Ok(chunk))) => {
                            buffer.extend(&chunk[..]);
                            if buffer.len() >= size {
                                buffer.truncate(size);
                                return Poll::Ready(Ok(buffer.clone()));
                            }
                        }
                        Poll::Ready(Some(Err(err))) => return Poll::Ready(Err(err)),
                        Poll::Ready(None) => {
                            return Poll::Ready(Err("unexpected end of response".into()))
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                ReaderBackend::Local {
                    buffer,
                    file,
                    read_count,
                } => {
                    buffer.resize(size, 0);
                    match Pin::new(file).poll_read(cx, &mut buffer[*read_count..]) {
                        Poll::Ready(Ok(n)) => {
                            *read_count += n;
                            if n == 0 {
                                return Poll::Ready(Err("unexpected end of file".into()));
                            } else if *read_count >= size {
                                return Poll::Ready(Ok(buffer.clone()));
                            }
                        }
                        Poll::Ready(Err(err)) => {
                            return Poll::Ready(Err(("failed to read from file", err).into()));
                        }
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                    };
                }
            }
        }
    }
}

pub struct Chunks {
    chunk_sizes: VecDeque<usize>,
    backend: ReaderBackend,
}

impl Stream for Chunks {
    type Item = Result<Vec<u8>, Error>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let inner = self.get_mut();
        let backend = &mut inner.backend;

        // Poll the backend until it says pending or we're done

        loop {
            if inner.chunk_sizes.is_empty() {
                // We're done
                return Poll::Ready(None);
            }

            let chunk_size = *inner.chunk_sizes.front().unwrap();
            match backend {
                ReaderBackend::Remote { request, buffer } => {
                    // Read from remote
                    if buffer.len() >= chunk_size {
                        // Got a full chunk
                        let chunk = buffer.drain(0..chunk_size).collect();
                        inner.chunk_sizes.pop_front();
                        return Poll::Ready(Some(Ok(chunk)));
                    }
                    match Pin::new(request).poll_next(cx) {
                        Poll::Ready(Some(Ok(chunk))) => {
                            buffer.extend(&chunk[..]);
                        }
                        Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                        Poll::Ready(None) => {
                            if inner.chunk_sizes.is_empty() {
                                return Poll::Ready(None);
                            } else {
                                return Poll::Ready(Some(Err(format!(
                                    "unexpected end (missing {} chunks)",
                                    inner.chunk_sizes.len()
                                )
                                .into())));
                            }
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                ReaderBackend::Local {
                    file,
                    buffer,
                    read_count,
                } => {
                    if *read_count >= chunk_size {
                        // Got a full chunk
                        inner.chunk_sizes.pop_front();
                        *read_count = 0;
                        return Poll::Ready(Some(Ok(buffer.clone())));
                    }

                    buffer.resize(chunk_size, 0);

                    match Pin::new(file).poll_read(cx, &mut buffer[*read_count..]) {
                        Poll::Ready(Ok(n)) => {
                            *read_count += n;
                            if n == 0 {
                                return Poll::Ready(Some(Err(format!(
                                    "unexpected end of file (missing {} chunks)",
                                    inner.chunk_sizes.len()
                                )
                                .into())));
                            }
                        }
                        Poll::Ready(Err(err)) => {
                            return Poll::Ready(Some(
                                Err(("failed to read from file", err).into()),
                            ));
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
            };
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
        let reader = Builder::new_local(file.path())
            .read_at(0, expected.len())
            .unwrap();
        let read_back = reader.await.unwrap();
        assert_eq!(read_back, expected);
    }
    #[tokio::test]
    async fn local_read_single_big() {
        let mut file = NamedTempFile::new().unwrap();
        let expected: Vec<u8> = (0..10 * 1024 * 1024).map(|v| v as u8).collect();
        file.write_all(&expected).unwrap();
        let reader = Builder::new_local(file.path())
            .read_at(0, expected.len())
            .unwrap();
        let read_back = reader.await.unwrap();
        assert_eq!(read_back, expected);
    }
    #[tokio::test]
    async fn local_read_chunks() {
        let mut file = NamedTempFile::new().unwrap();
        let expected: Vec<u8> = (0..10 * 1024 * 1024).map(|v| v as u8).collect();
        let chunk_sizes: Vec<usize> = vec![10, 20, 30, 100, 200, 400, 8 * 1024 * 1024];
        file.write_all(&expected).unwrap();
        let mut reader = Builder::new_local(file.path())
            .read_chunks(0, &chunk_sizes)
            .unwrap();
        {
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
