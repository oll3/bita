use bytes::{Bytes, BytesMut};
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use futures::stream::Stream;
use hyper::{Body, Uri};
use std::collections::VecDeque;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::fs::File;
use tokio::prelude::*;

use crate::error::Error;
use crate::http_range_request::HttpRangeRequest;

pub enum Builder {
    Remote {
        uri: Uri,
        retries: u32,
        rx_timeout: Duration,
    },
    Local(PathBuf),
}

impl Builder {
    pub fn new_remote(uri: Uri, retries: u32, rx_timeout: Duration) -> Self {
        Self::Remote {
            uri,
            retries,
            rx_timeout,
        }
    }
    pub fn new_local(path: &Path) -> Self {
        Self::Local(path.to_path_buf())
    }
    pub fn read_at(&self, offset: u64, size: usize) -> Single {
        match self {
            Self::Remote {
                uri,
                retries,
                rx_timeout,
            } => Single {
                offset,
                size,
                buffer: BytesMut::with_capacity(size),
                backend: ReaderBackend::Remote {
                    uri: uri.clone(),
                    retries: retries.clone(),
                    rx_timeout: rx_timeout.clone(),
                    body: None,
                },
            },
            Self::Local(path) => Single {
                offset,
                size,
                buffer: BytesMut::with_capacity(size),
                backend: ReaderBackend::Local(path.clone()),
            },
        }
    }
    /*pub async fn read_chunks(
        &self,
        start_offset: u64,
        chunk_sizes: &[usize],
    ) -> Result<Chunks, Error> {
        match self {
            Self::Remote(uri) => {
                let total_size: u64 = chunk_sizes.iter().map(|v| *v as u64).sum();
                println!("Fetch {} in total", total_size);
                let response = range_request(uri, start_offset, total_size).await?;
                Ok(Chunks {
                    chunk_sizes: chunk_sizes.to_vec().into(),
                    buffer: BytesMut::new(),
                    backend: ReaderBackend::Remote(response.into_body()),
                })
            }
            Self::Local(path) => {
                let mut file = tokio::fs::OpenOptions::new()
                    .read(true)
                    .open(path)
                    .await
                    .map_err(|e| ("failed to open file", e))?;
                file.seek(SeekFrom::Start(start_offset))
                    .await
                    .map_err(|e| ("failed to seek file", e))?;
                Ok(Chunks {
                    chunk_sizes: chunk_sizes.to_vec().into(),
                    buffer: BytesMut::new(),
                    backend: ReaderBackend::Local(file),
                })
            }
        }
    }*/
}

enum ReaderBackend {
    Remote {
        uri: Uri,
        body: Option<Body>,
        rx_timeout: Duration,
        retries: u32,
    },
    Local(PathBuf),
}

pub struct Single {
    offset: u64,
    size: usize,
    buffer: BytesMut,
    backend: ReaderBackend,
}

impl Future for Single {
    type Output = Result<Bytes, Error>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let inner = &mut *self;
        let buffer = &mut inner.buffer;
        let backend = &mut inner.backend;
        let size = inner.size;
        /*  match backend {
            ReaderBackend::Remote(ref mut body) => match Pin::new(body).poll_next(cx) {
                Poll::Ready(Some(Ok(chunk))) => {
                    buffer.extend(&chunk.into_bytes()[..]);
                    if buffer.len() >= size {
                        buffer.truncate(size);
                        Poll::Ready(Ok(buffer.clone().freeze()))
                    } else {
                        Poll::Pending
                    }
                }
                Poll::Ready(Some(Err(e))) => {
                    Poll::Ready(Err(("Failed to read from remote", e).into()))
                }
                Poll::Ready(None) => Poll::Ready(Err("Unexpected end of response".into())),
                Poll::Pending => Poll::Pending,
            },
            ReaderBackend::Local(ref mut file) => unimplemented!(),
            /* match Pin::new(file).poll_read_buf(cx, buffer) {
                Poll::Ready(Ok(_n)) => {
                    if buffer.len() >= size {
                        buffer.truncate(size);
                        Poll::Ready(Ok(buffer.clone().freeze()))
                    } else {
                        Poll::Pending
                    }
                }
                Poll::Ready(Err(e)) => Poll::Ready(Err(("Failed to read from remote", e).into())),
                Poll::Pending => Poll::Pending,
            },*/
        }*/
        Poll::Pending
    }
}

/*pub struct Chunks {
    chunk_sizes: VecDeque<usize>,
    buffer: BytesMut,
    backend: ReaderBackend,
}

impl Stream for Chunks {
    type Item = Result<Bytes, Error>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if self.chunk_sizes.is_empty() {
            // We're done
            return Poll::Ready(None);
        }
        let chunk_size = *self.chunk_sizes.front().unwrap();
        if self.buffer.len() >= chunk_size {
            // Got a full chunk already
            let chunk = self.buffer.split_to(chunk_size).freeze();
            self.chunk_sizes.pop_front();
            return Poll::Ready(Some(Ok(chunk)));
        }
        loop {
            match self.backend {
                // Poll until pending or got full chunk
                ReaderBackend::Remote(ref mut body) => match Pin::new(body).poll_next(cx) {
                    Poll::Ready(Some(Ok(chunk))) => {
                        self.buffer.extend(&chunk.into_bytes()[..]);
                        if self.buffer.len() >= chunk_size {
                            let chunk = self.buffer.split_to(chunk_size).freeze();
                            self.chunk_sizes.pop_front();
                            return Poll::Ready(Some(Ok(chunk)));
                        }
                    }
                    Poll::Ready(Some(Err(e))) => {
                        return Poll::Ready(Some(Err(("Failed to read from remote", e).into())))
                    }
                    Poll::Ready(None) => {
                        if self.chunk_sizes.is_empty() {
                            return Poll::Ready(None);
                        } else {
                            return Poll::Ready(Some(Err(format!(
                                "Unexpected end (missing {} chunks)",
                                self.chunk_sizes.len()
                            )
                            .into())));
                        }
                    }
                    Poll::Pending => return Poll::Pending,
                },
                ReaderBackend::Local(ref mut file) => {
                    //
                    return Poll::Pending;
                }
            };
        }
    }
}
*/
