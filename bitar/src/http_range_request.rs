use bytes::Bytes;
use core::pin::Pin;
use core::task::{Context, Poll};
use futures_util::{ready, stream::Stream, StreamExt};
use reqwest::RequestBuilder;
use std::future::Future;
use std::time::Duration;

use tokio::time::sleep;

use crate::reader_remote::ReaderRemoteError;

pub(crate) struct Builder {
    request: RequestBuilder,
    state: RequestState,
    size: u64,
    offset: u64,
    retry_delay: Duration,
    retry_count: u32,
}

impl Builder {
    pub fn new(request: RequestBuilder, offset: u64, size: u64) -> Self {
        Self {
            request,
            offset,
            size,
            retry_delay: Duration::from_secs(0),
            retry_count: 0,
            state: RequestState::Init,
        }
    }
    pub fn retry(mut self, retry_count: u32, retry_delay: Duration) -> Self {
        self.retry_delay = retry_delay;
        self.retry_count = retry_count;
        self
    }
    async fn single_fail(
        request: RequestBuilder,
        offset: u64,
        size: u64,
    ) -> Result<Bytes, ReaderRemoteError> {
        let end_offset = offset + size - 1;
        let request = request.header(
            reqwest::header::RANGE,
            format!("bytes={}-{}", offset, end_offset),
        );
        let response = request.send().await?;
        Ok(response.bytes().await?)
    }
    pub async fn single(mut self) -> Result<Bytes, ReaderRemoteError> {
        loop {
            match Self::single_fail(
                self.request
                    .try_clone()
                    .ok_or(ReaderRemoteError::RequestNotClonable)?,
                self.offset,
                self.size,
            )
            .await
            {
                Ok(item) => return Ok(item),
                Err(err) => {
                    if self.retry_count == 0 {
                        return Err(err);
                    } else {
                        log::warn!("request failed (retrying soon): {}", err);
                        self.retry_count -= 1;
                    }
                }
            }
            sleep(self.retry_delay).await;
        }
    }
    fn poll_read_fail(
        &mut self,
        cx: &mut Context,
    ) -> Poll<Option<Result<Bytes, ReaderRemoteError>>> {
        loop {
            match &mut self.state {
                RequestState::Init => {
                    let end_offset = self.offset + self.size - 1;
                    let request = match self.request.try_clone() {
                        Some(request) => request,
                        None => {
                            return Poll::Ready(Some(Err(ReaderRemoteError::RequestNotClonable)))
                        }
                    };
                    let request = request
                        .header(
                            reqwest::header::RANGE,
                            format!("bytes={}-{}", self.offset, end_offset),
                        )
                        .send();
                    self.state = RequestState::Request(Box::new(request));
                }
                RequestState::Request(request) => match ready!(Pin::new(&mut *request).poll(cx)) {
                    Ok(response) => {
                        self.state = RequestState::Stream(Box::new(response.bytes_stream()))
                    }
                    Err(err) => return Poll::Ready(Some(Err(ReaderRemoteError::from(err)))),
                },
                RequestState::Stream(stream) => match ready!(stream.poll_next_unpin(cx)) {
                    Some(Ok(item)) => {
                        self.offset += item.len() as u64;
                        self.size -= item.len() as u64;
                        return Poll::Ready(Some(Ok(item)));
                    }
                    Some(Err(err)) => return Poll::Ready(Some(Err(ReaderRemoteError::from(err)))),
                    None => return Poll::Ready(None),
                },
                RequestState::Delay(sleep) => {
                    ready!(Pin::new(sleep).poll(cx));
                    self.state = RequestState::Init;
                }
            }
        }
    }
    fn poll_read(&mut self, cx: &mut Context) -> Poll<Option<Result<Bytes, ReaderRemoteError>>> {
        loop {
            match self.poll_read_fail(cx) {
                Poll::Ready(Some(Err(err))) => {
                    if self.retry_count == 0 {
                        return Poll::Ready(Some(Err(err)));
                    } else {
                        log::warn!("request failed (retrying soon): {}", err);
                        self.retry_count -= 1;
                        self.state = RequestState::Delay(Box::pin(sleep(self.retry_delay)));
                    }
                }
                result => return result,
            }
        }
    }
}

enum RequestState {
    Init,
    Request(Box<dyn Future<Output = Result<reqwest::Response, reqwest::Error>> + Send + Unpin>),
    Stream(Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send + Unpin>),
    Delay(Pin<Box<tokio::time::Sleep>>),
}

impl<'a> Stream for Builder {
    type Item = Result<Bytes, ReaderRemoteError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.poll_read(cx)
    }
}
