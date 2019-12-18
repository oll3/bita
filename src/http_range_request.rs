use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use futures_core::stream::Stream;
use hyper::{Body, Client, Uri};
use hyper_rustls::HttpsConnector;
use std::time::Duration;

use crate::error::Error;

#[derive(Debug)]
enum RequestState {
    Init(Option<tokio::time::Delay>),
    Response(hyper::client::ResponseFuture),
    Receiving(Body),
}

pub struct Builder {
    uri: Uri,
    size: u64,
    offset: u64,
    retry_delay: Option<Duration>,
    receive_timeout: Option<Duration>,
    retry_count: u32,
}

impl Builder {
    pub fn new(uri: Uri, offset: u64, size: u64) -> Self {
        Self {
            uri,
            offset,
            size,
            retry_delay: None,
            retry_count: 0,
            receive_timeout: None,
        }
    }
    pub fn retry(mut self, retry_count: u32, retry_delay: Option<Duration>) -> Self {
        self.retry_delay = retry_delay;
        self.retry_count = retry_count;
        self
    }
    pub fn receive_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.receive_timeout = timeout;
        self
    }
    pub fn build(self) -> Request {
        Request {
            uri: self.uri,
            offset: self.offset,
            size: self.size,
            receive_timeout: self.receive_timeout,
            retry_count: self.retry_count,
            retry_delay: self.retry_delay,
            state: RequestState::Init(None),
            request_timeout_timer: None,
        }
    }
}

pub struct Request {
    uri: Uri,
    offset: u64,
    size: u64,
    retry_delay: Option<Duration>,
    receive_timeout: Option<Duration>,
    retry_count: u32,
    state: RequestState,
    request_timeout_timer: Option<tokio::time::Delay>,
}

impl Request {
    fn reset_timeout(&mut self) {
        if let Some(timeout) = self.receive_timeout {
            self.request_timeout_timer = Some(tokio::time::delay_for(timeout));
        }
    }
    fn retry(&mut self) {
        self.retry_count -= 1;
        self.state = RequestState::Init(self.retry_delay.map(tokio::time::delay_for));
        self.request_timeout_timer = None;
    }
}

impl Stream for Request {
    type Item = Result<hyper::body::Bytes, Error>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let pinned = Pin::get_mut(self);
        if let Some(delay) = &mut pinned.request_timeout_timer {
            if let Poll::Ready(()) = Pin::new(delay).poll(cx) {
                // Retry if got error
                if pinned.retry_count == 0 {
                    return Poll::Ready(Some(Err("request timeout".into())));
                }
                log::warn!("receive timeout (retires left: {})", pinned.retry_count);
                pinned.retry();
            }
        }
        loop {
            match pinned.state {
                RequestState::Init(ref mut delay) => {
                    if let Some(delay_f) = delay {
                        let rc = Pin::new(delay_f).poll(cx);
                        if let Poll::Ready(()) = rc {
                            *delay = None;
                        } else {
                            return Poll::Pending;
                        }
                    } else {
                        let end_offset = pinned.offset + pinned.size - 1;
                        let req = hyper::Request::builder()
                            .uri(&pinned.uri)
                            .header("Range", format!("bytes={}-{}", pinned.offset, end_offset))
                            .body(Body::empty())
                            .map_err(|e| ("failed to build request", e))?;
                        let https = HttpsConnector::new();
                        let client = Client::builder()
                            .http1_max_buf_size(64 * 1024)
                            .build::<_, hyper::Body>(https);
                        pinned.state = RequestState::Response(client.request(req));
                        pinned.reset_timeout();
                    }
                }
                RequestState::Response(ref mut response) => {
                    match Pin::new(response).poll(cx) {
                        Poll::Ready(Ok(body)) => {
                            pinned.reset_timeout();
                            pinned.state = RequestState::Receiving(body.into_body());
                        }
                        Poll::Ready(Err(err)) => {
                            // Retry if got error
                            if pinned.retry_count == 0 {
                                return Poll::Ready(Some(Err(("request failed", err).into())));
                            }
                            log::warn!(
                                "request failed: {} (retries left: {})",
                                err,
                                pinned.retry_count
                            );
                            pinned.retry();
                        }
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                    }
                }
                RequestState::Receiving(ref mut body) => {
                    match Pin::new(body).poll_next(cx) {
                        Poll::Ready(Some(Ok(body))) => {
                            pinned.reset_timeout();
                            // Advance in stream
                            pinned.offset += body.len() as u64;
                            pinned.size -= body.len() as u64;
                            return Poll::Ready(Some(Ok(body)));
                        }
                        Poll::Ready(Some(Err(err))) => {
                            // Retry if got error
                            if pinned.retry_count == 0 {
                                return Poll::Ready(Some(Err(("receive failed", err).into())));
                            }
                            log::warn!(
                                "receive failed: {} (retries left: {})",
                                err,
                                pinned.retry_count
                            );
                            pinned.retry();
                        }
                        Poll::Ready(None) => {
                            // End of stream
                            return Poll::Ready(None);
                        }
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                    }
                }
            }
        }
    }
}
