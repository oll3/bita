use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use futures::stream::Stream;
use hyper::{Body, Client, Request, Uri};
use hyper_tls::HttpsConnector;
use std::time::Duration;

use crate::error::Error;

#[derive(Debug)]
enum RequestState {
    Init(Option<tokio::timer::Delay>),
    Response(hyper::client::ResponseFuture),
    Receiving(Body),
}

pub struct HttpRangeRequest {
    uri: Uri,
    size: u64,
    offset: u64,
    retry_delay: Option<Duration>,
    timeout: Option<Duration>,
    retries: u32,
    state: RequestState,
    request_timeout_timer: Option<tokio::timer::Delay>,
}

impl HttpRangeRequest {
    pub fn new(
        uri: Uri,
        offset: u64,
        size: u64,
        timeout: Option<Duration>,
        retry_delay: Option<Duration>,
        retries: u32,
    ) -> Self {
        Self {
            uri,
            size,
            offset,
            timeout,
            state: RequestState::Init(None),
            retries,
            request_timeout_timer: None,
            retry_delay,
        }
    }
    fn reset_timeout(&mut self) {
        if let Some(timeout) = self.timeout {
            self.request_timeout_timer = Some(tokio::timer::delay_for(timeout));
        }
    }
    fn retry(&mut self) {
        self.retries -= 1;
        self.state =
            RequestState::Init(self.retry_delay.map(|delay| tokio::timer::delay_for(delay)));
        self.request_timeout_timer = None;
    }
}

impl Stream for HttpRangeRequest {
    type Item = Result<hyper::body::Chunk, Error>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if let Some(delay) = &mut self.as_mut().request_timeout_timer {
            if let Poll::Ready(()) = Pin::new(delay).poll(cx) {
                // Retry if got error
                if self.retries == 0 {
                    return Poll::Ready(Some(Err("request timeout".into())));
                }
                self.retry();
                log::error!("Timeout - Retry (retires left: {})!", self.retries);
            }
        }
        loop {
            match self.state {
                RequestState::Init(ref mut delay) => {
                    if let Some(delay_f) = delay {
                        let rc = Pin::new(delay_f).poll(cx);
                        if let Poll::Ready(()) = rc {
                            *delay = None;
                        } else {
                            return Poll::Pending;
                        }
                    } else {
                        let end_offset = self.offset + self.size - 1;
                        let req = Request::builder()
                            .uri(&self.uri)
                            .header("Range", format!("bytes={}-{}", self.offset, end_offset))
                            .body(Body::empty())
                            .map_err(|e| ("request build error", e))?;
                        let https =
                            HttpsConnector::new().map_err(|e| ("https connector error", e))?;
                        let client = Client::builder()
                            .http1_max_buf_size(64 * 1024)
                            .build::<_, hyper::Body>(https);
                        self.state = RequestState::Response(client.request(req));
                        self.reset_timeout();
                    }
                }
                RequestState::Response(ref mut response) => {
                    match Pin::new(response).poll(cx) {
                        Poll::Ready(Ok(body)) => {
                            self.reset_timeout();
                            self.state = RequestState::Receiving(body.into_body());
                        }
                        Poll::Ready(Err(err)) => {
                            // Retry if got error
                            if self.retries == 0 {
                                return Poll::Ready(Some(Err(("request failed", err).into())));
                            }
                            self.retry();
                        }
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                    }
                }
                RequestState::Receiving(ref mut body) => {
                    match Pin::new(body).poll_next(cx) {
                        Poll::Ready(Some(Ok(body))) => {
                            self.reset_timeout();
                            // Advance in stream
                            self.offset += body.len() as u64;
                            self.size -= body.len() as u64;
                            return Poll::Ready(Some(Ok(body)));
                        }
                        Poll::Ready(Some(Err(err))) => {
                            // Retry if got error
                            if self.retries == 0 {
                                return Poll::Ready(Some(Err(
                                    ("error while receiving", err).into()
                                )));
                            }
                            self.retry();
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
