use async_stream::try_stream;
use futures_core::stream::Stream;
use futures_util::{pin_mut, StreamExt};
use reqwest::RequestBuilder;
use std::time::Duration;
use tokio::time::delay_for;

use crate::Error;

pub(crate) struct Builder {
    request: RequestBuilder,
    size: u64,
    offset: u64,
    retry_delay: Option<Duration>,
    retry_count: u32,
}

impl Builder {
    pub fn new(request: RequestBuilder, offset: u64, size: u64) -> Self {
        Self {
            request,
            offset,
            size,
            retry_delay: None,
            retry_count: 0,
        }
    }
    pub fn retry(mut self, retry_count: u32, retry_delay: Option<Duration>) -> Self {
        self.retry_delay = retry_delay;
        self.retry_count = retry_count;
        self
    }
    fn stream_fail(
        request: RequestBuilder,
        offset: u64,
        size: u64,
    ) -> impl Stream<Item = Result<bytes::Bytes, Error>> {
        log::debug!(
            "Reading from remote, starting at offset {} with size {}...",
            offset,
            size
        );
        let end_offset = offset + size - 1;
        let request = request.header(
            reqwest::header::RANGE,
            format!("bytes={}-{}", offset, end_offset),
        );
        try_stream! {
            let response = request.send().await?;
            let mut stream = response.bytes_stream();
            while let Some(item) = stream.next().await {
                let chunk = item?;
                yield chunk;
            }
        }
    }
    pub fn stream(mut self) -> impl Stream<Item = Result<bytes::Bytes, Error>> {
        try_stream! {
            loop {
                let stream = Self::stream_fail(
                    self.request.try_clone().ok_or(Error::RequestNotClonable)?,
                    self.offset,
                    self.size,
                );
                pin_mut!(stream);
                while let Some(result) = stream.next().await {
                    match result {
                        Ok(item) => {
                            self.offset += item.len() as u64;
                            self.size -= item.len() as u64;
                            yield item
                        },
                        Err(err) => if self.retry_count == 0 {
                            Err(err)?
                        } else {
                            log::warn!("request failed (retrying soon): {}", err);
                            self.retry_count -= 1;
                        }
                    };
                }
                if let Some(retry_delay) = self.retry_delay {
                    delay_for(retry_delay).await;
                }
            }
        }
    }
    async fn single_fail(
        request: RequestBuilder,
        offset: u64,
        size: u64,
    ) -> Result<bytes::Bytes, Error> {
        let end_offset = offset + size - 1;
        let request = request.header(
            reqwest::header::RANGE,
            format!("bytes={}-{}", offset, end_offset),
        );
        let response = request.send().await?;
        Ok(response.bytes().await?)
    }
    pub async fn single(mut self) -> Result<bytes::Bytes, Error> {
        loop {
            match Self::single_fail(
                self.request.try_clone().ok_or(Error::RequestNotClonable)?,
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
            if let Some(retry_delay) = self.retry_delay {
                delay_for(retry_delay).await;
            }
        }
    }
}
