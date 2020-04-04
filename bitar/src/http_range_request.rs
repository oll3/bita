use async_stream::try_stream;
use futures_core::stream::Stream;
use futures_util::{pin_mut, StreamExt};
use reqwest::Url;
use std::time::Duration;
use tokio::time::delay_for;

use crate::Error;

pub(crate) struct Builder {
    url: Url,
    size: u64,
    offset: u64,
    retry_delay: Option<Duration>,
    receive_timeout: Option<Duration>,
    retry_count: u32,
    client: reqwest::Client,
}

impl Builder {
    pub fn new(url: Url, offset: u64, size: u64) -> Self {
        Self {
            url,
            offset,
            size,
            retry_delay: None,
            retry_count: 0,
            receive_timeout: None,
            client: reqwest::Client::new(),
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
    fn stream_fail(
        client: reqwest::Client,
        offset: u64,
        size: u64,
        url: Url,
        timeout: Option<Duration>,
    ) -> impl Stream<Item = Result<bytes::Bytes, Error>> {
        try_stream! {
            log::debug!("Requesting {}, starting at offset {} with size {}...", url, offset, size);
            let end_offset = offset + size - 1;
            let mut request = client.get(url).header(
                reqwest::header::RANGE,
                format!("bytes={}-{}", offset, end_offset),
            );
            if let Some(timeout) = timeout {
                request = request.timeout(timeout);
            }
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
                    self.client.clone(),
                    self.offset,
                    self.size,
                    self.url.clone(),
                    self.receive_timeout.clone(),
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
                            log::warn!("request for {} failed (retrying soon): {}", self.url, err);
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
    pub async fn single_fail(
        client: reqwest::Client,
        offset: u64,
        size: u64,
        url: Url,
        timeout: Option<Duration>,
    ) -> Result<bytes::Bytes, Error> {
        let end_offset = offset + size - 1;
        let mut request = client.get(url).header(
            reqwest::header::RANGE,
            format!("bytes={}-{}", offset, end_offset),
        );
        if let Some(timeout) = timeout {
            request = request.timeout(timeout);
        }
        let response = request.send().await?;
        Ok(response.bytes().await?)
    }
    pub async fn single(mut self) -> Result<bytes::Bytes, Error> {
        loop {
            match Self::single_fail(
                self.client.clone(),
                self.offset,
                self.size,
                self.url.clone(),
                self.receive_timeout,
            )
            .await
            {
                Ok(item) => return Ok(item),
                Err(err) => {
                    if self.retry_count == 0 {
                        return Err(err);
                    } else {
                        log::warn!("request for {} failed (retrying soon): {}", self.url, err);
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
