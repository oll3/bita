use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use core::pin::Pin;
use core::task::{Context, Poll};
use futures_core::stream::Stream;
use std::future::Future;
use std::io::SeekFrom;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

struct ChunkReader<'a, R>
where
    R: AsyncRead + AsyncSeek + Unpin + Send + ?Sized,
{
    should_seek: bool,
    start_offset: u64,
    chunk_sizes: &'a [usize],
    chunk_index: usize,
    buf: BytesMut,
    buf_offset: usize,
    reader: &'a mut R,
}

impl<'a, R> ChunkReader<'a, R>
where
    R: AsyncRead + AsyncSeekExt + Unpin + Send + ?Sized,
{
    fn new(reader: &'a mut R, start_offset: u64, chunk_sizes: &'a [usize]) -> Self {
        ChunkReader {
            reader,
            should_seek: true,
            chunk_index: 0,
            start_offset,
            buf: BytesMut::with_capacity(*chunk_sizes.get(0).unwrap_or(&0)),
            chunk_sizes,
            buf_offset: 0,
        }
    }
    fn poll_chunk(&mut self, cx: &mut Context) -> Poll<Option<Result<Bytes, std::io::Error>>>
    where
        R: AsyncSeekExt + AsyncRead + Send + Unpin,
        Self: Unpin + Send,
    {
        if self.should_seek {
            let mut seek = self.reader.seek(SeekFrom::Start(self.start_offset));
            match Pin::new(&mut seek).poll(cx) {
                Poll::Ready(Ok(_rc)) => self.should_seek = false,
                Poll::Ready(Err(err)) => return Poll::Ready(Some(Err(err))),
                Poll::Pending => return Poll::Pending,
            }
        }
        while self.chunk_index < self.chunk_sizes.len() {
            let chunk_size = self.chunk_sizes[self.chunk_index];
            if self.buf_offset >= chunk_size {
                let chunk = self.buf.split_to(chunk_size).freeze();
                self.buf_offset = 0;
                self.chunk_index += 1;
                self.buf.resize(chunk_size, 0);
                return Poll::Ready(Some(Ok(chunk)));
            }
            if self.buf.len() < chunk_size {
                self.buf.resize(chunk_size, 0);
            }
            match Pin::new(&mut self.reader).poll_read(cx, &mut self.buf[self.buf_offset..]) {
                Poll::Ready(Ok(rc)) => self.buf_offset += rc,
                Poll::Ready(Err(err)) => return Poll::Ready(Some(Err(err))),
                Poll::Pending => return Poll::Pending,
            }
        }
        Poll::Ready(None)
    }
}

impl<'a, R> Stream for ChunkReader<'a, R>
where
    R: AsyncRead + AsyncSeek + Unpin + Send,
{
    type Item = Result<Bytes, std::io::Error>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.poll_chunk(cx)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let chunks_left = self.chunk_sizes.len() - self.chunk_index;
        (chunks_left, Some(chunks_left))
    }
}

/// Read bytes at offset.
#[async_trait]
pub trait Reader {
    type Error;
    async fn read_at<'a>(&'a mut self, offset: u64, size: usize) -> Result<Bytes, Self::Error>;
    fn read_chunks<'a>(
        &'a mut self,
        start_offset: u64,
        chunk_sizes: &'a [usize],
    ) -> Pin<Box<dyn Stream<Item = Result<Bytes, Self::Error>> + Send + 'a>>;
}

#[async_trait]
impl<T> Reader for T
where
    T: AsyncRead + AsyncSeekExt + Unpin + Send,
{
    type Error = std::io::Error;
    async fn read_at(&mut self, offset: u64, size: usize) -> Result<Bytes, std::io::Error> {
        self.seek(SeekFrom::Start(offset)).await?;
        let mut buf = BytesMut::with_capacity(size);
        unsafe {
            buf.set_len(size);
        }
        self.read_exact(&mut buf).await?;
        Ok(buf.freeze())
    }
    fn read_chunks<'a>(
        &'a mut self,
        start_offset: u64,
        chunk_sizes: &'a [usize],
    ) -> Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + 'a>> {
        Box::pin(ChunkReader::new(self, start_offset, chunk_sizes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{pin_mut, StreamExt};
    use std::io::Write;
    use tempfile::NamedTempFile;
    use tokio::fs::File;

    #[tokio::test]
    async fn local_read_single_small() {
        let mut file = NamedTempFile::new().unwrap();
        let expected: Vec<u8> = b"hello file".to_vec();
        file.write_all(&expected).unwrap();
        let reader = File::open(&file.path()).await.unwrap();
        pin_mut!(reader);
        let read_back = reader.read_at(0, expected.len()).await.unwrap();
        assert_eq!(read_back, expected);
    }
    #[tokio::test]
    async fn local_read_single_big() {
        let mut file = NamedTempFile::new().unwrap();
        let expected: Vec<u8> = (0..10 * 1024 * 1024).map(|v| v as u8).collect();
        file.write_all(&expected).unwrap();
        let reader = File::open(&file.path()).await.unwrap();
        pin_mut!(reader);
        let read_back = reader.read_at(0, expected.len()).await.unwrap();
        assert_eq!(read_back, expected);
    }
    #[tokio::test]
    async fn local_read_chunks() {
        let mut file = NamedTempFile::new().unwrap();
        let expected: Vec<u8> = (0..10 * 1024 * 1024).map(|v| v as u8).collect();
        let chunk_sizes = vec![10, 20, 30, 100, 200, 400, 8 * 1024 * 1024];
        file.write_all(&expected).unwrap();
        let mut reader = File::open(&file.path()).await.unwrap();
        let stream = reader.read_chunks(0, &chunk_sizes[..]);
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
}
