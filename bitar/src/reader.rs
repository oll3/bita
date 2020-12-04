use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use core::pin::Pin;
use core::task::{Context, Poll};
use futures_core::stream::Stream;
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

#[derive(Debug, Clone, PartialEq)]
pub struct ReadAt {
    pub offset: u64,
    pub size: usize,
}

impl ReadAt {
    pub fn new(offset: u64, size: usize) -> Self {
        Self { offset, size }
    }
    pub fn end_offset(&self) -> u64 {
        self.offset + self.size as u64
    }
}

/// Read bytes at offset.
#[async_trait]
pub trait Reader {
    type Error;
    async fn read_at<'a>(&'a mut self, offset: u64, size: usize) -> Result<Bytes, Self::Error>;
    fn read_chunks<'a>(
        &'a mut self,
        chunks: Vec<ReadAt>,
    ) -> Pin<Box<dyn Stream<Item = Result<Bytes, Self::Error>> + Send + 'a>>;
}

enum State {
    Seek,
    PollSeek,
    Read,
}

struct ChunkReader<'a, R>
where
    R: AsyncRead + AsyncSeek + Unpin + Send + ?Sized,
{
    state: State,
    chunks: Vec<ReadAt>,
    chunk_index: usize,
    buf: BytesMut,
    buf_offset: usize,
    reader: &'a mut R,
}

impl<'a, R> ChunkReader<'a, R>
where
    R: AsyncRead + AsyncSeekExt + Unpin + Send + ?Sized,
{
    fn new(reader: &'a mut R, chunks: Vec<ReadAt>) -> Self {
        let first = chunks
            .get(0)
            .cloned()
            .unwrap_or(ReadAt { offset: 0, size: 0 });
        ChunkReader {
            reader,
            state: State::Seek,
            chunk_index: 0,
            buf: BytesMut::with_capacity(first.size),
            chunks,
            buf_offset: 0,
        }
    }
    fn poll_chunk<'b>(&'b mut self, cx: &mut Context) -> Poll<Option<Result<Bytes, io::Error>>>
    where
        R: AsyncSeek + AsyncRead + Send + Unpin,
        Self: Unpin + Send,
    {
        while self.chunk_index < self.chunks.len() {
            let read_at = &self.chunks[self.chunk_index];
            if self.buf_offset >= read_at.size {
                self.buf_offset = 0;
                self.chunk_index += 1;
                self.state = State::Seek;
                let chunk = self.buf.clone();
                return Poll::Ready(Some(Ok(chunk.freeze())));
            }
            match self.state {
                State::Seek => {
                    match Pin::new(&mut self.reader)
                        .start_seek(cx, io::SeekFrom::Start(read_at.offset))
                    {
                        Poll::Ready(Ok(())) => self.state = State::PollSeek,
                        Poll::Ready(Err(err)) => return Poll::Ready(Some(Err(err))),
                        Poll::Pending => return Poll::Pending,
                    }
                }
                State::PollSeek => match Pin::new(&mut self.reader).poll_complete(cx) {
                    Poll::Ready(Ok(_rc)) => self.state = State::Read,
                    Poll::Ready(Err(err)) => return Poll::Ready(Some(Err(err))),
                    Poll::Pending => return Poll::Pending,
                },
                State::Read => {
                    if self.buf.len() < read_at.size {
                        self.buf.resize(read_at.size, 0);
                    }
                    match Pin::new(&mut self.reader).poll_read(cx, &mut self.buf[self.buf_offset..])
                    {
                        Poll::Ready(Ok(0)) => {
                            return Poll::Ready(Some(Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "archive ended unexpectedly",
                            ))));
                        }
                        Poll::Ready(Ok(rc)) => self.buf_offset += rc,
                        Poll::Ready(Err(err)) => return Poll::Ready(Some(Err(err))),
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
        }
        Poll::Ready(None)
    }
}

impl<'a, R> Stream for ChunkReader<'a, R>
where
    R: AsyncRead + AsyncSeek + Unpin + Send,
{
    type Item = Result<Bytes, io::Error>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.poll_chunk(cx)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let chunks_left = self.chunks.len() - self.chunk_index;
        (chunks_left, Some(chunks_left))
    }
}

#[async_trait]
impl<T> Reader for T
where
    T: AsyncRead + AsyncSeekExt + Unpin + Send,
{
    type Error = io::Error;
    async fn read_at(&mut self, offset: u64, size: usize) -> Result<Bytes, io::Error> {
        self.seek(io::SeekFrom::Start(offset)).await?;
        let mut buf = Vec::with_capacity(size);
        unsafe {
            buf.set_len(size);
        }
        self.read_exact(&mut buf[..]).await?;
        Ok(Bytes::from(buf))
    }
    fn read_chunks<'a>(
        &'a mut self,
        chunks: Vec<ReadAt>,
    ) -> Pin<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + 'a>> {
        Box::pin(ChunkReader::new(self, chunks))
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
        let chunks = vec![
            ReadAt::new(0, 10),
            ReadAt::new(10, 20),
            ReadAt::new(30, 30),
            ReadAt::new(60, 100),
            ReadAt::new(160, 200),
            ReadAt::new(360, 400),
            ReadAt::new(760, 8 * 1024 * 1024),
        ];
        file.write_all(&expected).unwrap();
        let mut reader = File::open(&file.path()).await.unwrap();
        let stream = reader.read_chunks(chunks.clone());
        {
            pin_mut!(stream);
            let mut chunk_offset = 0;
            let mut chunk_count = 0;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.unwrap();
                let chunk_size = chunks[chunk_count].size;
                assert_eq!(chunk, &expected[chunk_offset..chunk_offset + chunk_size]);
                chunk_offset += chunk_size;
                chunk_count += 1;
            }
            assert_eq!(chunk_count, chunks.len());
        }
    }
}
