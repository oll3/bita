use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use core::pin::Pin;
use core::task::{Context, Poll};
use futures_util::{ready, stream::Stream};
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, ReadBuf};

use crate::reader::ReadAt;
use crate::Reader;

/// Wrapper which implements Reader for any T which implements
/// tokio AsyncRead and AsyncSeek
pub struct ReaderIo<T>(T);

impl<T> ReaderIo<T> {
    pub fn new(inner: T) -> Self {
        Self(inner)
    }
}

impl<T> From<T> for ReaderIo<T> {
    fn from(inner: T) -> Self {
        Self(inner)
    }
}

#[async_trait]
impl<T> Reader for ReaderIo<T>
where
    T: AsyncRead + AsyncSeek + Unpin + Send,
{
    type Error = io::Error;
    async fn read_at(&mut self, offset: u64, size: usize) -> Result<Bytes, io::Error> {
        self.0.seek(io::SeekFrom::Start(offset)).await?;
        let mut buf = BytesMut::with_capacity(size);
        while buf.len() < size {
            if self.0.read_buf(&mut buf).await? == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
        }
        Ok(buf.freeze())
    }

    fn read_chunks<'a>(
        &'a mut self,
        chunks: Vec<ReadAt>,
    ) -> Pin<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + 'a>> {
        Box::pin(IoChunkReader::new(&mut self.0, chunks))
    }
}

enum IoChunkReaderState {
    Seek,
    PollSeek,
    Read,
}

struct IoChunkReader<'a, R>
where
    R: AsyncRead + AsyncSeek + Unpin + Send + ?Sized,
{
    state: IoChunkReaderState,
    chunks: Vec<ReadAt>,
    chunk_index: usize,
    buf: BytesMut,
    buf_offset: usize,
    reader: &'a mut R,
}

impl<'a, R> IoChunkReader<'a, R>
where
    R: AsyncRead + AsyncSeekExt + Unpin + Send + ?Sized,
{
    fn new(reader: &'a mut R, chunks: Vec<ReadAt>) -> Self {
        let first = chunks
            .get(0)
            .cloned()
            .unwrap_or(ReadAt { offset: 0, size: 0 });
        Self {
            reader,
            state: IoChunkReaderState::Seek,
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
                self.state = IoChunkReaderState::Seek;
                let chunk = self.buf.clone();
                return Poll::Ready(Some(Ok(chunk.freeze())));
            }
            match self.state {
                IoChunkReaderState::Seek => {
                    match Pin::new(&mut self.reader).start_seek(io::SeekFrom::Start(read_at.offset))
                    {
                        Ok(()) => self.state = IoChunkReaderState::PollSeek,
                        Err(err) => return Poll::Ready(Some(Err(err))),
                    }
                }
                IoChunkReaderState::PollSeek => {
                    match ready!(Pin::new(&mut self.reader).poll_complete(cx)) {
                        Ok(_rc) => self.state = IoChunkReaderState::Read,
                        Err(err) => return Poll::Ready(Some(Err(err))),
                    }
                }
                IoChunkReaderState::Read => {
                    if self.buf.len() != read_at.size {
                        self.buf.resize(read_at.size, 0);
                    }
                    let mut buf = ReadBuf::new(&mut self.buf[self.buf_offset..]);
                    match ready!(Pin::new(&mut self.reader).poll_read(cx, &mut buf)) {
                        Ok(()) if buf.filled().is_empty() => {
                            return Poll::Ready(Some(Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "archive ended unexpectedly",
                            ))));
                        }
                        Ok(()) => self.buf_offset += buf.filled().len(),
                        Err(err) => return Poll::Ready(Some(Err(err))),
                    }
                }
            }
        }
        Poll::Ready(None)
    }
}

impl<'a, R> Stream for IoChunkReader<'a, R>
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
        let reader = ReaderIo(File::open(&file.path()).await.unwrap());
        pin_mut!(reader);
        let read_back = reader.read_at(0, expected.len()).await.unwrap();
        assert_eq!(read_back, expected);
    }
    #[tokio::test]
    async fn local_read_single_big() {
        let mut file = NamedTempFile::new().unwrap();
        let expected: Vec<u8> = (0..10 * 1024 * 1024).map(|v| v as u8).collect();
        file.write_all(&expected).unwrap();
        let reader = ReaderIo(File::open(&file.path()).await.unwrap());
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
        let mut reader = ReaderIo(File::open(&file.path()).await.unwrap());
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
