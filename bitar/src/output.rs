use async_trait::async_trait;
use blake2::{Blake2b, Digest};
use std::io::SeekFrom;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::chunk_index::ChunkIndex;
use crate::chunker::ChunkerConfig;
use crate::Error;
use crate::HashSum;

#[async_trait]
pub trait Output {
    /// Write a single chunk to output at the given offsets
    async fn write_chunk(
        &mut self,
        hash: &HashSum,
        offsets: &[u64],
        buf: &[u8],
    ) -> Result<(), Error>;
    /// Read a to buffer from file at offset
    async fn seek_read(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), Error>;
    /// Build a chunk index of output
    async fn chunk_index(
        &mut self,
        chunker_config: &ChunkerConfig,
        hash_length: usize,
    ) -> Result<ChunkIndex, Error>;
    /// Generate total checksum of the output
    async fn checksum(&mut self) -> Result<HashSum, Error>;
}

/// Output file
pub struct OutputFile {
    file: File,
    block_dev: bool,
}

impl OutputFile {
    pub async fn new_from(mut file: File) -> Result<Self, Error> {
        file.seek(SeekFrom::Start(0)).await?;
        let block_dev = is_block_dev(&mut file).await?;
        Ok(Self { file, block_dev })
    }

    pub fn is_block_dev(&self) -> bool {
        self.block_dev
    }

    pub async fn size(&mut self) -> Result<u64, Error> {
        self.file.seek(SeekFrom::Start(0)).await?;
        let size = self.file.seek(SeekFrom::End(0)).await?;
        self.file.seek(SeekFrom::Start(0)).await?;
        Ok(size)
    }

    async fn seek_write(&mut self, offset: u64, buf: &[u8]) -> Result<(), std::io::Error> {
        self.file.seek(SeekFrom::Start(offset)).await?;
        self.file.write_all(buf).await?;
        Ok(())
    }

    async fn seek_read(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), std::io::Error> {
        self.file.seek(SeekFrom::Start(offset)).await?;
        self.file.read_exact(buf).await?;
        Ok(())
    }

    pub async fn resize(&mut self, source_file_size: u64) -> Result<(), Error> {
        self.file.set_len(source_file_size).await?;
        Ok(())
    }

    async fn chunk_index(
        &mut self,
        chunker_config: &ChunkerConfig,
        hash_length: usize,
    ) -> Result<ChunkIndex, Error> {
        self.file.seek(SeekFrom::Start(0)).await?;
        let index =
            ChunkIndex::try_build_from_file(chunker_config, hash_length, &mut self.file).await?;
        self.file.seek(SeekFrom::Start(0)).await?;
        Ok(index)
    }

    async fn checksum(&mut self) -> Result<HashSum, Error> {
        self.file.seek(SeekFrom::Start(0)).await?;
        let mut output_hasher = Blake2b::new();
        let mut buffer: Vec<u8> = vec![0; 4 * 1024 * 1024];
        loop {
            let rc = self.file.read(&mut buffer).await?;
            if rc == 0 {
                break;
            }
            output_hasher.input(&buffer[0..rc]);
        }
        Ok(HashSum::from_slice(&output_hasher.result()[..]))
    }
}

#[async_trait]
impl Output for OutputFile {
    async fn write_chunk(
        &mut self,
        _hash: &HashSum,
        offsets: &[u64],
        buf: &[u8],
    ) -> Result<(), Error> {
        for &offset in offsets {
            self.seek_write(offset, buf).await?;
        }
        Ok(())
    }
    async fn seek_read(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), Error> {
        Ok(self.seek_read(offset, buf).await?)
    }
    async fn chunk_index(
        &mut self,
        chunker_config: &ChunkerConfig,
        hash_length: usize,
    ) -> Result<ChunkIndex, Error> {
        Ok(self.chunk_index(chunker_config, hash_length).await?)
    }
    async fn checksum(&mut self) -> Result<HashSum, Error> {
        self.checksum().await
    }
}

// Check if file is a regular file or block device
#[cfg(unix)]
async fn is_block_dev(file: &mut File) -> Result<bool, Error> {
    use std::os::linux::fs::MetadataExt;
    let meta = file.metadata().await?;
    if meta.st_mode() & 0x6000 == 0x6000 {
        Ok(true)
    } else {
        Ok(false)
    }
}

#[cfg(not(unix))]
async fn is_block_dev(_file: &mut File) -> Result<bool, Error> {
    Ok(false)
}
