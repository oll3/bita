use blake2::{Blake2b, Digest};
use std::io::SeekFrom;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use bitar::chunk_index::ChunkIndex;
use bitar::chunker::ChunkerConfig;
use bitar::error::Error;
use bitar::HashSum;

pub struct Output {
    file: File,
    block_dev: bool,
}

impl Output {
    pub async fn new_from(mut file: File) -> Result<Self, Error> {
        file.seek(SeekFrom::Start(0))
            .await
            .map_err(|err| ("failed to seek", err))?;
        let block_dev = is_block_dev(&mut file).await?;
        Ok(Self { file, block_dev })
    }

    pub fn is_block_dev(&self) -> bool {
        self.block_dev
    }

    pub async fn size(&mut self) -> Result<u64, Error> {
        self.file
            .seek(SeekFrom::Start(0))
            .await
            .map_err(|err| ("failed to seek", err))?;
        let size = self
            .file
            .seek(SeekFrom::End(0))
            .await
            .map_err(|err| ("failed to seek", err))?;
        self.file
            .seek(SeekFrom::Start(0))
            .await
            .map_err(|err| ("failed to seek", err))?;
        Ok(size)
    }

    pub async fn seek_write(&mut self, offset: u64, buf: &[u8]) -> Result<(), std::io::Error> {
        self.file.seek(SeekFrom::Start(offset)).await?;
        self.file.write_all(buf).await?;
        Ok(())
    }

    pub async fn seek_read(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), std::io::Error> {
        self.file.seek(SeekFrom::Start(offset)).await?;
        self.file.read_exact(buf).await?;
        Ok(())
    }

    pub async fn resize(&mut self, source_file_size: u64) -> Result<(), Error> {
        self.file
            .set_len(source_file_size)
            .await
            .map_err(|e| ("unable to resize output file", e))?;
        Ok(())
    }

    pub async fn chunk_index(
        &mut self,
        chunker_config: &ChunkerConfig,
        hash_length: usize,
    ) -> Result<ChunkIndex, Error> {
        self.file
            .seek(SeekFrom::Start(0))
            .await
            .map_err(|err| ("seek output failed", err))?;
        let index =
            ChunkIndex::try_build_from_file(chunker_config, hash_length, &mut self.file).await?;
        self.file
            .seek(SeekFrom::Start(0))
            .await
            .map_err(|err| ("seek output failed", err))?;
        Ok(index)
    }

    pub async fn checksum(&mut self) -> Result<HashSum, Error> {
        self.file
            .seek(SeekFrom::Start(0))
            .await
            .map_err(|err| ("failed to seek output", err))?;
        let mut output_hasher = Blake2b::new();
        let mut buffer: Vec<u8> = vec![0; 4 * 1024 * 1024];
        loop {
            let rc = self
                .file
                .read(&mut buffer)
                .await
                .map_err(|err| ("failed to read output", err))?;
            if rc == 0 {
                break;
            }
            output_hasher.input(&buffer[0..rc]);
        }
        Ok(HashSum::from_slice(&output_hasher.result()[..]))
    }
}

// Check if file is a regular file or block device
#[cfg(unix)]
async fn is_block_dev(file: &mut File) -> Result<bool, Error> {
    use std::os::linux::fs::MetadataExt;
    let meta = file
        .metadata()
        .await
        .map_err(|err| ("failed to seek", err))?;
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
