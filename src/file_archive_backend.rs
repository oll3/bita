use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;

use archive_reader::*;
use errors::*;

impl ArchiveBackend for File {
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<()> {
        self.seek(SeekFrom::Start(offset))
            .chain_err(|| "failed to seek archive file")?;
        self.read_exact(buf)
            .chain_err(|| "failed to read archive file")?;
        Ok(())
    }
    fn read_in_chunks<F: FnMut(Vec<u8>) -> Result<()>>(
        &mut self,
        start_offset: u64,
        chunk_sizes: &Vec<u64>,
        mut chunk_callback: F,
    ) -> Result<()> {
        self.seek(SeekFrom::Start(start_offset))
            .chain_err(|| "failed to seek archive file")?;
        for chunk_size in chunk_sizes {
            let mut buf: Vec<u8> = vec![0; *chunk_size as usize];
            self.read_exact(&mut buf[..])
                .chain_err(|| "failed to read archive file")?;
            chunk_callback(buf)?;
        }
        Ok(())
    }
}
