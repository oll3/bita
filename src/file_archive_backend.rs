use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;

use crate::archive_reader::*;
use crate::error::Error;

impl ArchiveBackend for File {
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), Error> {
        self.seek(SeekFrom::Start(offset))
            .map_err(|e| ("failed to seek archive file", e))?;
        self.read_exact(buf)
            .map_err(|e| ("failed to read archive file", e))?;
        Ok(())
    }
    fn read_in_chunks<F: FnMut(Vec<u8>) -> Result<(), Error>>(
        &mut self,
        start_offset: u64,
        chunk_sizes: &[u64],
        mut chunk_callback: F,
    ) -> Result<(), Error> {
        self.seek(SeekFrom::Start(start_offset))
            .map_err(|e| ("failed to seek archive file", e))?;
        for chunk_size in chunk_sizes {
            let mut buf: Vec<u8> = vec![0; *chunk_size as usize];
            self.read_exact(&mut buf[..])
                .map_err(|e| ("failed to read archive file", e))?;
            chunk_callback(buf)?;
        }
        Ok(())
    }
}
