use curl::easy::Easy;

use archive_reader::ArchiveBackend;
use errors::*;

pub struct RemoteReader {
    url: String,
    handle: curl::easy::Easy,
}

impl RemoteReader {
    pub fn new(url: &str) -> Self {
        let handle = Easy::new();
        RemoteReader {
            url: url.to_string(),
            handle,
        }
    }
}

impl ArchiveBackend for RemoteReader {
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<()> {
        if buf.is_empty() {
            return Ok(());
        }

        let end_offset = offset + (buf.len() - 1) as u64;
        let mut data = Vec::new();

        self.handle
            .url(&self.url)
            .chain_err(|| "unable to set url")?;

        self.handle
            .range(&format!("{}-{}", offset, end_offset))
            .chain_err(|| "unable to set range")?;

        {
            let mut transfer = self.handle.transfer();
            transfer
                .write_function(|new_data| {
                    data.extend_from_slice(new_data);
                    Ok(new_data.len())
                })
                .chain_err(|| "transfer write failed")?;

            transfer
                .perform()
                .chain_err(|| "failed to execute transfer")?;
        }

        buf[..data.len()].clone_from_slice(&data[..]);
        Ok(())
    }

    fn read_in_chunks<F: FnMut(Vec<u8>) -> Result<()>>(
        &mut self,
        start_offset: u64,
        chunk_sizes: &[u64],
        mut chunk_callback: F,
    ) -> Result<()> {
        let tot_size: u64 = chunk_sizes.iter().sum();

        // Create get request
        let mut chunk_buf: Vec<u8> = vec![];
        let mut chunk_index = 0;
        let end_offset = start_offset + tot_size - 1;

        self.handle
            .url(&self.url)
            .chain_err(|| "unable to set url")?;

        self.handle
            .range(&format!("{}-{}", start_offset, end_offset))
            .chain_err(|| "unable to set range")?;

        let mut transfer_result = Ok(());
        {
            let mut transfer = self.handle.transfer();
            transfer
                .write_function(|new_data| {
                    // Got data back from server
                    chunk_buf.extend_from_slice(new_data);

                    while chunk_index < chunk_sizes.len()
                        && chunk_buf.len() >= chunk_sizes[chunk_index] as usize
                    {
                        // Got a full chunk
                        let chunk_size = chunk_sizes[chunk_index] as usize;
                        transfer_result = chunk_callback(chunk_buf.drain(..chunk_size).collect());
                        if transfer_result.is_err() {
                            // TODO: Strange error to return here but the only one available?
                            return Err(curl::easy::WriteError::Pause);
                        }
                        chunk_index += 1;
                    }
                    Ok(new_data.len())
                })
                .chain_err(|| "transfer write failed")?;
            transfer
                .perform()
                .chain_err(|| "failed to execute transfer")?;
        }

        transfer_result
    }
}
