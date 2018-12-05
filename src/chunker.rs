use buzhash::BuzHash;
use std::io;
use std::io::prelude::*;
use std::time::{Duration, Instant};

fn append_to_buf<T>(source: &mut T, buf: &mut Vec<u8>, count: usize) -> io::Result<usize>
where
    T: Read,
{
    let mut read_size = 0;
    let buf_size = buf.len();
    buf.resize(buf_size + count, 0);
    while read_size < count {
        let offset = buf_size + read_size;
        let rc = source.read(&mut buf[offset..])?;
        if rc == 0 {
            break;
        } else {
            read_size += rc;
        }
    }
    buf.resize(buf_size + read_size, 0);
    return Ok(read_size);
}

#[derive(Debug, Clone)]
pub struct Chunk {
    pub offset: usize,
    pub data: Vec<u8>,
}

#[derive(Clone)]
pub struct Chunker {
    buzhash: BuzHash,
    filter_mask: u32,
    min_chunk_size: usize,
    max_chunk_size: usize,
    last_val: u8,
    repeated_count: usize,
    read_buf_size: usize,
    pub scan_time: Duration,
    pub read_time: Duration,
}

impl Chunker {
    pub fn new(
        read_buf_size: usize,
        chunk_filter_bits: u32,
        min_chunk_size: usize,
        max_chunk_size: usize,
        buzhash: BuzHash,
    ) -> Self {
        Chunker {
            buzhash: buzhash,
            read_buf_size: read_buf_size,
            filter_mask: !0 >> (32 - chunk_filter_bits),
            min_chunk_size: min_chunk_size,
            max_chunk_size: max_chunk_size,
            repeated_count: 0,
            last_val: 0,
            scan_time: Duration::new(0, 0),
            read_time: Duration::new(0, 0),
        }
    }

    pub fn scan<T, F>(&mut self, source: &mut T, mut result: F) -> io::Result<()>
    where
        T: Read,
        F: FnMut(usize, &[u8]),
    {
        let mut source_index: usize = 0;
        let mut buf = Vec::new();
        let mut buf_index = 0;
        let mut chunk_start = 0;

        // Assuming min chunk size is less than buzhash window size
        let buzhash_input_limit = self.min_chunk_size - self.buzhash.window_size();

        loop {
            // Fill buffer from source input
            let read_start_time = Instant::now();
            let rc = append_to_buf(source, &mut buf, self.read_buf_size)?;
            self.read_time += read_start_time.elapsed();
            if rc == 0 {
                // EOF
                if buf.len() > 0 {
                    result(chunk_start, &buf[..]);
                }
                return Ok(());
            }

            while !self.buzhash.valid() && buf_index < buf.len() {
                // Initialize the buzhash
                self.buzhash.init(buf[buf_index]);
                buf_index += 1;
                source_index += 1;
            }

            let mut start_scan_time = Instant::now();
            while buf_index < buf.len() {
                let val = buf[buf_index];
                let chunk_end = source_index + 1;
                let chunk_length = chunk_end - chunk_start;

                if chunk_length >= buzhash_input_limit {
                    self.buzhash.input(val);
                }

                buf_index += 1;
                source_index += 1;

                if chunk_length >= self.min_chunk_size {
                    let mut got_chunk = chunk_length >= self.max_chunk_size;

                    if !got_chunk {
                        let hash = self.buzhash.sum();
                        got_chunk = hash | self.filter_mask == hash;
                    }

                    if got_chunk {
                        // Match or big chunk - Report it
                        //let chunk_data = buf.drain(..chunk_length).collect();
                        self.scan_time += start_scan_time.elapsed();
                        result(chunk_start, &buf[..chunk_length]);
                        start_scan_time = Instant::now();
                        buf.drain(..chunk_length);
                        buf_index = 0;
                        chunk_start = chunk_end;
                    }
                }
            }
            self.scan_time += start_scan_time.elapsed();
        }
    }
}
