use crate::buzhash::BuzHash;
use std::io;
use std::io::prelude::*;
use std::time::{Duration, Instant};

const CHUNKER_BUF_SIZE: usize = 1024 * 1024;

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
    Ok(read_size)
}

#[derive(Debug, Clone)]
pub struct Chunk {
    pub offset: usize,
    pub data: Vec<u8>,
}

#[derive(Clone)]
pub struct ChunkerParams {
    pub buzhash: BuzHash,
    pub filter_mask: u32,
    pub min_chunk_size: usize,
    pub max_chunk_size: usize,
}

impl ChunkerParams {
    pub fn new(
        chunk_filter_bits: u32,
        min_chunk_size: usize,
        max_chunk_size: usize,
        buzhash: BuzHash,
    ) -> Self {
        ChunkerParams {
            filter_mask: !0 >> (32 - chunk_filter_bits),
            min_chunk_size,
            max_chunk_size,
            buzhash,
        }
    }
}

pub struct Chunker<'a, T>
where
    T: Read,
{
    buzhash: BuzHash,
    filter_mask: u32,
    min_chunk_size: usize,
    max_chunk_size: usize,
    source_buf: Vec<u8>,
    pub scan_time: Duration,
    pub read_time: Duration,
    source: &'a mut T,
}

impl<'a, T> Chunker<'a, T>
where
    T: Read,
{
    pub fn new(params: ChunkerParams, source: &'a mut T) -> Self {
        Chunker {
            filter_mask: params.filter_mask,
            min_chunk_size: params.min_chunk_size,
            max_chunk_size: params.max_chunk_size,
            buzhash: params.buzhash,
            source_buf: Vec::new(),
            scan_time: Duration::new(0, 0),
            read_time: Duration::new(0, 0),
            source,
        }
    }

    // Can be called before scan to preload scan buffer
    pub fn preload(&mut self, data: &[u8]) {
        self.source_buf.extend(data);
    }

    pub fn scan<F>(&mut self, mut result: F) -> io::Result<()>
    where
        F: FnMut(usize, &[u8]),
    {
        let mut source_index: usize = 0;
        let mut buf_index = 0;
        let mut chunk_start = 0;

        // Assuming min chunk size is less than buzhash window size
        let buzhash_input_limit = self.min_chunk_size - self.buzhash.window_size();

        loop {
            // Fill buffer from source input
            let read_start_time = Instant::now();
            let rc = append_to_buf(self.source, &mut self.source_buf, CHUNKER_BUF_SIZE)?;
            self.read_time += read_start_time.elapsed();
            if rc == 0 {
                // EOF
                if !self.source_buf.is_empty() {
                    result(chunk_start, &self.source_buf[..]);
                }
                return Ok(());
            }

            while !self.buzhash.valid() && buf_index < self.source_buf.len() {
                // Initialize the buzhash
                self.buzhash.init(self.source_buf[buf_index]);
                buf_index += 1;
                source_index += 1;
            }

            let mut start_scan_time = Instant::now();
            while buf_index < self.source_buf.len() {
                let val = self.source_buf[buf_index];
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
                        result(chunk_start, &self.source_buf[..chunk_length]);
                        start_scan_time = Instant::now();
                        self.source_buf.drain(..chunk_length);
                        buf_index = 0;
                        chunk_start = chunk_end;
                    }
                }
            }
            self.scan_time += start_scan_time.elapsed();
        }
    }
}
