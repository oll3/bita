use buzhash::BuzHash;
use std::io;
use std::io::prelude::*;

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
    chunk_filter: u32,
    min_chunk_size: usize,
    max_chunk_size: usize,
    last_val: u8,
    repeated_count: usize,
    read_buf_size: usize,
}

impl Chunker {
    pub fn new(
        read_buf_size: usize,
        avg_chunk_size: u32,
        min_chunk_size: usize,
        max_chunk_size: usize,
        buzhash: BuzHash,
    ) -> Self {
        Chunker {
            buzhash: buzhash,
            read_buf_size: read_buf_size,
            chunk_filter: avg_chunk_size / 2,
            min_chunk_size: min_chunk_size,
            max_chunk_size: max_chunk_size,
            repeated_count: 0,
            last_val: 0,
        }
    }

    pub fn scan<T, F>(&mut self, source: &mut T, mut result: F) -> io::Result<()>
    where
        T: Read,
        F: FnMut(usize, Vec<u8>),
    {
        let mut source_index: usize = 0;
        let mut buf = Vec::new();
        let mut buf_index = 0;
        let mut chunk_start = 0;
        let mut repeated_count = 0;
        let mut last_val = 0;

        // Assuming min chunk size is less than buzhash window size
        let buzhash_input_limit = self.min_chunk_size - self.buzhash.window_size();

        loop {
            // Fill buffer from source input
            let rc = append_to_buf(source, &mut buf, self.read_buf_size)?;
            if rc == 0 {
                // EOF
                if buf.len() > 0 {
                    result(chunk_start, buf.drain(..).collect());
                }
                return Ok(());
            }

            while buf_index < buf.len() {
                let val = buf[buf_index];
                let chunk_end = source_index + 1;
                let chunk_length = chunk_end - chunk_start;

                if chunk_length >= buzhash_input_limit {
                    if val == last_val {
                        repeated_count += 1;
                    } else {
                        repeated_count = 0;
                        last_val = val;
                    }
                    // Optimization - If the buzhash window is full of the same value
                    // then there is no need pushing another one of the same as the hash
                    // won't change.
                    if repeated_count < self.buzhash.window_size() {
                        self.buzhash.input(val);
                    }
                }

                buf_index += 1;
                source_index += 1;

                if chunk_length >= self.min_chunk_size {
                    let mut got_chunk = chunk_length >= self.max_chunk_size;

                    if !got_chunk && self.buzhash.valid() {
                        let hash = self.buzhash.sum();
                        got_chunk = (hash % self.chunk_filter) == (self.chunk_filter - 1);
                    }

                    if got_chunk {
                        // Match or big chunk - Report it
                        result(chunk_start, buf.drain(..chunk_length).collect());
                        buf_index = 0;
                        chunk_start = chunk_end;
                    }
                }
            }
        }
    }
}
