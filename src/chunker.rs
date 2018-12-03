use buzhash::BuzHash;
use std::io;
use std::io::prelude::*;

// Fill buffer with data from file, or until eof.
fn fill_buf<T>(source: &mut T, buf: &mut Vec<u8>) -> io::Result<usize>
where
    T: Read,
{
    let mut read_size = 0;
    let buf_size = buf.len();
    while read_size < buf_size {
        let rc = source.read(&mut buf[read_size..buf_size])?;
        if rc == 0 {
            break;
        } else {
            read_size += rc;
        }
    }
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
    buf: Vec<u8>,
    buf_size: usize,
    source_index: usize,
    chunk_start: usize,
    chunk_buf: Chunk,
    chunk_filter: u32,
    min_chunk_size: usize,
    max_chunk_size: usize,
    last_val: u8,
    repeated_count: usize,
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
            buf: vec![0; read_buf_size],
            chunk_buf: Chunk {
                offset: 0,
                data: Vec::new(),
            },
            buf_size: 0,
            source_index: 0,
            chunk_start: 0,
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
        F: FnMut(&Chunk),
    {
        loop {
            // Re-fill buffer from source
            self.buf_size = fill_buf(source, &mut self.buf)?;

            if self.buf_size == 0 {
                // EOF
                if self.chunk_buf.data.len() > 0 {
                    self.chunk_buf.offset = self.chunk_start;
                    self.chunk_start = self.source_index + 1;
                    result(&self.chunk_buf);
                }
                return Ok(());
            }

            for val in &self.buf {
                let chunk_start = self.chunk_start;
                let chunk_end = self.source_index + 1;
                let chunk_length = chunk_end - chunk_start;

                // Optimization - If the buzhash window is full of the same value
                // then there is no need pushing another one of the same as the hash
                // won't change.
                if *val == self.last_val {
                    self.repeated_count += 1;
                } else {
                    self.repeated_count = 0;
                    self.last_val = *val;
                }
                if self.repeated_count < self.buzhash.window_size() {
                    self.buzhash.input(*val);
                }

                self.chunk_buf.data.push(*val);

                if self.buzhash.valid() && chunk_length >= self.min_chunk_size {
                    let hash = self.buzhash.sum();

                    if (hash % self.chunk_filter) == (self.chunk_filter - 1)
                        || chunk_length >= self.max_chunk_size
                    {
                        // Match or big chunk - Report it
                        self.chunk_start = chunk_end;
                        self.chunk_buf.offset = chunk_start;
                        result(&self.chunk_buf);
                        self.chunk_buf.data.clear();
                    }
                }
                self.source_index += 1;
            }
        }
    }
}
