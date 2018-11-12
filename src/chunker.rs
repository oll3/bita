use buzhash::BuzHash;
use std::io::prelude::*;
use std::{fmt, io};

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

#[derive(Debug)]
pub struct Chunk<'a> {
    pub offset: usize,
    pub length: usize,
    pub data: &'a [u8],
}

impl<'a> fmt::Display for Chunk<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "offset: {}, size: {}", self.offset, self.length)
    }
}

pub struct Chunker {
    buzhash: BuzHash,
    mask: u32,
    buf: Vec<u8>,
    buf_index: usize,
    buf_size: usize,
    source_index: usize,
    chunk_start: usize,
    chunk_buf: Vec<u8>,
    min_chunk_size: usize,
    max_chunk_size: usize,
}

impl Chunker {
    pub fn new(
        mask_bits: u32,
        read_buf_size: usize,
        min_chunk_size: usize,
        max_chunk_size: usize,
        buzhash: BuzHash,
    ) -> Self {
        Chunker {
            buzhash: buzhash,
            buf: vec![0; read_buf_size],
            chunk_buf: Vec::new(),
            buf_index: 0,
            buf_size: 0,
            source_index: 0,
            chunk_start: 0,
            mask: 2u32.pow(mask_bits) - 1,
            min_chunk_size: min_chunk_size,
            max_chunk_size: max_chunk_size,
        }
    }

    pub fn scan<T, F>(&mut self, source: &mut T, mut result: F) -> io::Result<()>
    where
        T: Read,
        F: FnMut(Chunk),
    {
        loop {
            if self.buf_index >= self.buf_size {
                // Re-fill buffer from source
                self.buf_size = fill_buf(source, &mut self.buf)?;
                self.buf_index = 0;

                if self.buf_size == 0 && self.chunk_start == self.source_index {
                    return Ok(());
                }
            }

            let chunk_start = self.chunk_start;
            let chunk_end = self.source_index;
            let chunk_length = chunk_end - chunk_start;

            if self.buf_size == 0 {
                // EOF - Report chunk
                self.chunk_start = chunk_end;
                let chunk = Chunk {
                    offset: chunk_start,
                    length: chunk_length,
                    data: &self.chunk_buf,
                };
                result(chunk);
                return Ok(());
            }

            let val = self.buf[self.buf_index];
            self.chunk_buf.push(val);

            self.buzhash.input(val);
            if self.buzhash.valid() && chunk_length >= self.min_chunk_size {
                let hash = self.buzhash.sum();

                if (hash & self.mask) == 0 || chunk_length >= self.max_chunk_size {
                    // Match or big chunk - Report it
                    self.chunk_start = chunk_end;
                    {
                        let chunk = Chunk {
                            offset: chunk_start,
                            length: chunk_length,
                            data: &self.chunk_buf,
                        };
                        result(chunk);
                    }
                    self.chunk_buf.clear();
                }
            }
            self.buf_index += 1;
            self.source_index += 1;
        }
    }
}
