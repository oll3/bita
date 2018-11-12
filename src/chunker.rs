use buzhash::BuzHash;
use sha2::Digest;
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
pub struct Chunk {
    pub offset: usize,
    pub length: usize,
    pub hash: Vec<u8>,
}

impl fmt::Display for Chunk {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}-{} (size: {}, hash: ",
            self.offset,
            self.offset + self.length,
            self.length
        );

        self.hash.iter().for_each(|ref x| {
            write!(f, "{:x}", *x);
        });
        write!(f, ")");
        Ok(())
    }
}

pub struct Chunker<H> {
    hasher: H,
    buzhash: BuzHash,
    mask: u32,
    buf: Vec<u8>,
    buf_index: usize,
    buf_size: usize,
    source_index: usize,
    chunk_start: usize,
    min_chunk_size: usize,
    max_chunk_size: usize,
}

impl<H> Chunker<H>
where
    H: Digest,
{
    pub fn new(mask_bits: u32, read_buf_size: usize, buzhash: BuzHash, hasher: H) -> Self
    where
        H: Digest,
    {
        Chunker {
            buzhash: buzhash,
            buf: vec![0; read_buf_size],
            buf_index: 0,
            buf_size: 0,
            source_index: 0,
            chunk_start: 0,
            mask: 2u32.pow(mask_bits) - 1,
            hasher: hasher,
            min_chunk_size: 8 * 1024,
            max_chunk_size: 1024 * 1024 * 8,
        }
    }

    pub fn scan<T>(&mut self, source: &mut T) -> io::Result<(Option<Chunk>)>
    where
        T: Read,
    {
        loop {
            if self.buf_index >= self.buf_size {
                // Re-fill buffer from source
                self.buf_size = fill_buf(source, &mut self.buf)?;
                self.buf_index = 0;

                if self.buf_size == 0 && self.chunk_start == self.source_index {
                    return Ok(None);
                }
            }

            let chunk_start = self.chunk_start;
            let chunk_end = self.source_index;
            let chunk_length = chunk_end - chunk_start;

            if self.buf_size == 0 {
                // EOF - Report chunk
                self.chunk_start = self.source_index;
                return Ok(Some(Chunk {
                    offset: chunk_start,
                    length: chunk_length,
                    hash: self.hasher.result_reset().to_vec(),
                }));
            }

            let val = self.buf[self.buf_index];

            self.hasher.input(&[val]);
            self.buzhash.input(val);
            if self.buzhash.valid() && chunk_length >= self.min_chunk_size {
                let hash = self.buzhash.sum();
                if (hash & self.mask) == 0 || chunk_length >= self.max_chunk_size {
                    // Match or big chunk - Report it
                    self.chunk_start = self.source_index;
                    return Ok(Some(Chunk {
                        offset: chunk_start,
                        length: chunk_length,
                        hash: self.hasher.result_reset().to_vec(),
                    }));
                }
            }

            self.buf_index += 1;
            self.source_index += 1;
        }
    }
}
