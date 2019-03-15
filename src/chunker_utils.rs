use blake2::{Blake2b, Digest};
use std::collections::{hash_map::Entry, HashMap};
use std::io;
use std::io::prelude::*;
use threadpool::ThreadPool;

use crate::chunker::*;
use crate::para_pipe::ParaPipe;

pub type HashBuf = Vec<u8>;

#[derive(Debug, Clone)]
pub struct HashedChunk {
    pub hash: HashBuf,
    pub offset: usize,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct CompressedChunk {
    pub hash: HashBuf,
    pub offset: usize,
    pub data: Vec<u8>,
    pub cdata: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ChunkSourceDescriptor {
    pub unique_chunk_index: usize,
    pub offset: usize,
    pub size: usize,
    pub hash: HashBuf,
}

// Calculate a strong hash on every chunk and forward each chunk
// Returns an array of chunk index.
pub fn unique_chunks<T, F, H>(
    chunker: &mut Chunker<T>,
    hash_chunk: H,
    pool: &ThreadPool,
    hash_input: bool,
    mut result: F,
) -> io::Result<(usize, HashBuf, Vec<ChunkSourceDescriptor>)>
where
    T: Read,
    F: FnMut(HashedChunk),
    H: Fn(&[u8]) -> Vec<u8> + Send + 'static + Copy,
{
    let mut chunks: Vec<ChunkSourceDescriptor> = Vec::new();
    let mut chunk_map: HashMap<Vec<u8>, usize> = HashMap::new();
    let mut unique_chunk_index = 0;
    let mut file_size = 0;

    let mut input_hasher_opt = if hash_input {
        Some(Blake2b::new())
    } else {
        None
    };

    {
        let mut pipe = ParaPipe::new_output(
            pool,
            |(mut chunk_desc, hashed_chunk): (ChunkSourceDescriptor, HashedChunk)| {
                match chunk_map.entry(hashed_chunk.hash.clone()) {
                    Entry::Occupied(o) => {
                        chunk_desc.unique_chunk_index = *o.into_mut();
                    }
                    Entry::Vacant(v) => {
                        // Chunk is unique - Pass forward
                        chunk_desc.unique_chunk_index = unique_chunk_index;
                        v.insert(unique_chunk_index);
                        result(hashed_chunk);
                        unique_chunk_index += 1;
                    }
                }
                chunks.push(chunk_desc);
            },
        );

        while let Some((chunk_offset, chunk_data)) = chunker.scan().expect("scan") {
            // For each chunk in file
            if let Some(ref mut hasher) = input_hasher_opt {
                hasher.input(chunk_data)
            }
            //file_hash.input(chunk_data);
            let chunk_data = chunk_data.to_vec();
            file_size += chunk_data.len();
            pipe.input(
                (chunk_offset as usize, chunk_data),
                move |(chunk_offset, chunk_data): (usize, Vec<u8>)| {
                    // Generate checksun for each chunk
                    let hash = hash_chunk(&chunk_data);
                    (
                        ChunkSourceDescriptor {
                            unique_chunk_index: 0,
                            hash: hash.clone(),
                            offset: chunk_offset,
                            size: chunk_data.len(),
                        },
                        HashedChunk {
                            hash,
                            offset: chunk_offset,
                            data: chunk_data.to_vec(),
                        },
                    )
                },
            );
        }
    }

    let total_hash = match input_hasher_opt {
        Some(ref mut hasher) => hasher.clone().result().to_vec(),
        _ => vec![],
    };

    Ok((file_size, total_hash, chunks))
}

// Iterate unique and compressed chunks
pub fn unique_compressed_chunks<T, F, C, H>(
    chunker: &mut Chunker<T>,
    hash_chunk: H,
    compress_chunk: C,
    pool: &ThreadPool,
    hash_input: bool,
    chunk_callback: F,
) -> io::Result<(usize, Vec<u8>, Vec<ChunkSourceDescriptor>)>
where
    T: Read,
    F: FnMut(CompressedChunk),
    C: Fn(&[u8]) -> Vec<u8> + Send + 'static + Copy,
    H: Fn(&[u8]) -> Vec<u8> + Send + 'static + Copy,
{
    let mut pipe = ParaPipe::new_output(pool, chunk_callback);

    let (file_size, file_hash, chunks) =
        unique_chunks(chunker, hash_chunk, &pool, hash_input, |hashed_chunk| {
            pipe.input(hashed_chunk, move |hashed_chunk: HashedChunk| {
                let cdata = compress_chunk(&hashed_chunk.data);
                CompressedChunk {
                    hash: hashed_chunk.hash,
                    offset: hashed_chunk.offset,
                    data: hashed_chunk.data,
                    cdata,
                }
            });
        })?;

    Ok((file_size, file_hash, chunks))
}
