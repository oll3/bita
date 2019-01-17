use blake2::{Blake2b, Digest};
use crate::ordered_mpsc::OrderedMPSC;
use std::collections::{hash_map::Entry, HashMap};
use std::io;
use std::io::prelude::*;
use threadpool::ThreadPool;

use crate::chunker::*;

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
    src: &mut T,
    chunker: &mut Chunker,
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
    let mut chunk_channel = OrderedMPSC::new();
    let mut chunk_map: HashMap<Vec<u8>, usize> = HashMap::new();
    let mut unique_chunk_index = 0;
    let mut file_size = 0;

    let mut input_hasher_opt = if hash_input {
        Some(Blake2b::new())
    } else {
        None
    };
    chunker
        .scan(src, |chunk_offset, chunk_data| {
            // For each chunk in file
            if let Some(ref mut hasher) = input_hasher_opt {
                hasher.input(chunk_data)
            }
            //file_hash.input(chunk_data);
            let chunk_data = chunk_data.to_vec();
            file_size += chunk_data.len();
            let chunk_tx = chunk_channel.new_tx();
            pool.execute(move || {
                let hash = hash_chunk(&chunk_data);
                chunk_tx
                    .send((
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
                    ))
                    .expect("chunk_tx");
            });

            chunk_channel
                .rx()
                .try_iter()
                .for_each(|(mut chunk_desc, hashed_chunk)| {
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
                });
        })
        .expect("chunker");

    // Wait for threads to be done
    pool.join();

    // Forward the last hashed chunks
    chunk_channel
        .rx()
        .try_iter()
        .for_each(|(mut chunk_desc, hashed_chunk)| {
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
        });
    let total_hash = match input_hasher_opt {
        Some(ref mut hasher) => hasher.clone().result().to_vec(),
        _ => vec![],
    };

    Ok((file_size, total_hash, chunks))
}

// Iterate unique and compressed chunks
pub fn unique_compressed_chunks<T, F, C, H>(
    src: &mut T,
    chunker: &mut Chunker,
    hash_chunk: H,
    compress_chunk: C,
    pool: &ThreadPool,
    hash_input: bool,
    mut result: F,
) -> io::Result<(usize, Vec<u8>, Vec<ChunkSourceDescriptor>)>
where
    T: Read,
    F: FnMut(CompressedChunk),
    C: Fn(&[u8]) -> Vec<u8> + Send + 'static + Copy,
    H: Fn(&[u8]) -> Vec<u8> + Send + 'static + Copy,
{
    let mut chunk_channel = OrderedMPSC::new();
    let (file_size, file_hash, chunks) = unique_chunks(
        src,
        chunker,
        hash_chunk,
        &pool,
        hash_input,
        |hashed_chunk| {
            // For each unique chunk
            let chunk_tx = chunk_channel.new_tx();
            pool.execute(move || {
                // Compress the chunk (in thread context)
                let cdata = compress_chunk(&hashed_chunk.data);
                chunk_tx
                    .send(CompressedChunk {
                        hash: hashed_chunk.hash,
                        offset: hashed_chunk.offset,
                        data: hashed_chunk.data,
                        cdata,
                    })
                    .expect("chunk_tx");
            });

            chunk_channel.rx().try_iter().for_each(|compressed_chunk| {
                result(compressed_chunk);
            });
        },
    )?;

    // Wait for threads to be done
    pool.join();

    // Forward the compressed chunks
    chunk_channel.rx().try_iter().for_each(|compressed_chunk| {
        result(compressed_chunk);
    });
    Ok((file_size, file_hash, chunks))
}
