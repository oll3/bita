use futures_util::stream::StreamExt;
use std::collections::{BinaryHeap, HashMap};
use tokio::fs::File;

use crate::chunk_dictionary::ChunkDictionary;
use crate::chunker::{Chunker, ChunkerConfig};
use crate::error::Error;
use crate::hashsum::HashSum;

#[derive(Clone)]
pub struct ChunkIndex(HashMap<HashSum, (usize, BinaryHeap<u64>)>);

impl ChunkIndex {
    pub async fn try_build_from_file(
        chunker_config: &ChunkerConfig,
        hash_length: usize,
        file: &mut File,
    ) -> Result<Self, Error> {
        let chunker = Chunker::new(chunker_config, file);
        let mut chunk_stream = chunker
            .map(|result| {
                tokio::task::spawn(async move {
                    result
                        .map(|(offset, chunk)| {
                            (
                                HashSum::b2_digest(&chunk, hash_length as usize),
                                offset,
                                chunk.len(),
                            )
                        })
                        .map_err(|err| ("error while chunking", err))
                })
            })
            .buffered(8);

        let mut chunk_lookup: HashMap<HashSum, (usize, BinaryHeap<u64>)> = HashMap::new();
        while let Some(result) = chunk_stream.next().await {
            let (chunk_hash, chunk_offset, chunk_size) = result.unwrap()?;
            if let Some(cd) = chunk_lookup.get_mut(&chunk_hash) {
                cd.1.push(chunk_offset);
            } else {
                let mut bh = BinaryHeap::new();
                bh.push(chunk_offset);
                chunk_lookup.insert(chunk_hash.clone(), (chunk_size, bh));
            }
        }
        Ok(Self(chunk_lookup))
    }

    pub fn from_dictionary(dict: &ChunkDictionary) -> Self {
        let mut chunk_lookup: HashMap<HashSum, (usize, BinaryHeap<u64>)> = HashMap::new();
        let mut chunk_offset = 0;
        dict.rebuild_order.iter().for_each(|&chunk_index| {
            let chunk_index = chunk_index as usize;
            let chunk_hash = HashSum::from_slice(&dict.chunk_descriptors[chunk_index].checksum[..]);
            let chunk_size = dict.chunk_descriptors[chunk_index].source_size as usize;
            if let Some(cd) = chunk_lookup.get_mut(&chunk_hash) {
                cd.1.push(chunk_offset);
            } else {
                let mut bh = BinaryHeap::new();
                bh.push(chunk_offset);
                chunk_lookup.insert(chunk_hash.clone(), (chunk_size, bh));
            }
            chunk_offset += chunk_size as u64;
        });
        Self(chunk_lookup)
    }

    pub fn remove(&mut self, hash: &HashSum) -> bool {
        self.0.remove(hash).is_some()
    }

    pub fn contains(&self, hash: &HashSum) -> bool {
        self.0.contains_key(hash)
    }

    pub fn get(&self, hash: &HashSum) -> Option<&(usize, BinaryHeap<u64>)> {
        self.0.get(hash)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    // Get source offsets of a chunk
    pub fn offsets<'a>(&'a self, hash: &HashSum) -> Option<impl Iterator<Item = u64> + 'a> {
        self.0.get(hash).map(|d| d.1.iter().copied())
    }
}
