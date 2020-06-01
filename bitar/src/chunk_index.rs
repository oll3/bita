use futures_util::stream::StreamExt;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use tokio::io::AsyncRead;

use crate::{
    chunk_dictionary::ChunkDictionary, chunk_location_map::ChunkLocationMap,
    chunk_location_map::OffsetSize, chunker, HashSum,
};

/// Represents a single chunk re-ordering operation.
#[derive(Debug, Clone, PartialEq)]
pub enum ReorderOp<'a> {
    /// The `Copy` operation says we should copy a chunk from the source location to the
    /// destination offsets. If we already have the given chunk stored in memory (from a
    /// previous `StoreInMem` operation) we should copy it from memory instead of from
    /// the source.
    Copy {
        /// Identifies the chunk to copy.
        hash: &'a HashSum,
        /// Chunk size.
        size: usize,
        /// Chunk source offset.
        source: u64,
        /// Where to write the the chunk to.
        dest: Vec<u64>,
    },
    /// The `StoreInMem` operation says to read a chunk from the source and keep it in
    /// memory until next `Copy` of the given chunk.
    StoreInMem {
        /// Identifies the chunk to store in memory.
        hash: &'a HashSum,
        /// Chunk size.
        size: usize,
        /// Chunk source offset.
        source: u64,
    },
}

#[derive(Eq, PartialEq)]
struct MoveChunk<'a> {
    hash: &'a HashSum,
    size: usize,
    source: u64,
}

impl<'a> Ord for MoveChunk<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.source.cmp(&other.source)
    }
}

impl<'a> PartialOrd for MoveChunk<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Size and offsets of a chunk in a file.
#[derive(Clone, Debug, PartialEq)]
pub struct ChunkLocation {
    size: usize,
    offsets: Vec<u64>,
}

impl ChunkLocation {
    fn add_offset_sorted(&mut self, offset: u64) {
        if let Err(idx) = self.offsets.binary_search(&offset) {
            self.offsets.insert(idx, offset);
        }
    }
    /// Size of chunk.
    pub fn size(&self) -> usize {
        self.size
    }
    /// Offsets of the chunk, ordered lower to higher.
    pub fn offsets(&self) -> &[u64] {
        &self.offsets[..]
    }
}

/// Index of chunks for a file.
///
/// Allows us to map between chunk hashes and source content location.
#[derive(Clone, Debug)]
pub struct ChunkIndex(HashMap<HashSum, ChunkLocation>);

impl ChunkIndex {
    /// Create an empty chunk index.
    pub fn new_empty() -> Self {
        Self(HashMap::new())
    }
    /// Add a chunk with size and offset to the index.
    pub fn add_chunk(&mut self, chunk_hash: HashSum, size: usize, offset: u64) {
        self.0
            .entry(chunk_hash)
            .or_insert(ChunkLocation {
                size,
                offsets: vec![],
            })
            .add_offset_sorted(offset);
    }
    /// Build a ChunkIndex from a readable source.
    pub async fn try_from_readable<T>(
        chunker_config: &chunker::Config,
        hash_length: usize,
        max_buffered_chunks: usize,
        readable: &mut T,
    ) -> Result<Self, std::io::Error>
    where
        T: AsyncRead + Unpin,
    {
        let chunker = chunker::Chunker::new(chunker_config, readable);
        let mut chunk_stream = chunker
            .map(|result| {
                tokio::task::spawn_blocking(move || {
                    result.map(|(offset, chunk)| {
                        (
                            HashSum::b2_digest(&chunk, hash_length as usize),
                            chunk.len(),
                            offset,
                        )
                    })
                })
            })
            .buffered(max_buffered_chunks);
        let mut ci = ChunkIndex::new_empty();
        while let Some(result) = chunk_stream.next().await {
            let (chunk_hash, chunk_size, chunk_offset) = result.unwrap()?;
            ci.add_chunk(chunk_hash, chunk_size, chunk_offset);
        }
        Ok(ci)
    }
    /// Build a ChunkIndex from a ChunkDictionary.
    pub fn from_dictionary(dict: &ChunkDictionary) -> Self {
        let mut ci = ChunkIndex::new_empty();
        let mut chunk_offset = 0;
        dict.rebuild_order.iter().for_each(|&chunk_index| {
            let chunk_index = chunk_index as usize;
            let chunk_hash = HashSum::from_slice(&dict.chunk_descriptors[chunk_index].checksum[..]);
            let chunk_size = dict.chunk_descriptors[chunk_index].source_size as usize;
            ci.add_chunk(chunk_hash, chunk_size, chunk_offset);
            chunk_offset += chunk_size as u64;
        });
        ci
    }
    /// Remove a chunk by hash.
    pub fn remove(&mut self, hash: &HashSum) -> bool {
        self.0.remove(hash).is_some()
    }
    /// Test if a chunk is in the index.
    pub fn contains(&self, hash: &HashSum) -> bool {
        self.0.contains_key(hash)
    }
    fn get(&self, hash: &HashSum) -> Option<&ChunkLocation> {
        self.0.get(hash)
    }
    /// Get first source offset of a chunk.
    fn get_first_offset(&self, hash: &HashSum) -> Option<u64> {
        self.get(&hash)
            .map(|ChunkLocation { offsets, .. }| offsets[0])
    }
    /// Get number of chunks in the index.
    pub fn len(&self) -> usize {
        self.0.len()
    }
    /// Test if index is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    /// Iterate source offsets of a chunk.
    pub fn offsets<'a>(&'a self, hash: &HashSum) -> Option<impl Iterator<Item = u64> + 'a> {
        self.0.get(hash).map(|d| d.offsets.iter().copied())
    }
    /// Iterate chunk hashes in index.
    pub fn keys(&self) -> impl Iterator<Item = &HashSum> {
        self.0.keys()
    }
    /// Filter the given chunk index for chunks which are already in place in self
    ///
    /// Returns the number of chunks filtered and total size of them.
    pub fn strip_chunks_already_in_place(&self, chunk_set: &mut ChunkIndex) -> (usize, u64) {
        let mut num_alread_in_place = 0;
        let mut total_size = 0;
        let new_set: HashMap<HashSum, ChunkLocation> = chunk_set
            .0
            .iter()
            .filter_map(|(hash, cd)| {
                let mut cd = cd.clone();
                if let Some(ChunkLocation { offsets, size }) = self.get(hash) {
                    // For each chunk present in both target and source we compare the offsets and remove
                    // any offset which is present in both from the target.
                    offsets.iter().for_each(|remove_offset| {
                        cd.offsets
                            .iter()
                            .position(|offset| *offset == *remove_offset)
                            .map(|pos| cd.offsets.remove(pos));
                    });
                    let offsets_in_place = offsets.len() - cd.offsets.len();
                    num_alread_in_place += offsets_in_place;
                    total_size += (*size * offsets_in_place) as u64;
                    if cd.offsets.is_empty() {
                        // Chunk is already in place in the target
                        return None;
                    }
                }
                Some((hash.clone(), cd))
            })
            .collect();
        chunk_set.0 = new_set;
        (num_alread_in_place, total_size)
    }
    // Each chunk to reorder will potentially overwrite other chunks.
    // Hence we do a DFS for each chunk and the chunks it will overlap and build the
    // reordering operations in reversed order, down from the leaf nodes up to the root.
    //
    // The tree-like graph may have circular dependencies, where chunk 1 wants to
    // overwrite chunk 2 and chunk 2 wants to overwrite chunk 1. Here one chunk must
    // be kept in memory while the other one(s) is copied into place, and finally write
    // the in memory chunk to its new place.
    fn build_reorder_ops<'a>(
        &'a self,
        root_chunk: MoveChunk<'a>,
        new_order: &ChunkIndex,
        source_layout: &ChunkLocationMap<&'a HashSum>,
        visited: &mut HashSet<&'a HashSum>,
        ops: &mut Vec<ReorderOp<'a>>,
    ) {
        let mut stack: Vec<(MoveChunk, Option<ReorderOp>)> = Vec::new();
        stack.push((root_chunk, None));
        while let Some((chunk, op)) = stack.last_mut() {
            if !visited.contains(chunk.hash) {
                visited.insert(chunk.hash);

                let mut child_stack = Vec::new();
                let mut destinations = Vec::new();

                if let Some(ChunkLocation {
                    size,
                    offsets: target_offsets,
                }) = new_order.get(&chunk.hash)
                {
                    target_offsets.iter().for_each(|&target_offset| {
                        source_layout
                            .iter_overlapping(OffsetSize::new(target_offset, *size))
                            // filter overlapping chunks with same hash
                            .filter(|(_, &overlapped_hash)| chunk.hash != overlapped_hash)
                            .for_each(|(_location, &overlapped_hash)| {
                                let first_offset = self.get_first_offset(overlapped_hash).unwrap();
                                if visited.contains(overlapped_hash) {
                                    ops.push(ReorderOp::StoreInMem {
                                        hash: overlapped_hash,
                                        size: *size,
                                        source: first_offset,
                                    });
                                } else {
                                    child_stack.push((
                                        MoveChunk {
                                            hash: overlapped_hash,
                                            size: *size,
                                            source: first_offset,
                                        },
                                        None,
                                    ));
                                }
                            });
                        destinations.push(target_offset);
                    });
                }
                *op = Some(ReorderOp::Copy {
                    hash: chunk.hash,
                    size: chunk.size,
                    source: chunk.source,
                    dest: destinations,
                });
                stack.append(&mut child_stack);
            } else if let Some((_chunk, Some(op))) = stack.pop() {
                ops.push(op);
            }
        }
    }
    /// Get a description of how to transform one source file into another.
    ///
    /// The transformation is done by reordering the chunks of a file in place trying to match the new
    /// order. Only the chunks present in both the current index and the new index will be reordered,
    /// while chunks that are not present in the current index still has to be fetched from elsewhere.
    pub fn reorder_ops(&self, new_order: &ChunkIndex) -> Vec<ReorderOp> {
        // Generate an intersection between the two chunk sets to find which chunks that should be moved.
        // Also generate a layout of the source where we can go from offset+size to which chunks are
        // located within that range.
        let mut source_layout: ChunkLocationMap<&HashSum> = ChunkLocationMap::new();
        let chunks_to_move: Vec<MoveChunk> = {
            let mut chunks = self
                .0
                .iter()
                .filter_map(|cl| {
                    if new_order.contains(cl.0) {
                        let size = cl.1.size;
                        let first_offset = self.get_first_offset(cl.0).unwrap();
                        source_layout.insert(
                            OffsetSize {
                                size,
                                offset: first_offset,
                            },
                            cl.0,
                        );
                        Some(MoveChunk {
                            hash: cl.0,
                            size,
                            source: first_offset,
                        })
                    } else {
                        None
                    }
                })
                .collect::<Vec<MoveChunk>>();
            // Sort chunks on source offset to make the reordering deterministic.
            chunks.sort();
            chunks
        };
        let mut ops: Vec<ReorderOp> = Vec::new();
        let mut chunks_processed: HashSet<&HashSum> = HashSet::new();
        chunks_to_move.into_iter().for_each(|chunk| {
            if !chunks_processed.contains(&chunk.hash) {
                let mut chunks_in_tree = HashSet::new();
                self.build_reorder_ops(
                    chunk,
                    new_order,
                    &source_layout,
                    &mut chunks_in_tree,
                    &mut ops,
                );

                chunks_in_tree.into_iter().for_each(|hash| {
                    let ChunkLocation { size, offsets } = &self.get(&hash).unwrap();
                    offsets.iter().for_each(|offset| {
                        source_layout.remove(&OffsetSize::new(*offset, *size));
                    });
                    chunks_processed.insert(hash);
                });
            }
        });
        ops
    }
    /// Iterate all chunks in the index.
    ///
    /// Chunks are returned in undefined order.
    pub fn iter_chunks(&self) -> impl Iterator<Item = (&HashSum, &ChunkLocation)> {
        self.0.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl From<(usize, &[u64])> for ChunkLocation {
        fn from((size, offsets): (usize, &[u64])) -> Self {
            Self {
                size,
                offsets: offsets.iter().copied().collect(),
            }
        }
    }

    #[test]
    fn location_sorted_offsets() {
        let mut l = ChunkLocation {
            size: 10,
            offsets: vec![],
        };
        l.add_offset_sorted(20);
        l.add_offset_sorted(10);
        l.add_offset_sorted(5);
        l.add_offset_sorted(8);
        l.add_offset_sorted(0);
        assert_eq!(&l.offsets[..], &[0, 5, 8, 10, 20]);
    }
    #[test]
    fn location_no_duplicate_offset() {
        let mut l = ChunkLocation {
            size: 10,
            offsets: vec![],
        };
        l.add_offset_sorted(5);
        l.add_offset_sorted(5);
        assert_eq!(&l.offsets[..], &[5]);
    }
    #[test]
    fn reorder_with_overlap_and_loop() {
        let current_index = {
            let mut chunks: HashMap<HashSum, ChunkLocation> = HashMap::new();
            chunks.insert(HashSum::from_slice(&[1]), (10, &[0][..]).into());
            chunks.insert(HashSum::from_slice(&[2]), (20, &[10][..]).into());
            chunks.insert(HashSum::from_slice(&[3]), (20, &[30, 50][..]).into());
            ChunkIndex(chunks)
        };
        let target_index = {
            let mut chunks: HashMap<HashSum, ChunkLocation> = HashMap::new();
            chunks.insert(HashSum::from_slice(&[1]), (10, &[60][..]).into());
            chunks.insert(HashSum::from_slice(&[2]), (20, &[50][..]).into());
            chunks.insert(HashSum::from_slice(&[3]), (20, &[10, 30][..]).into());
            chunks.insert(HashSum::from_slice(&[4]), (5, &[0, 5][..]).into());
            ChunkIndex(chunks)
        };
        let ops = current_index.reorder_ops(&target_index);
        assert_eq!(
            ops[0],
            ReorderOp::Copy {
                hash: &HashSum::from_slice(&[1]),
                size: 10,
                source: 0,
                dest: vec![60],
            }
        );
        assert_eq!(
            ops[1],
            ReorderOp::Copy {
                hash: &HashSum::from_slice(&[2]),
                size: 20,
                source: 10,
                dest: vec![50],
            }
        );
        assert_eq!(
            ops[2],
            ReorderOp::Copy {
                hash: &HashSum::from_slice(&[3]),
                size: 20,
                source: 30,
                dest: vec![10, 30],
            }
        );
    }
    #[test]
    fn reorder_but_do_not_copy_to_self() {
        let current_index = {
            let mut chunks: HashMap<HashSum, ChunkLocation> = HashMap::new();
            chunks.insert(HashSum::from_slice(&[1]), (10, &[0, 20][..]).into());
            ChunkIndex(chunks)
        };
        let mut target_index = {
            let mut chunks: HashMap<HashSum, ChunkLocation> = HashMap::new();
            chunks.insert(HashSum::from_slice(&[1]), (10, &[20, 40][..]).into());
            ChunkIndex(chunks)
        };
        current_index.strip_chunks_already_in_place(&mut target_index);
        let ops = current_index.reorder_ops(&target_index);
        assert_eq!(
            ops[0],
            ReorderOp::Copy {
                hash: &HashSum::from_slice(&[1]),
                size: 10,
                source: 0,
                dest: vec![40],
            }
        );
    }
    #[test]
    fn filter_one_chunk_in_place() {
        let current_index = {
            let mut chunks: HashMap<HashSum, ChunkLocation> = HashMap::new();
            chunks.insert(HashSum::from_slice(&[1]), (10, &[0][..]).into());
            ChunkIndex(chunks)
        };
        let mut target_index = {
            let mut chunks: HashMap<HashSum, ChunkLocation> = HashMap::new();
            chunks.insert(HashSum::from_slice(&[1]), (10, &[0][..]).into());
            ChunkIndex(chunks)
        };
        current_index.strip_chunks_already_in_place(&mut target_index);
        assert_eq!(target_index.len(), 0);
    }
    #[test]
    fn filter_one_chunk_multiple_offsets_in_place() {
        let current_index = {
            let mut chunks: HashMap<HashSum, ChunkLocation> = HashMap::new();
            chunks.insert(HashSum::from_slice(&[1]), (10, &[20, 0][..]).into());
            ChunkIndex(chunks)
        };
        let mut target_index = {
            let mut chunks: HashMap<HashSum, ChunkLocation> = HashMap::new();
            chunks.insert(HashSum::from_slice(&[1]), (10, &[0, 20][..]).into());
            ChunkIndex(chunks)
        };
        current_index.strip_chunks_already_in_place(&mut target_index);
        assert_eq!(target_index.len(), 0);
    }
    #[test]
    fn filter_one_chunk_multiple_offsets_not_in_place() {
        let current_index = {
            let mut chunks: HashMap<HashSum, ChunkLocation> = HashMap::new();
            chunks.insert(HashSum::from_slice(&[1]), (10, &[0, 20][..]).into());
            ChunkIndex(chunks)
        };
        let mut target_index = {
            let mut chunks: HashMap<HashSum, ChunkLocation> = HashMap::new();
            chunks.insert(HashSum::from_slice(&[1]), (10, &[20, 30][..]).into());
            ChunkIndex(chunks)
        };
        current_index.strip_chunks_already_in_place(&mut target_index);
        assert_eq!(target_index.len(), 1);
        assert_eq!(
            target_index
                .0
                .get(&HashSum::from_slice(&[1]))
                .unwrap()
                .offsets,
            &[30]
        );
    }
    #[test]
    fn filter_multiple_chunks_in_place() {
        let current_index = {
            let mut chunks: HashMap<HashSum, ChunkLocation> = HashMap::new();
            chunks.insert(HashSum::from_slice(&[1]), (10, &[0][..]).into());
            chunks.insert(HashSum::from_slice(&[2]), (20, &[20][..]).into());
            chunks.insert(HashSum::from_slice(&[3]), (20, &[30, 50][..]).into());
            chunks.insert(HashSum::from_slice(&[4]), (5, &[70, 80][..]).into());
            ChunkIndex(chunks)
        };
        let mut target_index = {
            let mut chunks: HashMap<HashSum, ChunkLocation> = HashMap::new();
            chunks.insert(HashSum::from_slice(&[1]), (10, &[10][..]).into());
            chunks.insert(HashSum::from_slice(&[2]), (20, &[20][..]).into());
            chunks.insert(HashSum::from_slice(&[3]), (20, &[30, 50][..]).into());
            chunks.insert(HashSum::from_slice(&[4]), (5, &[80, 90][..]).into());
            ChunkIndex(chunks)
        };
        current_index.strip_chunks_already_in_place(&mut target_index);
        assert_eq!(target_index.len(), 2);
        assert_eq!(
            target_index.get(&HashSum::from_slice(&[1])).unwrap(),
            &ChunkLocation {
                size: 10,
                offsets: vec![10],
            }
        );
        assert_eq!(
            target_index.get(&HashSum::from_slice(&[4])).unwrap(),
            &ChunkLocation {
                size: 5,
                offsets: vec![90],
            }
        );
    }
}
