use std::collections::BTreeMap;
use std::ops::Bound;

use crate::ChunkLocation;

#[derive(Default)]
pub(crate) struct ChunkLocationMap<V> {
    btm: BTreeMap<ChunkLocation, V>,
}

impl<V> ChunkLocationMap<V> {
    pub fn new() -> Self {
        Self {
            btm: BTreeMap::new(),
        }
    }
    // Insert at location. Does not verify if the given location overlaps.
    pub fn insert(&mut self, location: ChunkLocation, value: V) {
        self.btm.insert(location, value);
    }
    // Remove at exact location.
    pub fn remove(&mut self, location: &ChunkLocation) -> Option<V> {
        self.btm.remove(location)
    }
    // Iterate all locations which the given location overlaps.
    pub fn iter_overlapping(
        &self,
        location: &ChunkLocation,
    ) -> impl Iterator<Item = (&ChunkLocation, &V)> {
        let location_offset = location.offset;
        self.btm
            .range((
                Bound::Unbounded,
                Bound::Excluded(ChunkLocation::new(location.end(), 0)),
            ))
            .rev()
            .take_while(move |(loc, _v)| location_offset < loc.end())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_iter_in_order() {
        let mut clm = ChunkLocationMap::new();
        clm.insert(ChunkLocation::new(0, 10), 1);
        clm.insert(ChunkLocation::new(15, 7), 3);
        clm.insert(ChunkLocation::new(10, 5), 2);
        let mut iter = clm.btm.iter();
        assert_eq!(iter.next(), Some((&ChunkLocation::new(0, 10), &1)));
        assert_eq!(iter.next(), Some((&ChunkLocation::new(10, 5), &2)));
        assert_eq!(iter.next(), Some((&ChunkLocation::new(15, 7), &3)));
        assert_eq!(iter.next(), None);
    }
    #[test]
    fn remove_one() {
        let mut clm = ChunkLocationMap::new();
        clm.insert(ChunkLocation::new(0, 10), 1);
        clm.insert(ChunkLocation::new(15, 7), 3);
        clm.insert(ChunkLocation::new(10, 5), 2);
        assert_eq!(clm.remove(&ChunkLocation::new(15, 7)), Some(3));
        assert_eq!(clm.btm.len(), 2);
    }
    #[test]
    fn remove_first() {
        let mut clm = ChunkLocationMap::new();
        clm.insert(ChunkLocation::new(0, 10), 1);
        clm.insert(ChunkLocation::new(15, 7), 2);
        assert_eq!(clm.remove(&ChunkLocation::new(0, 10)), Some(1));
        assert_eq!(clm.btm.len(), 1);
    }
    #[test]
    fn remove_last() {
        let mut clm = ChunkLocationMap::new();
        clm.insert(ChunkLocation::new(0, 10), 1);
        clm.insert(ChunkLocation::new(15, 7), 2);
        assert_eq!(clm.remove(&ChunkLocation::new(15, 7)), Some(2));
        assert_eq!(clm.btm.len(), 1);
    }
    #[test]
    fn no_remove() {
        let mut clm = ChunkLocationMap::new();
        clm.insert(ChunkLocation::new(0, 10), 1);
        clm.insert(ChunkLocation::new(15, 7), 2);
        assert_eq!(clm.remove(&ChunkLocation::new(0, 1)), None);
        assert_eq!(clm.remove(&ChunkLocation::new(0, 9)), None);
        assert_eq!(clm.remove(&ChunkLocation::new(1, 10)), None);
        assert_eq!(clm.btm.len(), 2);
    }
    #[test]
    fn some_overlap() {
        let mut clm = ChunkLocationMap::new();
        clm.insert(ChunkLocation::new(0, 10), 1);
        clm.insert(ChunkLocation::new(10, 5), 2);
        clm.insert(ChunkLocation::new(15, 7), 3);
        clm.insert(ChunkLocation::new(30, 10), 4);
        clm.insert(ChunkLocation::new(40, 10), 5);

        let mut iter = clm.iter_overlapping(&ChunkLocation::new(10, 30));
        assert_eq!(iter.next(), Some((&ChunkLocation::new(30, 10), &4)));
        assert_eq!(iter.next(), Some((&ChunkLocation::new(15, 7), &3)));
        assert_eq!(iter.next(), Some((&ChunkLocation::new(10, 5), &2)));
        assert_eq!(iter.next(), None);
    }
    #[test]
    fn exact_overlap() {
        let mut clm = ChunkLocationMap::new();
        clm.insert(ChunkLocation::new(0, 5), 1);
        clm.insert(ChunkLocation::new(5, 5), 2);
        clm.insert(ChunkLocation::new(10, 5), 3);
        clm.insert(ChunkLocation::new(15, 5), 4);

        let mut iter = clm.iter_overlapping(&ChunkLocation::new(5, 5));
        assert_eq!(iter.next(), Some((&ChunkLocation::new(5, 5), &2)));
        assert_eq!(iter.next(), None);
    }
    #[test]
    fn exact_overlap_plus_one() {
        let mut clm = ChunkLocationMap::new();
        clm.insert(ChunkLocation::new(0, 5), 1);
        clm.insert(ChunkLocation::new(5, 5), 2);
        clm.insert(ChunkLocation::new(10, 5), 3);
        clm.insert(ChunkLocation::new(15, 5), 4);

        let mut iter = clm.iter_overlapping(&ChunkLocation::new(5, 6));
        assert_eq!(iter.next(), Some((&ChunkLocation::new(10, 5), &3)));
        assert_eq!(iter.next(), Some((&ChunkLocation::new(5, 5), &2)));
        assert_eq!(iter.next(), None);
    }
    #[test]
    fn exact_overlap_minus_one() {
        let mut clm = ChunkLocationMap::new();
        clm.insert(ChunkLocation::new(0, 5), 1);
        clm.insert(ChunkLocation::new(5, 5), 2);
        clm.insert(ChunkLocation::new(10, 5), 3);
        clm.insert(ChunkLocation::new(15, 5), 4);

        let mut iter = clm.iter_overlapping(&ChunkLocation::new(4, 6));
        assert_eq!(iter.next(), Some((&ChunkLocation::new(5, 5), &2)));
        assert_eq!(iter.next(), Some((&ChunkLocation::new(0, 5), &1)));
        assert_eq!(iter.next(), None);
    }
    #[test]
    fn above_no_overlap() {
        let mut clm = ChunkLocationMap::new();
        clm.insert(ChunkLocation::new(0, 5), 1);
        clm.insert(ChunkLocation::new(5, 5), 2);
        clm.insert(ChunkLocation::new(10, 5), 3);
        clm.insert(ChunkLocation::new(15, 5), 4);

        let mut iter = clm.iter_overlapping(&ChunkLocation::new(20, 10));
        assert_eq!(iter.next(), None);
    }
    #[test]
    fn between_no_overlap() {
        let mut clm = ChunkLocationMap::new();
        clm.insert(ChunkLocation::new(15, 7), 1);
        clm.insert(ChunkLocation::new(30, 10), 2);

        let mut iter = clm.iter_overlapping(&ChunkLocation::new(23, 6));
        assert_eq!(iter.next(), None);
    }
}
