use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::Bound;

#[derive(Eq, PartialEq, Debug, Clone)]
pub(crate) struct OffsetSize {
    pub offset: u64,
    pub size: usize,
}

impl OffsetSize {
    pub fn new(offset: u64, size: usize) -> Self {
        Self { offset, size }
    }
    pub fn end(&self) -> u64 {
        self.offset + self.size as u64
    }
}

impl Ord for OffsetSize {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.offset.cmp(&other.offset) {
            Ordering::Equal => self.size.cmp(&other.size),
            v => v,
        }
    }
}

impl PartialOrd for OffsetSize {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Default)]
pub(crate) struct ChunkLocationMap<V> {
    btm: BTreeMap<OffsetSize, V>,
}

impl<V> ChunkLocationMap<V> {
    pub fn new() -> Self {
        Self {
            btm: BTreeMap::new(),
        }
    }
    // Insert at location. Does not verify if the given location overlaps.
    pub fn insert(&mut self, location: OffsetSize, value: V) {
        self.btm.insert(location, value);
    }
    // Remove at exact location.
    pub fn remove(&mut self, location: &OffsetSize) -> Option<V> {
        self.btm.remove(location)
    }
    // Iterate all locations which the given location overlaps.
    pub fn iter_overlapping(
        &self,
        location: OffsetSize,
    ) -> impl Iterator<Item = (&OffsetSize, &V)> {
        let location_offset = location.offset;
        self.btm
            .range((
                Bound::Unbounded,
                Bound::Excluded(OffsetSize::new(location.end(), 0)),
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
        clm.insert(OffsetSize::new(0, 10), 1);
        clm.insert(OffsetSize::new(15, 7), 3);
        clm.insert(OffsetSize::new(10, 5), 2);
        let mut iter = clm.btm.iter();
        assert_eq!(iter.next(), Some((&OffsetSize::new(0, 10), &1)));
        assert_eq!(iter.next(), Some((&OffsetSize::new(10, 5), &2)));
        assert_eq!(iter.next(), Some((&OffsetSize::new(15, 7), &3)));
        assert_eq!(iter.next(), None);
    }
    #[test]
    fn remove_one() {
        let mut clm = ChunkLocationMap::new();
        clm.insert(OffsetSize::new(0, 10), 1);
        clm.insert(OffsetSize::new(15, 7), 3);
        clm.insert(OffsetSize::new(10, 5), 2);
        assert_eq!(clm.remove(&OffsetSize::new(15, 7)), Some(3));
        assert_eq!(clm.btm.len(), 2);
    }
    #[test]
    fn remove_first() {
        let mut clm = ChunkLocationMap::new();
        clm.insert(OffsetSize::new(0, 10), 1);
        clm.insert(OffsetSize::new(15, 7), 2);
        assert_eq!(clm.remove(&OffsetSize::new(0, 10)), Some(1));
        assert_eq!(clm.btm.len(), 1);
    }
    #[test]
    fn remove_last() {
        let mut clm = ChunkLocationMap::new();
        clm.insert(OffsetSize::new(0, 10), 1);
        clm.insert(OffsetSize::new(15, 7), 2);
        assert_eq!(clm.remove(&OffsetSize::new(15, 7)), Some(2));
        assert_eq!(clm.btm.len(), 1);
    }
    #[test]
    fn no_remove() {
        let mut clm = ChunkLocationMap::new();
        clm.insert(OffsetSize::new(0, 10), 1);
        clm.insert(OffsetSize::new(15, 7), 2);
        assert_eq!(clm.remove(&OffsetSize::new(0, 1)), None);
        assert_eq!(clm.remove(&OffsetSize::new(0, 9)), None);
        assert_eq!(clm.remove(&OffsetSize::new(1, 10)), None);
        assert_eq!(clm.btm.len(), 2);
    }
    #[test]
    fn some_overlap() {
        let mut clm = ChunkLocationMap::new();
        clm.insert(OffsetSize::new(0, 10), 1);
        clm.insert(OffsetSize::new(10, 5), 2);
        clm.insert(OffsetSize::new(15, 7), 3);
        clm.insert(OffsetSize::new(30, 10), 4);
        clm.insert(OffsetSize::new(40, 10), 5);

        let mut iter = clm.iter_overlapping(OffsetSize::new(10, 30));
        assert_eq!(iter.next(), Some((&OffsetSize::new(30, 10), &4)));
        assert_eq!(iter.next(), Some((&OffsetSize::new(15, 7), &3)));
        assert_eq!(iter.next(), Some((&OffsetSize::new(10, 5), &2)));
        assert_eq!(iter.next(), None);
    }
    #[test]
    fn exact_overlap() {
        let mut clm = ChunkLocationMap::new();
        clm.insert(OffsetSize::new(0, 5), 1);
        clm.insert(OffsetSize::new(5, 5), 2);
        clm.insert(OffsetSize::new(10, 5), 3);
        clm.insert(OffsetSize::new(15, 5), 4);

        let mut iter = clm.iter_overlapping(OffsetSize::new(5, 5));
        assert_eq!(iter.next(), Some((&OffsetSize::new(5, 5), &2)));
        assert_eq!(iter.next(), None);
    }
    #[test]
    fn exact_overlap_plus_one() {
        let mut clm = ChunkLocationMap::new();
        clm.insert(OffsetSize::new(0, 5), 1);
        clm.insert(OffsetSize::new(5, 5), 2);
        clm.insert(OffsetSize::new(10, 5), 3);
        clm.insert(OffsetSize::new(15, 5), 4);

        let mut iter = clm.iter_overlapping(OffsetSize::new(5, 6));
        assert_eq!(iter.next(), Some((&OffsetSize::new(10, 5), &3)));
        assert_eq!(iter.next(), Some((&OffsetSize::new(5, 5), &2)));
        assert_eq!(iter.next(), None);
    }
    #[test]
    fn exact_overlap_minus_one() {
        let mut clm = ChunkLocationMap::new();
        clm.insert(OffsetSize::new(0, 5), 1);
        clm.insert(OffsetSize::new(5, 5), 2);
        clm.insert(OffsetSize::new(10, 5), 3);
        clm.insert(OffsetSize::new(15, 5), 4);

        let mut iter = clm.iter_overlapping(OffsetSize::new(4, 6));
        assert_eq!(iter.next(), Some((&OffsetSize::new(5, 5), &2)));
        assert_eq!(iter.next(), Some((&OffsetSize::new(0, 5), &1)));
        assert_eq!(iter.next(), None);
    }
    #[test]
    fn above_no_overlap() {
        let mut clm = ChunkLocationMap::new();
        clm.insert(OffsetSize::new(0, 5), 1);
        clm.insert(OffsetSize::new(5, 5), 2);
        clm.insert(OffsetSize::new(10, 5), 3);
        clm.insert(OffsetSize::new(15, 5), 4);

        let mut iter = clm.iter_overlapping(OffsetSize::new(20, 10));
        assert_eq!(iter.next(), None);
    }
    #[test]
    fn between_no_overlap() {
        let mut clm = ChunkLocationMap::new();
        clm.insert(OffsetSize::new(15, 7), 1);
        clm.insert(OffsetSize::new(30, 10), 2);

        let mut iter = clm.iter_overlapping(OffsetSize::new(23, 6));
        assert_eq!(iter.next(), None);
    }
}
