use blake2::{Blake2b, Digest};
use smallvec::SmallVec;
use std::fmt;
use std::hash::{Hash, Hasher};
/// A hash sum, typically used for representing chunk or source hash.
#[derive(Clone, Debug, Default, Eq)]
pub struct HashSum(SmallVec<[u8; 64]>);

impl HashSum {
    /// Create new empty hash sum.
    pub fn new() -> Self {
        Self::default()
    }
    /// Create new hash sum using blake2 to digest the given data.
    pub fn b2_digest(data: &[u8], hash_length: usize) -> Self {
        let mut b2 = Blake2b::new();
        b2.input(data);
        Self {
            0: SmallVec::from_slice(&b2.result()[0..hash_length]),
        }
    }
    /// Create new hash sum from vec.
    pub fn from_vec(v: Vec<u8>) -> Self {
        Self { 0: v.into() }
    }
    /// Create new hash sum from slice.
    pub fn from_slice(s: &[u8]) -> Self {
        Self {
            0: SmallVec::from_slice(s),
        }
    }
    /// Returns a new vec containing the hash sum.
    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_vec()
    }
    /// Returns the hash sum as a slice.
    pub fn slice(&self) -> &[u8] {
        &self.0[..]
    }
    /// Returns the length of the hash sum in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }
    /// Returns true if the hash sum is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<Vec<u8>> for HashSum {
    fn from(v: Vec<u8>) -> Self {
        Self::from_vec(v)
    }
}

impl From<&[u8]> for HashSum {
    fn from(v: &[u8]) -> Self {
        Self::from_slice(v)
    }
}

impl Hash for HashSum {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl PartialEq<Vec<u8>> for HashSum {
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.0[..] == other[..]
    }
}

impl PartialEq<&[u8]> for HashSum {
    fn eq(&self, other: &&[u8]) -> bool {
        self.0[..] == other[..]
    }
}

impl PartialEq<HashSum> for HashSum {
    fn eq(&self, other: &Self) -> bool {
        self.0[..] == other.0[..]
    }
}

impl fmt::Display for HashSum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0[..] {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_length() {
        let zero_length_hash = HashSum::from_slice(&[]);
        let hash_with_length = HashSum::from_slice(&[0, 1, 2, 3, 4]);
        assert_ne!(zero_length_hash, hash_with_length);
    }

    #[test]
    fn same_sum() {
        let hash1 = HashSum::from_slice(&[0, 1, 2, 3, 4]);
        let hash2 = HashSum::from_slice(&[0, 1, 2, 3, 4]);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn compare_different_sum_diferent_lengths() {
        let hash1 = HashSum::from_slice(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
        let hash2 = HashSum::from_slice(&[0, 1, 2, 3, 4, 0]);
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn compare_different_sum_same_lengths() {
        let hash1 = HashSum::from_slice(&[0, 1, 2, 3, 4, 5]);
        let hash2 = HashSum::from_slice(&[0, 1, 2, 3, 4, 0]);
        assert_ne!(hash1, hash2);
    }
}
