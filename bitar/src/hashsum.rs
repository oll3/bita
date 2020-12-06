use blake2::{Blake2b, Digest};
use std::{
    cmp, fmt,
    hash::{Hash, Hasher},
};

const MAX_SUM_LENGHT: usize = 64;

/// Holds a hash sum.
///
/// Typically used for representing the hash of a chunk or the hash of file.
/// Sum can be a maximum of 512 bits/64 bytes.
#[derive(Clone)]
pub struct HashSum {
    sum: [u8; MAX_SUM_LENGHT],
    length: usize,
}

impl HashSum {
    /// Create new hash sum using blake2 to digest the given data.
    pub fn b2_digest(data: &[u8]) -> Self {
        let mut b2 = Blake2b::new();
        b2.update(data);
        let mut sum: [u8; MAX_SUM_LENGHT] = [0; MAX_SUM_LENGHT];
        sum.copy_from_slice(&b2.finalize());
        Self {
            sum,
            length: MAX_SUM_LENGHT,
        }
    }
    /// Returns a new vec containing the hash sum.
    pub fn to_vec(&self) -> Vec<u8> {
        self.slice().to_vec()
    }
    /// Returns the hash sum as a slice.
    pub fn slice(&self) -> &[u8] {
        &self.sum[..self.length]
    }
    /// Returns the length of the hash sum in bytes.
    pub fn len(&self) -> usize {
        self.length
    }
    /// Returns true if the hash sum is empty.
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }
}

impl<T> From<T> for HashSum
where
    T: AsRef<[u8]>,
{
    fn from(v: T) -> Self {
        let min_len = cmp::min(v.as_ref().len(), MAX_SUM_LENGHT);
        let mut sum: [u8; MAX_SUM_LENGHT] = [0; MAX_SUM_LENGHT];
        sum[0..min_len].copy_from_slice(&v.as_ref()[0..min_len]);
        Self {
            sum,
            length: min_len,
        }
    }
}

impl Hash for HashSum {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.slice().hash(state);
    }
}

impl Eq for HashSum {}

impl PartialEq<Vec<u8>> for HashSum {
    fn eq(&self, other: &Vec<u8>) -> bool {
        let min_len = cmp::min(self.len(), other.len());
        &self.sum[0..min_len] == &other[0..min_len]
    }
}

impl PartialEq<&[u8]> for HashSum {
    fn eq(&self, other: &&[u8]) -> bool {
        let min_len = cmp::min(self.len(), other.len());
        &self.sum[0..min_len] == &other[0..min_len]
    }
}

impl PartialEq<HashSum> for HashSum {
    fn eq(&self, other: &Self) -> bool {
        let min_len = cmp::min(self.len(), other.len());
        &self.sum[0..min_len] == &other.sum[0..min_len]
    }
}

impl fmt::Display for HashSum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.slice() {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

impl fmt::Debug for HashSum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.slice() {
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
        let zero_length_hash = HashSum::from(&[]);
        let hash_with_length = HashSum::from(&[0, 1, 2, 3, 4]);
        assert_eq!(zero_length_hash, hash_with_length);
    }

    #[test]
    fn same_sum() {
        let hash1 = HashSum::from(&[0, 1, 2, 3, 4]);
        let hash2 = HashSum::from(&[0, 1, 2, 3, 4]);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn compare_different_sum_diferent_lengths() {
        let hash1 = HashSum::from(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
        let hash2 = HashSum::from(&[0, 1, 2, 3, 4, 0]);
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn compare_same_sum_diferent_lengths() {
        let hash1 = HashSum::from(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
        let hash2 = HashSum::from(&[0, 1, 2, 3, 4]);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn compare_different_sum_same_lengths() {
        let hash1 = HashSum::from(&[0, 1, 2, 3, 4, 5]);
        let hash2 = HashSum::from(&[0, 1, 2, 3, 4, 0]);
        assert_ne!(hash1, hash2);
    }
}
