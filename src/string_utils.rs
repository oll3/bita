use std::fmt;
use std::num::ParseIntError;
use std::ops::{Add, Div, Sub};

pub trait Int:
    Add<Output = Self> + Sub<Output = Self> + Div<Output = Self> + PartialOrd + PartialEq + Copy
{
    fn val(val: usize) -> Self;
}
impl Int for usize {
    fn val(val: usize) -> Self {
        val as Self
    }
}
impl Int for u32 {
    fn val(val: usize) -> Self {
        val as Self
    }
}
impl Int for u64 {
    fn val(val: usize) -> Self {
        val as Self
    }
}

pub struct HexSlice<'a>(&'a [u8]);
impl<'a> HexSlice<'a> {
    pub fn new<T>(data: &'a T) -> HexSlice<'a>
    where
        T: ?Sized + AsRef<[u8]> + 'a,
    {
        HexSlice(data.as_ref())
    }
}
impl<'a> fmt::Display for HexSlice<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

pub fn size_to_str<T: Int + fmt::Display>(size: T) -> String {
    if size > T::val(1024 * 1024) {
        format!("{} MiB ({} bytes)", size / T::val(1024 * 1024), size)
    } else if size > T::val(1024) {
        format!("{} KiB ({} bytes)", size / T::val(1024), size)
    } else {
        format!("{} bytes", size)
    }
}

pub fn hex_str_to_vec(hex_str: &str) -> Result<Vec<u8>, ParseIntError> {
    let mut hex_str = hex_str.to_string();
    if hex_str.len() % 2 == 1 {
        hex_str = "0".to_string() + &hex_str;
    }
    (0..hex_str.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex_str[i..i + 2], 16))
        .collect()
}
