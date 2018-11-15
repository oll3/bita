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
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{:x}", byte)?;
        }
        Ok(())
    }
}

pub fn size_to_str(size: &usize) -> String {
    if *size > 1024 * 1024 {
        format!("{} MiB ({} bytes)", size / (1024 * 1024), size)
    } else if *size > 1024 {
        format!("{} KiB ({} bytes)", size / (1024), size)
    } else {
        format!("{} bytes", size)
    }
}
