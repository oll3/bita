use std::num::ParseIntError;

#[macro_export]
macro_rules! human_size {
    ($size:expr) => {{
        let size = ($size) as u64;
        if size > 1024 * 1024 {
            format!("{:.1} MiB ({} bytes)", size as f64 / 1048576.0, size)
        } else if size > 1024 {
            format!("{:.1} KiB ({} bytes)", size as f64 / 1024.0, size)
        } else {
            format!("{} bytes", size)
        }
    }};
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_to_vec_valid() {
        assert_eq!(hex_str_to_vec("1234ef").unwrap(), vec![0x12, 0x34, 0xef]);
    }
    #[test]
    fn hex_to_vec_invalid() {
        hex_str_to_vec("1234efy1").unwrap_err();
    }
    #[test]
    fn human_size_small() {
        assert_eq!(human_size!(100).as_str(), "100 bytes");
    }
    #[test]
    fn human_size_kib() {
        assert_eq!(human_size!(10_000).as_str(), "9.8 KiB (10000 bytes)");
    }
    #[test]
    fn human_size_mib() {
        assert_eq!(
            human_size!(100_000_000).as_str(),
            "95.4 MiB (100000000 bytes)"
        );
    }
    #[test]
    fn human_size_large() {
        assert_eq!(
            human_size!(10_000_000_000).as_str(),
            "9536.7 MiB (10000000000 bytes)"
        );
    }
}
