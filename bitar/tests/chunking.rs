use std::path::Path;

use bitar::chunker::{Config, FilterBits, FilterConfig};
use blake2::{Blake2b512, Digest};
use futures_util::stream::StreamExt;

#[tokio::test]
async fn buzhash() {
    for source in &[
        Path::new("tests/resources/random.img"),
        Path::new("tests/resources/zero.img"),
    ] {
        for &(avg, min, max, win) in &[
            (256u32, 10usize, 512usize, 16usize),
            (512, 20, 1200, 20),
            (1024, 500, 6000, 10),
        ] {
            let stem = source.file_stem().unwrap().to_str().unwrap();
            verify_chunks(
                Config::BuzHash(FilterConfig {
                    filter_bits: FilterBits::from_size(avg),
                    min_chunk_size: min,
                    max_chunk_size: max,
                    window_size: win,
                }),
                source,
                format!(
                    "tests/golden/buzhash_{}_{}_{}_{}_{}.sums",
                    stem, avg, min, max, win
                ),
            )
            .await;
        }
    }
}

#[tokio::test]
async fn rollsum() {
    for source in &[
        Path::new("tests/resources/random.img"),
        Path::new("tests/resources/zero.img"),
    ] {
        for &(avg, min, max, win) in &[
            (256u32, 10usize, 512usize, 16usize),
            (512, 20, 1200, 20),
            (1024, 500, 6000, 10),
        ] {
            let stem = source.file_stem().unwrap().to_str().unwrap();
            verify_chunks(
                Config::RollSum(FilterConfig {
                    filter_bits: FilterBits::from_size(avg),
                    min_chunk_size: min,
                    max_chunk_size: max,
                    window_size: win,
                }),
                source,
                format!(
                    "tests/golden/rollsum_{}_{}_{}_{}_{}.sums",
                    stem, avg, min, max, win
                ),
            )
            .await;
        }
    }
}

#[tokio::test]
async fn fixed_size() {
    for source in &[
        Path::new("tests/resources/random.img"),
        Path::new("tests/resources/zero.img"),
    ] {
        for &size in &[128usize, 300, 2048] {
            let stem = source.file_stem().unwrap().to_str().unwrap();
            verify_chunks(
                Config::FixedSize(size),
                source,
                format!("tests/golden/fixed_{}_{}.sums", stem, size),
            )
            .await;
        }
    }
}

// Chunk a source file and verify the hash sums for each chunk against the sums
// in the expected sums file.
async fn verify_chunks<P1: AsRef<Path>, P2: AsRef<Path>>(
    config: Config,
    source: P1,
    expected_sums_path: P2,
) {
    const SUM_LEN: usize = 16;
    let chunker = config.new_chunker(tokio::fs::File::open(source).await.unwrap());
    let sums: Vec<Vec<u8>> = chunker
        .map(|result| Blake2b512::digest(result.unwrap().1.data())[0..SUM_LEN].to_vec())
        .collect()
        .await;

    if std::env::var("MAKE_GOLDEN").is_ok() {
        // If MAKE_GOLDEN is set then generate the expected sums file.
        let flat_sums: Vec<u8> = sums.clone().into_iter().flatten().collect();
        std::fs::write(&expected_sums_path, flat_sums).unwrap();
    }
    let expected_sums = std::fs::read(expected_sums_path).unwrap();
    for (i, expected_sum) in expected_sums[..].chunks(SUM_LEN).enumerate() {
        assert_eq!(sums[i], expected_sum, "sum index {}", i);
    }
    assert_eq!(sums.len(), expected_sums.chunks(SUM_LEN).count());
}
