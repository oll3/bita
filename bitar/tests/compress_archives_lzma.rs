#![cfg(all(feature = "compress", feature = "lzma-compression"))]
mod common;

use bitar::chunker;

use tokio::fs::File;

use common::*;

// ============================================================================
// Compress zero bytes
// ============================================================================

#[tokio::test]
async fn compress_zero_bytes_fixed_lzma() {
    let mut input = File::from_std(tempfile::tempfile().unwrap());
    let mut output = File::from_std(tempfile::tempfile().unwrap());

    compress_archive(
        &mut input,
        &mut output,
        chunker::Config::FixedSize(64),
        Some(bitar::CompressionAlgorithm::Lzma),
    )
    .await;

    check_archive_equals_source(&mut output, &mut input).await;
}

#[tokio::test]
async fn compress_zero_bytes_rollsum_lzma() {
    let mut input = File::from_std(tempfile::tempfile().unwrap());
    let mut output = File::from_std(tempfile::tempfile().unwrap());

    compress_archive(
        &mut input,
        &mut output,
        chunker::Config::RollSum(chunker::FilterConfig::default()),
        Some(bitar::CompressionAlgorithm::Lzma),
    )
    .await;

    check_archive_equals_source(&mut output, &mut input).await;
}

#[tokio::test]
async fn compress_zero_bytes_buzhash_lzma() {
    let mut input = File::from_std(tempfile::tempfile().unwrap());
    let mut output = File::from_std(tempfile::tempfile().unwrap());

    compress_archive(
        &mut input,
        &mut output,
        chunker::Config::BuzHash(chunker::FilterConfig::default()),
        Some(bitar::CompressionAlgorithm::Lzma),
    )
    .await;

    check_archive_equals_source(&mut output, &mut input).await;
}

// ============================================================================
// Compress random bytes
// ============================================================================
#[tokio::test]
async fn compress_random_bytes_fixed_lzma() {
    let mut input = File::from_std(tempfile::tempfile().unwrap());
    let mut output = File::from_std(tempfile::tempfile().unwrap());

    write_random_bytes(&mut input, 8096).await;

    compress_archive(
        &mut input,
        &mut output,
        chunker::Config::FixedSize(64),
        Some(bitar::CompressionAlgorithm::Lzma),
    )
    .await;

    check_archive_equals_source(&mut output, &mut input).await;
}

#[tokio::test]
async fn compress_random_bytes_rollsum_lzma() {
    let mut input = File::from_std(tempfile::tempfile().unwrap());
    let mut output = File::from_std(tempfile::tempfile().unwrap());

    write_random_bytes(&mut input, 8096).await;

    compress_archive(
        &mut input,
        &mut output,
        chunker::Config::RollSum(chunker::FilterConfig::default()),
        Some(bitar::CompressionAlgorithm::Lzma),
    )
    .await;

    check_archive_equals_source(&mut output, &mut input).await;
}

#[tokio::test]
async fn compress_random_bytes_buzhash_lzma() {
    let mut input = File::from_std(tempfile::tempfile().unwrap());
    let mut output = File::from_std(tempfile::tempfile().unwrap());

    write_random_bytes(&mut input, 8096).await;

    compress_archive(
        &mut input,
        &mut output,
        chunker::Config::BuzHash(chunker::FilterConfig::default()),
        Some(bitar::CompressionAlgorithm::Lzma),
    )
    .await;

    check_archive_equals_source(&mut output, &mut input).await;
}
