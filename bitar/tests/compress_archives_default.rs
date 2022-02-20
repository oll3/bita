#![cfg(feature = "compress")]
mod common;

use bitar::chunker;

use tokio::fs::File;

use common::*;

// ============================================================================
// Compress zero bytes
// ============================================================================
#[tokio::test]
async fn compress_zero_bytes_fixed_nocompression() {
    let mut input = File::from_std(tempfile::tempfile().unwrap());
    let mut output = File::from_std(tempfile::tempfile().unwrap());

    compress_archive(
        &mut input,
        &mut output,
        chunker::Config::FixedSize(64),
        None,
    )
    .await;

    check_archive_equals_source(&mut output, &mut input).await;
}

#[tokio::test]
async fn compress_zero_bytes_fixed_brotli() {
    let mut input = File::from_std(tempfile::tempfile().unwrap());
    let mut output = File::from_std(tempfile::tempfile().unwrap());

    compress_archive(
        &mut input,
        &mut output,
        chunker::Config::FixedSize(64),
        Some(bitar::CompressionAlgorithm::Brotli),
    )
    .await;

    check_archive_equals_source(&mut output, &mut input).await;
}

#[tokio::test]
async fn compress_zero_bytes_rollsum_nocompression() {
    let mut input = File::from_std(tempfile::tempfile().unwrap());
    let mut output = File::from_std(tempfile::tempfile().unwrap());

    compress_archive(
        &mut input,
        &mut output,
        chunker::Config::RollSum(chunker::FilterConfig::default()),
        None,
    )
    .await;

    check_archive_equals_source(&mut output, &mut input).await;
}

#[tokio::test]
async fn compress_zero_bytes_rollsum_brotli() {
    let mut input = File::from_std(tempfile::tempfile().unwrap());
    let mut output = File::from_std(tempfile::tempfile().unwrap());

    compress_archive(
        &mut input,
        &mut output,
        chunker::Config::RollSum(chunker::FilterConfig::default()),
        Some(bitar::CompressionAlgorithm::Brotli),
    )
    .await;

    check_archive_equals_source(&mut output, &mut input).await;
}

#[tokio::test]
async fn compress_zero_bytes_buzhash_nocompression() {
    let mut input = File::from_std(tempfile::tempfile().unwrap());
    let mut output = File::from_std(tempfile::tempfile().unwrap());

    compress_archive(
        &mut input,
        &mut output,
        chunker::Config::BuzHash(chunker::FilterConfig::default()),
        None,
    )
    .await;

    check_archive_equals_source(&mut output, &mut input).await;
}

#[tokio::test]
async fn compress_zero_bytes_buzhash_brotli() {
    let mut input = File::from_std(tempfile::tempfile().unwrap());
    let mut output = File::from_std(tempfile::tempfile().unwrap());

    compress_archive(
        &mut input,
        &mut output,
        chunker::Config::BuzHash(chunker::FilterConfig::default()),
        Some(bitar::CompressionAlgorithm::Brotli),
    )
    .await;

    check_archive_equals_source(&mut output, &mut input).await;
}

// ============================================================================
// Compress random bytes
// ============================================================================
#[tokio::test]
async fn compress_random_bytes_fixed_nocompression() {
    let mut input = File::from_std(tempfile::tempfile().unwrap());
    let mut output = File::from_std(tempfile::tempfile().unwrap());

    write_random_bytes(&mut input, 8096).await;

    compress_archive(
        &mut input,
        &mut output,
        chunker::Config::FixedSize(64),
        None,
    )
    .await;

    check_archive_equals_source(&mut output, &mut input).await;
}

#[tokio::test]
async fn compress_random_bytes_fixed_brotli() {
    let mut input = File::from_std(tempfile::tempfile().unwrap());
    let mut output = File::from_std(tempfile::tempfile().unwrap());

    write_random_bytes(&mut input, 8096).await;

    compress_archive(
        &mut input,
        &mut output,
        chunker::Config::FixedSize(64),
        Some(bitar::CompressionAlgorithm::Brotli),
    )
    .await;

    check_archive_equals_source(&mut output, &mut input).await;
}

#[tokio::test]
async fn compress_random_bytes_rollsum_nocompression() {
    let mut input = File::from_std(tempfile::tempfile().unwrap());
    let mut output = File::from_std(tempfile::tempfile().unwrap());

    write_random_bytes(&mut input, 8096).await;

    compress_archive(
        &mut input,
        &mut output,
        chunker::Config::RollSum(chunker::FilterConfig::default()),
        None,
    )
    .await;

    check_archive_equals_source(&mut output, &mut input).await;
}

#[tokio::test]
async fn compress_random_bytes_rollsum_brotli() {
    let mut input = File::from_std(tempfile::tempfile().unwrap());
    let mut output = File::from_std(tempfile::tempfile().unwrap());

    write_random_bytes(&mut input, 8096).await;

    compress_archive(
        &mut input,
        &mut output,
        chunker::Config::RollSum(chunker::FilterConfig::default()),
        Some(bitar::CompressionAlgorithm::Brotli),
    )
    .await;

    check_archive_equals_source(&mut output, &mut input).await;
}

#[tokio::test]
async fn compress_random_bytes_buzhash_nocompression() {
    let mut input = File::from_std(tempfile::tempfile().unwrap());
    let mut output = File::from_std(tempfile::tempfile().unwrap());

    write_random_bytes(&mut input, 8096).await;

    compress_archive(
        &mut input,
        &mut output,
        chunker::Config::BuzHash(chunker::FilterConfig::default()),
        None,
    )
    .await;

    check_archive_equals_source(&mut output, &mut input).await;
}

#[tokio::test]
async fn compress_random_bytes_buzhash_brotli() {
    let mut input = File::from_std(tempfile::tempfile().unwrap());
    let mut output = File::from_std(tempfile::tempfile().unwrap());

    write_random_bytes(&mut input, 8096).await;

    compress_archive(
        &mut input,
        &mut output,
        chunker::Config::BuzHash(chunker::FilterConfig::default()),
        Some(bitar::CompressionAlgorithm::Brotli),
    )
    .await;

    check_archive_equals_source(&mut output, &mut input).await;
}
