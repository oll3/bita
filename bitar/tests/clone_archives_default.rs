mod common;

use std::io::ErrorKind;

use bitar::{archive_reader::IoReader, Archive};
use futures_util::stream::StreamExt;
use tokio::fs::File;

use common::*;

#[cfg(not(feature = "lzma-compression"))]
#[tokio::test]
async fn clone_local_v0_1_1_lzma_not_supported() {
    assert!(matches!(
        Archive::try_init(IoReader::new(File::open(ARCHIVE_0_1_1_LZMA).await.unwrap())).await,
        Err(bitar::ArchiveError::InvalidArchive(_))
    ))
}

#[cfg(not(feature = "zstd-compression"))]
#[tokio::test]
async fn clone_local_v0_1_1_zstd_not_supported() {
    assert!(matches!(
        Archive::try_init(IoReader::new(File::open(ARCHIVE_0_1_1_ZSTD).await.unwrap())).await,
        Err(bitar::ArchiveError::InvalidArchive(_))
    ))
}

#[tokio::test]
async fn clone_local_v0_1_1_none() {
    clone_local_expect_checksum(ARCHIVE_0_1_1_NONE, RAND_B2SUM).await;
}

#[tokio::test]
async fn clone_remote_v0_1_1_none() {
    clone_remote_expect_checksum(ARCHIVE_0_1_1_NONE, RAND_B2SUM).await;
}

#[tokio::test]
async fn clone_local_v0_7_1_brotli() {
    clone_local_expect_checksum(ARCHIVE_0_7_1_BROTLI, ZERO_B2SUM).await;
}

#[tokio::test]
async fn clone_remote_v0_7_1_brotli() {
    clone_remote_expect_checksum(ARCHIVE_0_7_1_BROTLI, ZERO_B2SUM).await;
}

#[tokio::test]
async fn clone_local_v0_7_1_corrupt_header() {
    assert!(matches!(
        Archive::try_init(IoReader::new(
            File::open(ARCHIVE_0_7_1_CORRUPT_HEADER).await.unwrap()
        ))
        .await,
        Err(bitar::ArchiveError::InvalidArchive(_))
    ));
}

#[tokio::test]
async fn clone_local_v0_7_1_corrupt_chunk() {
    let mut archive = Archive::try_init(IoReader::new(
        File::open(ARCHIVE_0_7_1_CORRUPT_CHUNK).await.unwrap(),
    ))
    .await
    .unwrap();
    let mut chunk_stream = archive.chunk_stream(&archive.build_source_index());
    while let Some(result) = chunk_stream.next().await {
        if let Err(bitar::HashSumMismatchError { .. }) = result
            .expect("chunk")
            .decompress()
            .expect("decompress")
            .verify()
        {
            // Got the expected hashsum mismatch error!
            return;
        }
    }
    panic!("no hashsum mismatch error?!");
}

#[tokio::test]
async fn clone_local_v0_11_0_brotli_expect_unexpected_end() {
    let mut archive = Archive::try_init(IoReader::new(
        File::open(ARCHIVE_0_11_0_BROTLI_TRUNCATED).await.unwrap(),
    ))
    .await
    .unwrap();
    let mut chunk_stream = archive.chunk_stream(&archive.build_source_index());
    let _chunk1 = chunk_stream
        .next()
        .await
        .unwrap()
        .unwrap()
        .decompress()
        .unwrap()
        .verify()
        .unwrap();
    let first_err = chunk_stream.next().await.unwrap().unwrap_err();
    assert_eq!(first_err.kind(), ErrorKind::UnexpectedEof);
    assert!(chunk_stream.next().await.is_none());
    assert!(chunk_stream.next().await.is_none());
    assert!(chunk_stream.next().await.is_none());
}
