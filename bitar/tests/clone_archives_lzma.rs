#![cfg(feature = "lzma-compression")]
mod common;

use common::*;

#[tokio::test]
async fn clone_local_v0_1_1_lzma() {
    clone_local_expect_checksum(ARCHIVE_0_1_1_LZMA, ZERO_B2SUM).await;
}

#[tokio::test]
async fn clone_remote_v0_1_1_lzma() {
    clone_remote_expect_checksum(ARCHIVE_0_1_1_LZMA, ZERO_B2SUM).await;
}
