#![allow(dead_code)]
use std::io::Cursor;
use std::io::Read;

use blake2::{Blake2b512, Digest};
use futures_util::stream::StreamExt;
use hyper::service::{make_service_fn, service_fn};
use rand::Rng;
use reqwest::Url;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use bitar::archive_reader::{HttpReader, IoReader};
use bitar::{api, chunker, Archive, CloneOutput, CompressionAlgorithm};

// Checksum of the rand archive source file.
pub static RAND_B2SUM: &[u8] = &[
    0x90, 0x40, 0x55, 0x51, 0x4a, 0xd3, 0x89, 0x66, 0x05, 0x53, 0x65, 0xd5, 0xaa, 0x53, 0x9a, 0x9e,
    0x58, 0xb4, 0x03, 0x68, 0x0a, 0xec, 0x1e, 0x2a, 0x1b, 0x47, 0xf7, 0x99, 0x89, 0x67, 0xa8, 0xba,
    0xa4, 0x50, 0x94, 0xa5, 0x4c, 0x58, 0x6b, 0x88, 0x73, 0x75, 0x92, 0xad, 0x4f, 0xf6, 0xc8, 0xc6,
    0x6a, 0xb2, 0xaf, 0x55, 0x32, 0x39, 0x54, 0xdf, 0x16, 0x79, 0xab, 0x26, 0x34, 0xbd, 0x17, 0xf9,
];
// Checksum of the zero archive source file.
pub static ZERO_B2SUM: &[u8] = &[
    0xcd, 0x47, 0x10, 0x39, 0x0f, 0x85, 0x42, 0xc6, 0x37, 0x65, 0xf0, 0x48, 0x37, 0x13, 0x42, 0x42,
    0xd9, 0x06, 0xb2, 0x97, 0x73, 0x32, 0x82, 0xf2, 0xe9, 0x06, 0xe0, 0x15, 0x1d, 0x3e, 0xa4, 0xec,
    0x31, 0xdb, 0x43, 0xb9, 0x25, 0xce, 0xea, 0x90, 0x84, 0x77, 0x60, 0xf6, 0x47, 0x0e, 0xb7, 0x2a,
    0x1c, 0xf8, 0xfe, 0xaf, 0x1c, 0xd4, 0xb7, 0x6d, 0xdc, 0xde, 0x96, 0xa6, 0x14, 0xb3, 0x52, 0x7a,
];

pub static ARCHIVE_0_1_1_NONE: &str = "tests/resources/rand-0_1_1-none.cba";
pub static ARCHIVE_0_1_1_LZMA: &str = "tests/resources/zero-0_1_1-lzma.cba";
pub static ARCHIVE_0_1_1_ZSTD: &str = "tests/resources/zero-0_1_1-zstd.cba";
pub static ARCHIVE_0_7_1_CORRUPT_HEADER: &str = "tests/resources/rand-0_7_1-corrupt-header.cba";
pub static ARCHIVE_0_7_1_CORRUPT_CHUNK: &str = "tests/resources/rand-0_7_1-corrupt-chunk.cba";
pub static ARCHIVE_0_7_1_BROTLI: &str = "tests/resources/zero-0_7_1-brotli.cba";

pub async fn clone_local_expect_checksum(path: &str, b2sum: &[u8]) {
    clone_expect_checksum(
        Archive::try_init(IoReader::new(File::open(path).await.unwrap()))
            .await
            .unwrap(),
        b2sum,
    )
    .await;
}

pub async fn clone_remote_expect_checksum(path: &str, b2sum: &'static [u8]) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let server_port = listener.local_addr().unwrap().port();
    let server = serve_archive(listener, path);
    let clone_task = tokio::spawn(async move {
        clone_expect_checksum(
            Archive::try_init(HttpReader::from_url(
                Url::parse(&format!("http://127.0.0.1:{}", server_port)).unwrap(),
            ))
            .await
            .unwrap(),
            b2sum,
        )
        .await
    });
    tokio::select! {
        _ = server => panic!("server ended"),
        _ = clone_task => {},
    };
}

async fn clone_expect_checksum<R: bitar::archive_reader::ArchiveReader>(
    mut archive: Archive<R>,
    b2sum: &[u8],
) where
    R::Error: std::fmt::Debug,
{
    let mut output_buf = vec![];
    {
        let mut output =
            CloneOutput::new(Cursor::new(&mut output_buf), archive.build_source_index());
        let mut chunk_stream = archive.chunk_stream(output.chunks());
        while let Some(result) = chunk_stream.next().await {
            output
                .feed(
                    &result
                        .expect("chunk")
                        .decompress()
                        .expect("decompress")
                        .verify()
                        .expect("verify"),
                )
                .await
                .unwrap();
        }
    }
    let mut hash = Blake2b512::new();
    hash.update(&output_buf[..]);
    assert_eq!(&hash.finalize()[..], b2sum);
    assert_eq!(archive.source_checksum().slice(), b2sum);
}

async fn serve_archive(listener: std::net::TcpListener, path: &str) {
    let mut archive_data = vec![];
    std::fs::File::open(path)
        .unwrap()
        .read_to_end(&mut archive_data)
        .unwrap();
    hyper::Server::from_tcp(listener)
        .unwrap()
        .serve(make_service_fn(move |_conn| {
            let data = archive_data.clone();
            async move {
                Ok::<_, std::convert::Infallible>(service_fn(move |req| {
                    // Only respond with the requested range of bytes
                    let range = req
                        .headers()
                        .get("range")
                        .expect("range header")
                        .to_str()
                        .unwrap()[6..]
                        .split('-')
                        .map(|s| s.parse::<u64>().unwrap())
                        .collect::<Vec<u64>>();
                    let start = range[0] as usize;
                    let end = std::cmp::min(range[1] as usize + 1, data.len());
                    let data = data[start..end].to_vec();
                    async move {
                        Ok::<_, hyper::Error>(hyper::Response::new(hyper::Body::from(data)))
                    }
                }))
            }
        }))
        .await
        .unwrap();
}

pub async fn clone_to_memory<R: bitar::archive_reader::ArchiveReader>(
    mut archive: Archive<R>,
) -> Vec<u8>
where
    R::Error: std::fmt::Debug,
{
    let mut output_buf = vec![];
    {
        let mut output =
            CloneOutput::new(Cursor::new(&mut output_buf), archive.build_source_index());
        let mut chunk_stream = archive.chunk_stream(output.chunks());
        while let Some(result) = chunk_stream.next().await {
            output
                .feed(
                    &result
                        .expect("chunk")
                        .decompress()
                        .expect("decompress")
                        .verify()
                        .expect("verify"),
                )
                .await
                .unwrap();
        }
    }
    output_buf
}

pub async fn write_random_bytes(input: &mut File, byte_count: usize) {
    let mut rng = rand::thread_rng();

    let mut data = Vec::with_capacity(byte_count);
    for _ in 0..byte_count {
        data.push(rng.gen());
    }

    input.write_all(&mut data).await.unwrap();
    input.flush().await.unwrap();
    input.rewind().await.unwrap();
}

pub async fn check_archive_equals_source(archive: &mut File, source: &mut File) {
    archive.rewind().await.unwrap();
    source.rewind().await.unwrap();

    let archive = Archive::try_init(IoReader::new(archive)).await.unwrap();

    let cloned_bytes = clone_to_memory(archive).await;
    let mut input_bytes = Vec::new();
    source.read_to_end(&mut input_bytes).await.unwrap();

    assert_eq!(input_bytes, cloned_bytes);
}

pub async fn compress_archive(
    input: &mut File,
    output: &mut File,
    chunker_config: chunker::Config,
    algorithm: Option<CompressionAlgorithm>,
) {
    let compression = if let Some(compression_algorithm) = algorithm {
        Some(
            bitar::Compression::try_new(compression_algorithm, compression_algorithm.max_level())
                .unwrap(),
        )
    } else {
        None
    };

    let options = bitar::api::compress::CreateArchiveOptions {
        chunker_config,
        compression,
        ..Default::default()
    };

    api::compress::create_archive(input, output, &options)
        .await
        .unwrap();
}
