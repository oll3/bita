use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::SeekFrom;
use threadpool::ThreadPool;

use archive_reader::*;
use chunker::*;
use chunker_utils::*;
use config::*;
use file_format;

pub fn run(config: UnpackConfig, _pool: ThreadPool) {
    println!("Do unpack ({:?})", config);

    let src_file =
        File::open(&config.input).expect(&format!("failed to open file ({})", config.input));

    let mut reader = ArchiveReader::new(src_file);
    let chunk_hash_set = reader.chunk_hash_set();

    // Create or open output file.
    // TODO: Check if the given file is a block device or a regular file.
    // If it is a block device we should not try to change its size,
    // instead ensure that the source size is the same as the block device.
    let mut output_file = OpenOptions::new()
        .write(true)
        .create(config.base.force_create)
        .truncate(config.base.force_create)
        .create_new(!config.base.force_create)
        .open(&config.output)
        .expect(&format!("failed to create file ({})", config.output));

    output_file
        .set_len(reader.source_total_size)
        .expect("resize output file");

    reader.read_chunk_data(&chunk_hash_set, |chunk| {
        output_file
            .seek(SeekFrom::Start(chunk.offset as u64))
            .expect("seek output");
        let wc = output_file.write(&chunk.data).expect("write output");
        if wc != chunk.data.len() {
            panic!("wc != chunk.data.len()");
        }
    });
}
