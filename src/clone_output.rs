use protobuf::{RepeatedField, SingularPtrField};
use std::collections::{hash_map::Entry, HashMap};
use std::io::prelude::*;
use std::io::BufWriter;
use std::io::SeekFrom;

use crate::archive;
use crate::archive_reader::ArchiveReader;
use crate::chunk_dictionary;
use crate::chunker::ChunkerParams;
use crate::chunker_utils::HashBuf;
use crate::compression::Compression;
use crate::errors::*;
use crate::string_utils::*;

pub trait CloneOutput {
    fn write_chunk(&mut self, checksum: &[u8], offsets: &[u64], chunk_data: &[u8]) -> Result<()>;
}

// Clone output for unpacked file
impl<O> CloneOutput for BufWriter<O>
where
    O: Write + Seek,
{
    fn write_chunk(&mut self, _checksum: &[u8], offsets: &[u64], chunk_data: &[u8]) -> Result<()> {
        for offset in offsets {
            self.seek(SeekFrom::Start(*offset))
                .chain_err(|| "failed to seek")?;
            self.write_all(chunk_data).chain_err(|| "failed to write")?;
        }
        Ok(())
    }
}

// Clone output for archive file
pub struct ArchiveOutput<O>
where
    O: Write + Seek,
{
    file: O,
    compression: Compression,
    chunk_map: HashMap<HashBuf, usize>,
    chunk_descriptors: Vec<archive::ChunkDescriptor>,
    chunk_data_offset: u64,
    current_archive_offset: u64,
    rebuild_order: Vec<HashBuf>,
    source_checksum: HashBuf,
    source_total_size: u64,
    chunker_params: ChunkerParams,
    hash_length: u32,
}

impl<O> ArchiveOutput<O>
where
    O: Write + Seek,
{
    pub fn new(file: O, archive: &ArchiveReader, chunker_params: ChunkerParams) -> Self {
        let chunk_data_offset = 4 // file magic
                + 1 // archive version
                + 8 // dictionary size
                + u64::from(archive::max_dictionary_size(
                    archive.total_chunks(),
                    archive.unique_chunks(),
                    archive.hash_length,
                )) // dictionary
                + 8 // chunk data offset
            + 64; // header checksum

        ArchiveOutput {
            file,
            compression: archive.chunk_compression,
            chunk_map: HashMap::new(),
            chunk_descriptors: Vec::new(),
            rebuild_order: archive.source_rebuild_order(),
            source_total_size: archive.source_total_size,
            source_checksum: archive.source_checksum.clone(),
            hash_length: archive.hash_length as u32,
            chunker_params,
            chunk_data_offset,

            // Reserve some space in the file for the archive header
            current_archive_offset: 0,
        }
    }

    pub fn write_header(&mut self) -> Result<()> {
        // Write the archive header to file
        let file_header = chunk_dictionary::ChunkDictionary {
            rebuild_order: self
                .rebuild_order
                .iter()
                .map(|hash| self.chunk_map[hash] as u32)
                .collect(),
            application_version: crate::PKG_VERSION.to_string(),
            chunk_descriptors: RepeatedField::from_vec(
                self.chunk_descriptors
                    .iter()
                    .map(|cd| (*cd).clone().into())
                    .collect(),
            ),
            source_checksum: self.source_checksum.clone(),
            chunk_compression: SingularPtrField::some(self.compression.into()),
            source_total_size: self.source_total_size,
            chunker_params: SingularPtrField::some(chunk_dictionary::ChunkerParameters {
                chunk_filter_bits: self.chunker_params.filter_bits,
                min_chunk_size: self.chunker_params.min_chunk_size as u32,
                max_chunk_size: self.chunker_params.max_chunk_size as u32,
                hash_window_size: self.chunker_params.buzhash.window_size() as u32,
                chunk_hash_length: self.hash_length,
                unknown_fields: std::default::Default::default(),
                cached_size: std::default::Default::default(),
            }),
            unknown_fields: std::default::Default::default(),
            cached_size: std::default::Default::default(),
        };
        let header_buf = archive::build_header(&file_header, Some(self.chunk_data_offset))?;
        println!("Header size: {}", header_buf.len());

        if header_buf.len() as u64 >= self.chunk_data_offset {
            panic!(
                "Bug: Header ({}) is bigger than the calculated max: {}",
                size_to_str(header_buf.len()),
                size_to_str(self.chunk_data_offset)
            );
        }

        self.file
            .seek(SeekFrom::Start(0))
            .chain_err(|| "failed to seek")?;;
        self.file
            .write_all(&header_buf)
            .chain_err(|| "failed to write header")?;
        Ok(())
    }
}

impl<O> CloneOutput for ArchiveOutput<O>
where
    O: Write + Seek,
{
    fn write_chunk(&mut self, checksum: &[u8], _offsets: &[u64], chunk_data: &[u8]) -> Result<()> {
        match self.chunk_map.entry(checksum.to_vec()) {
            Entry::Occupied(_desc) => {
                // Chunk already written to archive
            }
            Entry::Vacant(v) => {
                // Compress the chunk, write it to archive and store a chunk descriptor
                let archive_data = self.compression.compress(chunk_data)?;
                v.insert(self.chunk_descriptors.len());
                self.chunk_descriptors.push(archive::ChunkDescriptor {
                    checksum: checksum.to_vec(),
                    archive_size: archive_data.len() as u32,
                    archive_offset: self.current_archive_offset,
                    source_size: chunk_data.len() as u32,
                });
                self.file
                    .seek(SeekFrom::Start(
                        self.chunk_data_offset + self.current_archive_offset,
                    ))
                    .chain_err(|| "failed to seek")?;
                self.file
                    .write_all(&archive_data)
                    .chain_err(|| "failed to write")?;
                self.current_archive_offset += archive_data.len() as u64;
            }
        }

        Ok(())
    }
}
