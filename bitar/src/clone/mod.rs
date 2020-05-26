//! Clone helpers.
mod from_archive;
mod from_readable;
mod in_place;
mod output;

pub use from_archive::{from_archive, CloneFromArchiveError};
pub use from_readable::{from_readable, CloneFromReadableError};
pub use in_place::{in_place, CloneInPlaceError};
pub use output::CloneOutput;

/// Clone options.
#[derive(Default, Clone)]
pub struct Options {
    pub max_buffered_chunks: usize,
}

impl Options {
    /// Set the maximum number of chunk buffers to use while cloning.
    ///
    /// 0 will result in an automatically selected value.
    pub fn max_buffered_chunks(mut self, num: usize) -> Self {
        self.max_buffered_chunks = num;
        self
    }
    pub(crate) fn get_max_buffered_chunks(&self) -> usize {
        if self.max_buffered_chunks == 0 {
            // Single buffer if we have a single core, otherwise number of cores x 2
            match num_cpus::get() {
                0 | 1 => 1,
                n => n * 2,
            }
        } else {
            self.max_buffered_chunks
        }
    }
}
