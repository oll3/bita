use bitar::{Archive, ChunkIndex, CloneOutput};
use futures_util::{StreamExt, TryStreamExt};
use tokio::fs::{File, OpenOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output_name = "sickan.jpg";
    let input_path = "examples/resources/example-archive.cba";

    // Open archive which source we want to clone
    let mut archive = Archive::try_init(File::open(input_path).await?).await?;

    // Create a file for clone output
    let mut output_file = OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .open(output_name)
        .await
        .expect("open output");

    // Scan the output file for chunks and build a chunk index
    let mut output_index = ChunkIndex::new_empty();
    {
        let chunker = archive.chunker_config().new_chunker(&mut output_file);
        let mut chunk_stream = chunker.map_ok(|(offset, chunk)| (offset, chunk.verify()));
        while let Some(r) = chunk_stream.next().await {
            let (offset, verified) = r?;
            let (hash, chunk) = verified.into_parts();
            output_index.add_chunk(hash, chunk.len(), &[offset]);
        }
    }

    // Create output to contain the clone of the archive's source
    let mut output = CloneOutput::new(output_file, archive.build_source_index());

    // Reorder chunks in the output
    let reused_bytes = output.reorder_in_place(output_index).await?;

    // Fetch the rest of the chunks from the archive
    let mut chunk_stream = archive.chunk_stream(&output.chunks());
    let mut read_archive_bytes = 0;
    while let Some(result) = chunk_stream.next().await {
        let compressed = result?;
        read_archive_bytes += compressed.len();
        let unverified = compressed.decompress()?;
        let verified = unverified.verify()?;
        output.feed(&verified).await?;
    }

    println!(
        "Cloned {} to {} using {} bytes from {} and {} bytes from archive",
        input_path, output_name, reused_bytes, output_name, read_archive_bytes
    );
    Ok(())
}
