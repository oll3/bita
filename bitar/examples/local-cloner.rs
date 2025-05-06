use bitar::{archive_reader::IoReader, Archive, CloneOutput};
use futures_util::{StreamExt, TryStreamExt};
use tokio::fs::{File, OpenOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output_name = "sickan.jpg";
    let input_path = "examples/resources/example-archive.cba";
    let example_seed = "examples/resources/example.seed";

    // Open archive which source we want to clone
    let mut archive = Archive::try_init(IoReader::new(File::open(input_path).await?)).await?;

    // Create output to contain the clone of the archive's source
    let mut output = CloneOutput::new(
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(output_name)
            .await
            .expect("open output"),
        // Get a list of all chunks needed to create the clone
        archive.build_source_index(),
    );

    // Use as much data as possible from the example seed
    let mut read_seed_bytes = 0;
    let mut chunk_stream = archive
        .chunker_config()
        .new_chunker(OpenOptions::new().read(true).open(example_seed).await?)
        .map_ok(|(_offset, chunk)| chunk.verify());
    while let Some(result) = chunk_stream.next().await {
        let verified = result?;
        read_seed_bytes += output.feed(&verified).await?;
    }

    // Fetch the rest of the chunks from the archive
    let mut chunk_stream = archive.chunk_stream(output.chunks());
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
        input_path, output_name, read_seed_bytes, example_seed, read_archive_bytes
    );
    Ok(())
}
