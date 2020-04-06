use bitar;
use bitar::{clone_from_archive, clone_from_readable, Archive, CloneOptions};
use tokio;
use tokio::fs::{File, OpenOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output_name = "sickan.jpg";
    let input_path = "examples/resources/example-archive.cba";
    let example_seed = "examples/resources/example.seed";

    // Open archive which source we want to clone
    let mut archive_file = File::open(input_path).await?;
    let archive = Archive::try_init(&mut archive_file).await?;

    // Create output to contain the clone oof the archive source
    let mut output = OpenOptions::new()
        .create(true)
        .write(true)
        .open(output_name)
        .await?;

    // Get a list of all chunks needed to create the clone of the archive source
    let mut chunks_to_clone = archive.source_index().clone();

    // Use as much data as possible from the example seed
    let read_seed_bytes = clone_from_readable(
        &CloneOptions::default(),
        &mut OpenOptions::new().read(true).open(example_seed).await?,
        &archive,
        &mut chunks_to_clone,
        &mut output,
    )
    .await?;

    // Fetch the rest of the chunks from the archive
    let read_archive_bytes = clone_from_archive(
        &CloneOptions::default(),
        &mut archive_file,
        &archive,
        &mut chunks_to_clone,
        &mut output,
    )
    .await?;

    println!(
        "Cloned {} to {} using {} bytes from {} and {} bytes from archive",
        input_path, output_name, read_seed_bytes, example_seed, read_archive_bytes
    );
    Ok(())
}
