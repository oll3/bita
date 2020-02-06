use prost_build;

fn main() {
    prost_build::compile_protos(&["proto/chunk_dictionary.proto"], &["proto/"]).unwrap();
}
