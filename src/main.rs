extern crate bincode;
extern crate docopt;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate deflate;
extern crate num_cpus;
extern crate sha2;
extern crate threadpool;

mod buzhash;
mod chunker;
mod chunker_utils;
mod compress_cmd;
mod config;
mod file_format;
mod ordered_mpsc;
mod string_utils;

use std::env;
use std::process;
use threadpool::ThreadPool;

use config::*;
use docopt::Docopt;

const USAGE: &'static str = "
Usage:
  bita [options] compress [INPUT] <OUTPUT>
  bita [options] unpack <URL/FILE> [OUTPUT]

Generic options:
  -h, --help              Show this screen.
  -f, --force-create      Overwrite output files if they exist.

Compressor options:
  --hash-length LENGTH    Truncate the length of the stored strong hash [default: 64].
";

fn parse_opts() -> Config {
    let argv: Vec<String> = env::args().collect();

    let args = Docopt::new(USAGE)
        .and_then(|d| d.argv(argv.into_iter()).parse())
        .unwrap_or_else(|e| e.exit());

    if args.get_bool("compress") {
        let output = args.get_str("OUTPUT");
        let temp_file = output.to_string() + ".tmp";

        Config::Compress(CompressConfig {
            base: BaseConfig {
                force_create: args.get_bool("--force-create"),
            },
            input: args.get_str("INPUT").to_string(),
            output: output.to_string(),
            hash_length: args.get_str("--hash-length").parse().expect("LENGTH"),
            temp_file: temp_file,
        })
    } else {
        println!("Unknown command");
        process::exit(1);
    }
}

fn main() {
    //let config = parse_opts();
    let num_threads = num_cpus::get();
    let pool = ThreadPool::new(num_threads);

    match parse_opts() {
        Config::Compress(config) => compress_cmd::run(config, pool),
        _ => process::exit(1),
    }
}
