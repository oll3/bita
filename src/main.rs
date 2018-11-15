extern crate bincode;
extern crate getopts;
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
mod ordered_mpsc;
mod string_utils;

use getopts::Options;
use std::env;
use std::process;
use threadpool::ThreadPool;

use config::*;

fn print_usage(program: &str, opts: Options, compress_opts: Options) {
    let brief = format!("Usage:\n");
    let brief = brief + &format!("    {} [options] compress [PATH]\n", program);
    let brief = brief + &format!("    {} [options] unpack [PATH/URL] [PATH]", program);
    println!();
    println!("{}", opts.usage(&brief));
    println!();
    println!(
        "{}",
        compress_opts.usage_with_format(|opts| {
            let mut result = "Options for compress:\n".to_string();
            opts.for_each(|opt| result += &format!("{}\n", opt));
            result
        })
    );
    println!("    SIZE can be given in units 'B' (default), 'KiB', 'MiB', 'GiB', and 'TiB'.");
    println!();
}

fn parse_size(size_str: &str) -> usize {
    let size_val: String = size_str.chars().filter(|a| a.is_numeric()).collect();
    let size_val: usize = size_val.parse().expect("parse");
    let size_unit: String = size_str.chars().filter(|a| !a.is_numeric()).collect();
    if size_unit.len() == 0 {
        return size_val;
    }
    match size_unit.as_str() {
        "TiB" => 1024 * 1024 * 1024 * 1024 * size_val,
        "GiB" => 1024 * 1024 * 1024 * size_val,
        "MiB" => 1024 * 1024 * size_val,
        "KiB" => 1024 * size_val,
        "B" => size_val,
        _ => panic!("Invalid size unit"),
    }
}

fn parse_opts() -> Config {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();
    let mut base_opts = Options::new();
    let mut compress_opts = Options::new();

    base_opts.optflag("h", "help", "print command help");
    base_opts.optflag("f", "force-create", "overwrite output files if they exist");

    compress_opts.optopt("i", "input", "file to digest (defaults to stdin)", "FILE");
    compress_opts.optopt(
        "t",
        "hash-lenght",
        "truncate the length of the stored strong hash",
        "LENGTH",
    );

    let base_matches = match base_opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => panic!(f.to_string()),
    };
    if base_matches.opt_present("h") || base_matches.free.len() < 1 {
        print_usage(&program, base_opts, compress_opts);
        process::exit(1);
    }

    let force_create = base_matches.opt_present("f");
    let command = base_matches.free[0].clone();

    // Remove the command from the args array when parsing further
    let args: Vec<String> = args.into_iter().filter(|arg| *arg != command).collect();

    let base_config = BaseConfig {
        force_create: force_create,
    };

    match command.as_str() {
        "compress" => {
            // Parse compress related options

            let compress_matches = match compress_opts.parse(&args[1..]) {
                Ok(m) => m,
                Err(f) => panic!(f.to_string()),
            };
            let input = compress_matches.opt_str("i");
            let truncate_hash: Option<usize> = if let Some(ref val) = compress_matches.opt_str("t")
            {
                Some(val.parse().expect("LENGTH"))
            } else {
                None
            };

            if compress_matches.free.len() < 1 {
                println!("Missing output file");
                print_usage(&program, base_opts, compress_opts);
                process::exit(1);
            }
            let output_file = compress_matches.free[0].clone();
            Config::Compress(CompressConfig {
                base: base_config,
                input: input,
                output: output_file,
                truncate_hash: truncate_hash,
            })
        }
        _ => {
            println!("Unknown command '{}'", command);
            print_usage(&program, base_opts, compress_opts);
            process::exit(1);
        }
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
