mod cli;
mod clone_cmd;
mod compress_cmd;
mod diff_cmd;
mod info_cmd;
mod string_utils;

use anyhow::{Context, Result};
use cli::parse_opts;
use cli::CommandOpts;
use cli::LogOpts;
use log::LevelFilter;

pub const PKG_NAME: &str = env!("CARGO_PKG_NAME");
pub const PKG_VERSION: &str = env!("CARGO_PKG_VERSION");

fn main() -> Result<()> {
    let (command_opts, log_opts) = parse_opts(std::env::args_os()).unwrap_or_else(|e| e.exit());
    init_log(log_opts)?;
    tokio::runtime::Runtime::new()?.block_on(async {
        match command_opts {
            CommandOpts::Compress(opts) => compress_cmd::compress_cmd(opts).await,
            CommandOpts::Clone(opts) => clone_cmd::clone_cmd(opts).await,
            CommandOpts::Info(opts) => info_cmd::info_cmd(opts).await,
            CommandOpts::Diff(opts) => diff_cmd::diff_cmd(opts).await,
        }
    })
}

fn init_log(log_opts: LogOpts) -> Result<()> {
    let local_level = log_opts.filter;
    fern::Dispatch::new()
        .format(move |out, message, record| {
            if local_level > LevelFilter::Info {
                // Add some extra info to each message in debug
                out.finish(format_args!(
                    "[{}]({})({}) {}",
                    chrono::Local::now().format("%Y-%m-%dT%H:%M:%S%.3f"),
                    record.target(),
                    record.level(),
                    message
                ))
            } else {
                out.finish(format_args!("{}", message))
            }
        })
        .level(log_opts.filter)
        .chain(std::io::stdout())
        .apply()
        .context("Unable to initialize log")?;
    Ok(())
}
