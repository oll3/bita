#!/bin/sh

set -e

cargo build

if [ "x${RUN_CLIPPY}" = "xtrue" ] ; then
    rustup component add clippy
    cargo clippy --all -- -D warnings
fi

if [ "x${RUN_RUSTFMT}" = "xtrue" ] ; then
    rustup component add rustfmt
    cargo fmt --all -- --check
fi

cargo test --workspace --verbose
