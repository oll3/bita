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

# Build with rustls tls backend and ensure we're not linked with libssl
cargo build --no-default-features --features rustls-tls
ldd target/debug/bita | grep libssl -q && echo "!!! should NOT link with libssl !!!" && exit 1 || true