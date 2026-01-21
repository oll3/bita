# Build with rustls tls backend and ensure we're not linked with libssl
cargo build
ldd target/debug/bita | grep libssl -q && echo "!!! should NOT link with libssl !!!" && exit 1 || true
