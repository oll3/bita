[![CI](https://github.com/oll3/bita/workflows/CI/badge.svg)](https://github.com/oll3/bita/actions?query=workflow%3ACI)
[![crates.io](http://meritbadge.herokuapp.com/bitar)](https://crates.io/crates/bitar)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](../LICENSE)

## bitar

A library to use for low bandwidth file synchronization over HTTP.

* [Documentation](https://docs.rs/bitar)
* See [bita](https://github.com/oll3/bita) for a more detailed description of the synchronization process.


### Minimum Supported Rust Version (MSRV)

This crate is guaranteed to compile on stable rust 1.51 and up. It might compile with older versions depending
on features set but it may change in any new patch release.


### Usage

Examples of how to use *bitar* is available under the ![examples](examples) directory.
Also see [bita](https://github.com/oll3/bita) for more example usage.

```console
# Run example using cargo
olle@home:~/bita/bitar$ cargo run --example local-cloner
```