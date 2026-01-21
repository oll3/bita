# Changelog

All notable changes to the bita CLI will be documented in this file.

## [Unreleased]

## [0.14.0]

- Update dependencies
- Make rustls the default TLS backend
- Set MSRV to 1.83.0

## [0.13.0]

- Add ability to embed user-provided metadata in archives using `--metadata-value` and `--metadata-file` flags
- Extract metadata with `bita info --metadata-key`
- Fix zstd compression level range (allow 1-22)
- Android build fix

## [0.12.0]

- Update OpenSSL to 0.10.64
- Bump clap to 4.x
- Update MSRV to 1.74

## [0.11.0]

- Automate binary release builds and publishing
- Fix macOS compatibility issue
- Establish MSRV at 1.61
