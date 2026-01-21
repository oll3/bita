# Changelog

All notable changes to the bitar library will be documented in this file.

## [Unreleased]

## [0.14.0]

- Remove use of unsafe code
- Make rustls the default TLS backend
- Update dependencies
- Set MSRV to 1.83.0


## [0.13.0]

- Add metadata key-value pair embedding in archive dictionary
- Update dependencies (brotli, prost)
- Bump MSRV to 1.71.1

## [0.12.0]

- Fail gracefully on unexpected end of archive
- Bump reqwest to 0.12; disable HTTP/2 support
- Regenerate protobuf code on-demand
- Set MSRV to 1.70
- Add PartialEq, Eq, and Hash traits to chunker types

## [0.11.0]

- Automate binary release builds and publishing
- Fix macOS compatibility issue
- Establish MSRV at 1.61
