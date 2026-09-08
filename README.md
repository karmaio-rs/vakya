# Vakya

Vakya is an asynchronous HTTP library for Rust, built specifically for the [Karmaio](https://github.com/karmaio-rs/karmaio) completion-based runtime.

The goal is to provide low-level HTTP building blocks for clients and servers while taking advantage of completion-based I/O, owned buffers, and multishot operations rather than adapting to a readiness-based runtime model.

## Goals

- HTTP/1 client and server connections, with HTTP/2 and HTTP/3 planned later
- Streaming request and response bodies
- Transport handoff for upgrades and tunnels
- Completion-native I/O and buffer handling
- Tight integration with Karmaio
- Small, low-level API suitable for building servers, clients, proxies, and web frameworks

## Status

Vakya is currently in early development and is not ready for production use.

The current implementation contains the crate foundation. HTTP connection APIs are not available yet.
Rust 1.95 or newer and edition 2024 are required.

No features are enabled by default. `client` and `server` each enable `http1`;
`tls` enables Karmaio's provider-neutral Rustls integration; `tracing` enables instrumentation support.
`full` selects all these features. Applications select their TLS crypto provider and trust configuration through Karmaio.

## License

Licensed under either the Apache License, Version 2.0 or the MIT License, at your option.
