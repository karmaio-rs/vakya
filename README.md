# Vakya

Vakya is a low-level HTTP library for Rust, built for the [Karmaio](https://github.com/karmaio-rs/karmaio) completion-based runtime.
It provides caller-driven client and server connections, streaming bodies, informational responses, protocol upgrades, graceful shutdown, and optional TLS integration.

Applications provide established transports and own dialing, listeners, task supervision, TLS configuration, connection pools, and retries. Vakya handles HTTP framing and connection execution.

> **Alpha:** Vakya is an early release. APIs and behavior may change between prereleases.

## What Vakya provides

- Completion-native I/O over Karmaio-owned buffers.
- HTTP/1 client and server connection drivers over established transports.
- Streaming request and response bodies, trailers, bounded collection, and body combinators.
- Informational responses, request admission, graceful shutdown, deadlines, protocol upgrades, and CONNECT handoff.
- Optional Karmaio Rustls integration for already-established TLS streams.
- Optional connection and exchange tracing without recording headers, URIs, or payload contents.

Vakya does not dial, listen, resolve names, establish TLS, spawn application tasks, pool connections, or retry requests.

## Getting started

Rust **1.95** or newer is required.

```toml
[dependencies]
vakya = { version = "0.1.0-alpha.1", features = ["client", "server"] }
karmaio = { version = "0.1.0", default-features = false, features = ["bytes", "net"] }
```

No Vakya features are enabled by default:

| Feature | Enables |
| --- | --- |
| `http1` | HTTP/1 protocol implementation and connection types |
| `client` | HTTP/1 client APIs; implies `http1` |
| `server` | HTTP/1 server APIs; implies `http1` |
| `tls` | Integration with established Karmaio Rustls streams |
| `tracing` | Connection and exchange instrumentation |
| `full` | `client`, `server`, `tls`, and `tracing` |

See the [API documentation](https://docs.rs/vakya) for the complete public surface.

## Platform notes

The explicit `handshake_tcp` and `serve_tcp` constructors use Linux managed receive where available; that path requires Linux **6.12+** with io_uring enabled.
The library does not perform any runtime checks, so it is up to you to ensure that the Linux kernel baseline is met.

## Examples

The repository contains complete programs for clients, servers, reverse proxying, JSON APIs, graceful shutdown, upgrades, informational responses, body utilities, and HTTPS.

Browse the [examples catalog](https://github.com/karmaio-rs/vakya/tree/main/examples).
From a checkout, run a plain HTTP server and client with:

```sh
cargo run --example hello --features server
cargo run --example client --features client -- http://127.0.0.1:3000/
```

## License

Licensed under either the Apache License, Version 2.0 or the MIT License, at your option.
