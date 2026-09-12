# Vakya

Vakya is a low-level HTTP library for Rust, built for the [Karmaio](https://github.com/karmaio-rs/karmaio) completion-based runtime. It provides caller-driven HTTP/1 connections, streaming bodies, informational responses, protocol upgrades and CONNECT handoff, shutdown controls, and TLS integration.

Applications provide established transports and own dialing, listeners, task supervision, TLS configuration, connection pools, and retries. Vakya handles HTTP framing and connection execution.

## Getting started

Rust **1.95** or newer is required.

```toml
[dependencies]
vakya = { version = "0.1.0", features = ["client", "server"] }
karmaio = { version = "0.1.0", default-features = false, features = ["bytes", "net"] }
```

No Vakya features are enabled by default. Bodies, body utilities, common HTTP types, and the native `Service` trait remain available without a connection role.

| Feature | Enables |
| --- | --- |
| `http1` | HTTP/1 protocol implementation and connection outcome/handoff types |
| `client` | HTTP/1 client connection API; implies `http1` |
| `server` | HTTP/1 server connection API; implies `http1` |
| `tls` | Integration with established Karmaio Rustls streams; no role or crypto provider selected |
| `tracing` | Connection/exchange spans, lifecycle, status, I/O progress, and error categories |
| `full` | `client`, `server`, `tls`, and `tracing` |

The examples are complete programs:

- [Client](examples/client.rs): supply a socket, drive HTTP concurrently, consume a response, and supervise shutdown.
- [Server](examples/server.rs): accept a socket and supply an async service.
- [Streaming echo](examples/streaming_echo.rs): return the incoming request body as the response body.
- [Body utilities](examples/body_utilities.rs): compose bodies and collect them with explicit limits.
- [Informational responses](examples/informational.rs): send heads through the request context.
- [Transport handoff](examples/upgrade.rs): continue application I/O after HTTP settles.
- [HTTPS client](examples/https_client.rs) and [HTTPS server](examples/https_server.rs): configure a provider and certificates in the application, perform TLS through Karmaio, then give the stream to Vakya.

For the plain HTTP examples, run `cargo run --example server --features server` and, in another terminal, `cargo run --example client --features client`.

## Connections and ownership

`client::conn::http1::Builder::handshake` returns a typed `SendRequest<B>` and a connection driver. `server::conn::http1::Builder::serve_connection` returns a driver for a service receiving `(Request<Incoming>, RequestContext)`. These constructors perform no dialing, accepting, or background spawning. The caller must drive `connection.run()` concurrently with application work.

Generic constructors accept independently splittable Karmaio transports and use portable reads. The explicit `handshake_tcp` and `serve_tcp` constructors select Linux managed receive where applicable. That path requires Linux **6.12+** with io_uring enabled. Retained payload leases and partial parser prefixes use bounded portable fallback; other platforms use portable reads. Managed leases remain an internal optimization.

Bodies use owned buffers and native async methods. Vakya does not require application bodies or services to be `Send` or `Sync`. Submitted data stays owned by the write path until completion; only recovered data is passed to the producer's `recycle` callback. Cloned and sliced incoming data remains valid independently of its body or connection.

Client admission permits one active exchange or unsubmitted permit, plus one waiting reservation. A rejected submission returns the original request. Once accepted, the request belongs to the driver and an error does not establish retry safety. Early final responses do not cancel uploads. Dropping `PendingResponse` before final delivery abandons the exchange; after delivery, `Incoming` controls receiving. `UploadControl::abort` explicitly stops an unfinished upload and retires the HTTP/1 connection.

Dropping an unfinished incoming body abandons receiving. `Incoming::drain(limit)` opts into bounded discard when reuse is desired: an explicit payload limit, an additional 64 KiB framing allowance, and a five-second total deadline. Reuse still requires the other direction to settle successfully.

## Shutdown, deadlines, and handoff

`connection.control()` provides graceful shutdown, immediate abort, and graceful shutdown with an absolute deadline. Graceful shutdown stops new admission and lets accepted work finish. Abort cancels retained transport operations and drives them to settlement. Continue driving `run()` to observe completion; dropping a driver preserves Karmaio buffer safety but promises neither graceful flushing nor producer recycling. Custom transports must cooperate with Karmaio cancellation.

The default server head timeout is 30 seconds, including idle keep-alive, and can be disabled. Client head, body-progress, and write-progress timeouts are disabled by default. Body-progress timeouts apply to demanded reads; write-progress timeouts exclude waiting for an application body producer. Total head deadlines do not reset when bytes arrive. Timeout setters return `Result<&mut Self, Error>` and reject unrepresentable durations without changing the previous setting; handle their results with `?` or an explicit configuration error policy.

`run()` returns `ConnectionOutcome::Closed` or a typed `Upgraded<R, W>`. Validated 101 and successful CONNECT responses transfer transport ownership only after HTTP activity settles. Upgraded reads serve retained read-ahead first. Karmaio's `IntoOwnedSplit` returns an `UpgradedReadHalf<R>` that preserves this prefix and the original write half, allowing another HTTP or TLS connection over the tunnel. If extracting raw halves with `into_parts`, consume the returned read-ahead before reading the transport.

## TLS

With `tls`, `handshake_tls` and `serve_tls` accept already-handshaken Karmaio TLS streams. They accept absent ALPN or `http/1.1`, attach a `TlsInfo` snapshot to received message extensions, and use portable decrypted reads. Handoff preserves the concrete TLS halves and their encryption.

Applications choose providers, trust anchors, certificates, peer names, and TLS establishment deadlines. The production `tls` feature selects no crypto provider or trust store. Tests and examples explicitly enable Ring through a development dependency. Generic connection constructors require callers to select a compatible protocol themselves; use the TLS-specific constructors for ALPN validation and metadata.

## License

Licensed under either the Apache License, Version 2.0 or the MIT License, at your option.

With `tracing`, Vakya emits connection and exchange spans at trace level, with a connection ID, role, protocol, and exchange number. I/O events report operation and completed byte count; response events report status. Error categories are emitted at debug level. Applications install and configure their own subscriber. Headers, URIs, and payload contents are never recorded. Instrumentation is compiled out when the feature is disabled.
