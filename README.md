# Vakya

Vakya is an asynchronous HTTP library for Rust, built specifically for the [Karmaio](https://github.com/) completion-based runtime.

The goal is to provide low-level HTTP building blocks for clients and servers while taking advantage of completion-based I/O, owned buffers, and multishot operations rather than adapting to a readiness-based runtime model.

## Goals

- HTTP/1.0, HTTP/1.1 and HTTP/2 client and server support
- Streaming request and response bodies
- WebSocket support
- Completion-native I/O and buffer handling
- Tight integration with Karmaio
- Small, low-level API suitable for building servers, clients, proxies, and web frameworks

## Status

Vakya is currently in early development and is not ready for production use.

## License

Licensed under either the Apache License, Version 2.0 or the MIT License, at your option.
