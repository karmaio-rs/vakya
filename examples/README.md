# Examples of using Vakya

These examples show how to do common tasks using Vakya.

If you check out this repository, you can run any of the examples with:

```
cargo run --example {example_name} --features {features}
```

Pass program arguments after `--`. Applications own listening, dialing, name resolution, TLS, and task supervision; Vakya frames HTTP over the transport you supply.

## Getting Started

### Clients

* [`client`](client.rs) - A simple CLI HTTP client that requests the URL passed as an argument and prints the response status, headers, and body, reading content chunk-by-chunk.

### Servers

* [`hello`](hello.rs) - A simple server that returns "Hello World!".

* [`echo`](echo.rs) - An echo server that copies POST request content to the response, with streaming uppercase and bounded reverse routes.

## Going Further

* [`client_json`](client_json.rs) - GET JSON, collect the body, parse it with serde, and print the result.

* [`gateway`](gateway.rs) - A reverse proxy that forwards to the `hello` service above.

* [`graceful_shutdown`](graceful_shutdown.rs) - A server that times out incoming connections and shuts them down through `connection.control()`.

* [`http_proxy`](http_proxy.rs) - A simple HTTP proxy that forwards ordinary requests and tunnels `CONNECT`.

* [`params`](params.rs) - A server that accepts a form with a name and a number, checks that the fields are present, and validates the input.

* [`service_struct_impl`](service_struct_impl.rs) - A struct that implements the `Service` trait and shares a counter across requests.

* [`state`](state.rs) - Shared `Rc`/`Cell` state across requests. Vakya does not require `Send` application state.

* [`upgrades`](upgrades.rs) - A server and client demonstrating HTTP upgrades.

* [`web_api`](web_api.rs) - A JSON API and a route that calls that API over a client connection.

## Vakya-specific

These examples cover APIs that are specific to Vakya. TLS is application-owned: Vakya does not pick a crypto provider or trust store.

* [`informational`](informational.rs) - Send HTTP 103 Early Hints from the request context before the final response.

* [`https_client`](https_client.rs) - Configure Rustls, dial, complete TLS in Karmaio, then hand the stream to Vakya.

* [`https_server`](https_server.rs) - Accept TCP, complete TLS in Karmaio, then serve HTTP/1.

* [`body_utilities`](body_utilities.rs) - Compose bodies, collect them with explicit limits, and erase them locally.
