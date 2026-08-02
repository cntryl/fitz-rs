# cntryl-fitz

The Rust SDK for [Fitz](https://github.com/cntryl/fitz). The crate uses the
`cntryl_fitz` import name, treats tokens and routes as opaque values, and
supports TCP and binary WebSocket transports.

```toml
[dependencies]
cntryl-fitz = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust,no_run
use cntryl_fitz::{Client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::builder("tcp://127.0.0.1:4091", || async {
        // Fetch a fresh token here. The provider is called again on reconnect.
        Ok(std::env::var("FITZ_TOKEN").unwrap_or_default())
    })
    .build()?;

    client.connect().await?;
    println!("state: {:?}", client.state());
    client.close().await?;
    Ok(())
}
```

For a broker that permits anonymous sessions, use
`Client::anonymous("ws://127.0.0.1:4190/ws")`.

The canonical protocol, acceptance criteria, and cross-language scenarios live
in the Fitz server repository under `docs/clients`. Production code in this
crate never creates or inspects JWTs.

## Subscription registrations

All domain operations are async. Domain accessors such as `client.kv()` and
`client.notice()` return typed clients, while subscription and RPC worker
handles implement `futures_core::Stream`. Wire registration IDs remain private
and are replaced transparently after reconnect. Dropping a one-shot future is
cancellation-safe; slow bounded streams terminate with a typed backpressure
error instead of stalling the receive loop.

KV, Queue, Stream, Notice, RPC worker, and Schedule registrations accept exact
routes and whole-segment `*` or `**` patterns, including wildcard realms. KV,
Queue, and Stream patterns must be capable of matching three segments;
Schedule patterns must match four; Notice and RPC have flexible depth. The
broker permits 128 wildcard registrations per domain and session, while exact
registrations do not consume the quota. Lease subscriptions accept only an
exact `lease://realm/area/resource` route.

Notifications expose the exact concrete route. Queue availability
notifications additionally report ready, delayed, and inflight message counts.

## Local broker

```console
docker compose up -d
```

This starts `ghcr.io/cntryl/fitz:latest` as an authenticated broker on
`127.0.0.1:4090/4091` and an anonymous broker on `127.0.0.1:4190/4191`.
Both are loopback-only and use local storage volumes. The development JWT
secret is `dev-test-secret`, the audience is `fitz`, and tenant `dev` maps to
identity `1`.

Run the broker-backed tests explicitly:

```console
cargo test --features legacy-blocking --test integration_kv_tcp --test integration_kv_websocket --test integration_domains --test integration_multiprotocol --test integration_rpc --test integration_stream -- --ignored
CONFORMANCE_TRANSPORT=tcp CONFORMANCE_AUTH_MODE=anonymous cargo test --test conformance -- --nocapture
docker compose down --volumes
```

The broker-backed tests cover KV, Queue, RPC, Lease, Notice, Stream, and
Schedule lifecycles. The conformance runner covers shared scenarios
`CS-001` through `CS-017`, including a real relay-induced transport loss that
must recover on the same `Client` instance.

## Managed leases

`LeaseClient::with_lease` and `with_lease_with_options` supervise acquisition, renewal,
callback cancellation, and release without blocking Tokio executor threads. The low-level
API remains deliberately stateless: callers must replace the fencing token after every
successful `extend`, and must treat any uncertain renewal as ownership loss.

## Development

```console
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings -D clippy::pedantic
cargo test --locked --workspace --all-targets --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
cargo package --allow-dirty
```

Licensed under Apache-2.0.
