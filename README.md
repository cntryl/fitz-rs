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
`Client::anonymous("ws://127.0.0.1:4090/ws")`.

The canonical protocol, acceptance criteria, and cross-language scenarios live
in the Fitz server repository under `docs/clients`. Production code in this
crate never creates or inspects JWTs.

## Managed leases

`LeaseClient::with_lease` and `with_lease_with_options` supervise acquisition, renewal,
callback cancellation, and release without blocking Tokio executor threads. The low-level
API remains deliberately stateless: callers must replace the fencing token after every
successful `extend`, and must treat any uncertain renewal as ownership loss.

## Development

```console
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
cargo package --allow-dirty
```

Licensed under Apache-2.0.
