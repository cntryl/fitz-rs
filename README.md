# cntryl-rs: Fitz Client Library

A high-performance, fully type-safe Rust client library for the [Fitz](../README.md) distributed actor broker.

## Features

- ✅ **Zero coupling to server**: Imports only from standard Rust crates and internal modules
- ✅ **Multi-transport**: Supports TCP and WebSocket equally with runtime selection
- ✅ **All 7 domains**: Full implementation for KV, Queue, Notice, RPC, Lease, Stream, and Schedule
- ✅ **Transaction support**: Full ACID transaction semantics for KV domain
- ✅ **Authentication helpers**: Built-in JWT token generation for valid auth plus anonymous connect support
- ✅ **Synchronous API**: Blocking API over async tokio runtime (no callback hell)
- ✅ **Focused domain helpers**: Small blocking clients for each broker subsystem

## Quick Start

### Add to Cargo.toml

```toml
[dependencies]
cntryl = { path = "./cntryl-rs" }
```

### Basic Usage: TCP

```rust
use cntryl::{FitzClient, protocol::TransactionMode};

// Connect to Fitz server
let client = FitzClient::connect_tcp(
    "127.0.0.1",
    4091,
    "my-realm",
    "shared-secret"
)?;

// Get KV client
let kv = client.kv();

// Begin transaction on a fully qualified Fitz route
let tx = kv.begin("kv://my-realm/app/users", TransactionMode::ReadWrite)?;

// Put / Get / Delete
tx.put(b"alice", b"Alice Johnson")?;
let value = tx.get(b"alice")?.expect("not found");
tx.delete(b"alice")?;

// Commit or rollback
tx.commit()?;
// or: tx.rollback()?;

// Close connection
client.close()?;
```

### Using WebSocket Instead

```rust
// Same API, just swap the connect method
let client = FitzClient::connect_ws(
    "ws://127.0.0.1:4090/ws",
    "my-realm",
    "shared-secret"
)?;

// Everything else is identical - transport is abstracted
let kv = client.kv();
let tx = kv.begin("kv://my-realm/app/users", TransactionMode::ReadWrite)?;
// ...
```

## Documentation

- [docs/README.md](docs/README.md)
- [docs/GRADING.md](docs/GRADING.md)
- [CLIENT_SPEC.md](CLIENT_SPEC.md)
- [CLIENT_ACCEPTANCE_CRITERIA.md](CLIENT_ACCEPTANCE_CRITERIA.md)

Canonical Fitz client behavior is defined in the server repository under [fitz/docs/clients](../fitz/docs/clients).

## Architecture

### Transport Abstraction

The library uses a **trait-based transport abstraction**, allowing both TCP and WebSocket to be used interchangeably:

```
FitzClient
    └─ Arc<Mutex<FitzConnection>>
           └─ AnyTransport (enum)
                  ├─ Tcp (TcpTransport)
                  └─ WebSocket (WebSocketTransport)
```

### Wire Protocol

All communication uses **TLV (Tag-Length-Value) encoding**:

- **Message frames**: `[u32 BE length][u8 msg_type or u16 msg_type][tlv payload]`
- **TCP**: Length-prefixed frames
- **WebSocket**: Binary message per frame
- **Authentication**: HS256 JWT in CONNECT message

### Message Types (By Domain)

| Domain | Range | Constants |
|--------|-------|-----------|
| KV | 100-199 | `BEGIN`, `GET`, `PUT`, `DELETE`, `COMMIT`, `ROLLBACK` |
| Queue | 200-299 | `ENQUEUE`, `RESERVE`, `EXTEND`, `COMPLETE`, `SUBSCRIBE`, `NOTIFY` |
| RPC | 300-399 | `SUBSCRIBE`, `UNSUBSCRIBE`, `REQUEST`, `RESPONSE`, `ACK` |
| Lease | 400-499 | `ACQUIRE`, `RENEW`, `RELEASE`, `QUERY` |
| Notice | 500-599 | `PUBLISH`, `SUBSCRIBE`, `UNSUBSCRIBE`, `UNSUBSCRIBE_ALL`, `NOTIFY` |
| Stream | 600-699 | `BEGIN`, `APPEND`, `COMMIT`, `ROLLBACK`, `READ`, `SUBSCRIBE`, `NOTIFY` |
| Schedule | 700-799 | `CREATE`, `CANCEL`, `LIST`, `SUBSCRIBE`, `NOTIFY` |

## Domain Clients

### KV (Key-Value)

Full transaction support with ACID semantics:

```rust
let kv = client.kv();
let tx = kv.begin("kv://my-realm/app/users", TransactionMode::ReadWrite)?;

tx.put(key, value)?;           // Write value
let v = tx.get(key)?;           // Read value (Option<Vec<u8>>)
tx.delete(key)?;               // Delete value

tx.commit()?;                  // Commit all changes
// or
tx.rollback()?;                // Discard all changes
```

**Features:**
- Read-Write and Read-Only modes
- Transaction isolation
- Rollback support

### Queue

```rust
let queue = client.queue();
queue.enqueue("queue://my-realm/jobs/email", message, None)?;

let items = queue.reserve("queue://my-realm/jobs/email", 30, Some(1), Some(5))?;
if let Some(item) = items.first() {
    item.complete()?;
}
```

### Notice (Pub/Sub)

```rust
let notice = client.notice();
notice.publish("notice://my-realm/app/events", message)?;

let subscription = notice.subscribe("notice://my-realm/app/*")?;
let msg = subscription.next()?;
println!("Received notice on {}", msg.route);
```

### RPC (Remote Procedure Call)

```rust
let rpc = client.rpc();

let mut responses = rpc.call("rpc://my-realm/app/echo", b"ping")?;
while let Some(frame) = responses.next()? {
    println!("rpc frame {}: {} bytes", frame.sequence, frame.body.len());
}

let worker = rpc.register_worker("rpc://my-realm/app/echo")?;
let mut request = worker.next()?;
request.respond(request.body.as_slice(), true)?;
```

### Lease (Distributed Locks)

```rust
let lease = client.lease();
let grant = lease.acquire("lease://my-realm/locks/leader", "node-1", 30)?;
let renewed = lease.extend("lease://my-realm/locks/leader", "node-1", grant.fencing_token, 30)?;
lease.release("lease://my-realm/locks/leader", "node-1", renewed.fencing_token)?;
```

### Stream (Named Channels)

```rust
let stream = client.stream();
let mut session = stream.begin("stream://my-realm/orders/events", 0, None)?;
let discriminator = cntryl::domains::stream::StreamDiscriminator::from("proj.alpha");
session.append(0, b"created", None, Some(&discriminator))?;
session.commit(cntryl::domains::stream::StreamCommitMode::Sync)?;

let records = stream.read(
    "stream://my-realm/orders/events",
    0,
    100,
    None,
    Some(&cntryl::domains::stream::StreamFilterSet {
        clauses: vec![cntryl::domains::stream::StreamFilterClause::Equals("proj.alpha".to_string())],
    }),
)?;
let last = stream.peek("stream://my-realm/orders/events")?;
let metadata = stream.metadata("stream://my-realm/orders/events")?;

let subscription = stream.subscribe("stream://my-realm/orders/events")?;
let notification = subscription.next()?;
println!("{} {}", notification.route, notification.event);
```

### Schedule (Job Scheduling)

```rust
let schedule = client.schedule();
let schedule_id = schedule.create("schedule://my-realm/app/orders/run", "*/5 * * * *", b"sync")?;
let (_entries, total) = schedule.list(None, Some(100))?;
schedule.cancel(&schedule_id)?;
```

## Testing

### Unit Tests

```bash
cargo test --lib
```

### Integration Tests (Requires Running Server)

```bash
# Full library and integration suite
cargo test --lib --tests

# Targeted end-to-end suites
cargo test --test integration_rpc -- --nocapture
cargo test --test integration_stream -- --nocapture
```

### Conformance Matrix

The ignored conformance runner mirrors the shared `CS-001` through `CS-015` suite and writes normalized JSON artifacts.

```bash
cargo test --test conformance -- --ignored --nocapture
CONFORMANCE_TRANSPORT=ws CONFORMANCE_AUTH_MODE=valid_jwt cargo test --test conformance -- --ignored --nocapture
```

Default output path:

```text
./artifacts/conformance-results.json
```

To run integration tests, start the Fitz server first:

```bash
# In the fitz root directory
cargo run -F boot
```

Server will listen on:
- **TCP**: `127.0.0.1:4091`
- **WebSocket**: `ws://127.0.0.1:4092/fitz`

## Error Handling

All operations return `Result<T, FitzError>`:

```rust
use cntryl::error::FitzError;

match client.kv().begin("app", "data", mode) {
    Ok(tx) => { /* use transaction */ },
    Err(FitzError::Connection(msg)) => { /* connection error */ }
    Err(FitzError::Protocol(msg)) => { /* protocol error */ }
    Err(FitzError::Transport(msg)) => { /* transport I/O error */ }
    Err(FitzError::Timeout) => { /* operation timeout */ }
    Err(e) => { /* other error */ }
}
```

## Performance

The library is optimized for:

1. **Minimal allocations** in hot paths
2. **Zero-copy** frame handling where possible
3. **Connection pooling** ready (implement via Arc<Client>)
4. **Synchronous blocking API** to avoid scheduler overhead

For async use, `block_in_place` integrates cleanly with tokio:

```rust
// Inside async context
let result = tokio::task::block_in_place(|| {
    client.kv().begin("app", "data", mode)
})?;
```

## Troubleshooting

### Connection Refused

- Verify Fitz server is running: `cargo run -F boot` in repo root
- Check host/port: defaults are `127.0.0.1:4091` (TCP), `ws://127.0.0.1:4092/fitz` (WS)
- Check realm matches between client and server expectations

### Authentication Failed

- Secret key must match server configuration
- JWT token generation uses HS256, verify algorithm compatibility

### Transaction Errors

- Check `TransactionMode`: `ReadWrite` for modifications, `ReadOnly` for reads
- Verify resource exists (server creates on demand)
- Rollback on error; don't reuse failed transaction

### Frame Too Large

- KV payloads are limited by frame size; split large operations
- Individual put/get is not constrained by individual key size but by maximum frame size

## Architecture Decisions

### Why Sync API Over Async?

The client provides a **synchronous blocking API** despite using tokio internally, because:

1. **Determinism**: No scheduler jitter for latency-sensitive operations
2. **Simplicity**: Synchronous code is easier to reason about and test
3. **Interop**: Works in both sync and async contexts (via `block_in_place`)
4. **Performance**: Reduces task overhead for high-throughput scenarios

### Why Transport Trait?

The `Transport` trait abstraction enables:

1. **Protocol flexibility**: Easy to add gRPC, HTTP/3, or other protocols
2. **Testing**: Mock transport for unit tests without server
3. **Runtime selection**: Choose TCP or WebSocket based on environment/config
4. **Zero coupling**: Transport layer independent of domain logic

### Why Zero Server Coupling?

This design enables:

1. **Standalone client library**: Can be distributed independently
2. **Version compatibility**: Server updates don't break client compilation
3. **Unit testing**: Tests don't depend on server internals
4. **Reusability**: Client works with compatible server implementations

## Contributing

See [CONTRIBUTING.md](../CONTRIBUTING.md) for guidelines.

## License

Same as Fitz: See [LICENSE](../LICENSE)
