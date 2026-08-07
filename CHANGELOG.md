# Changelog

## 0.2.0 - Unreleased

- Breaking: Schedule listing uses canonical message 702 with offset/limit and `total_count`.
- Breaking: Queue delays are expressed in wire-shaped seconds, and Lease acquisition exposes `wait_seconds`.
- Breaking: Queue notifications expose the canonical opaque length-prefixed payload.
- Stream selectors accept the full documented matrix and global records expose their global offsets.
- Duplicate Notice, Schedule, and Stream subscriptions are locally reference counted.
- Reconnect generation fencing, best-effort registration restoration, refreshed-token auth retry, and cancellation-safe Lease cleanup prevent stale replay and leaked work.
- Breaking: Queue reserve, Stream read, and Stream last wire items now require their concrete matched route. `QueueItem`, `StreamReadItem`, and `StreamRecord` expose it.
- Breaking: `Client::connect()` is one-shot; use `connect_when_ready` for bounded startup retry.
- Breaking: `KvClient::begin` now requires `KvDurability::Buffered` or `KvDurability::Sync`.
- Reconnect attempts now default to unlimited (`maximum_attempts == 0`).
- Add managed leases, replay-safe retry configuration, idle heartbeat configuration, and dependency-light observability callbacks to the default async client.
- Remove the synchronous client, transports, domains, feature flag, and test suite; `Client` is exclusively Tokio-native.
- Add bounded, cancellation-safe request multiplexing and typed async streams.
- Reconnect with a fresh token, fail stale stateful handles, and restore active
  subscriptions and RPC workers before returning to `Authenticated`.
- Complete the async KV, Queue, Notice, RPC, Lease, Stream, and Schedule APIs,
  keeping wire IDs private and exposing typed found/not-found and list results.
- Add subscriber-driven `tracing` spans and lifecycle events without requiring
  an OpenTelemetry implementation.
- Rename the package to `cntryl-fitz` and the Rust import to `cntryl_fitz`.
- Add the async `Client` builder, lifecycle state observation, token providers,
  operation options, and reconnect configuration.
- Correct concurrent same-type response routing and retain timeout tombstones.
- Treat routes as opaque strings and remove production JWT behavior.
- Use the canonical versioned Stream filter encoding and remove RPC ACK usage.
