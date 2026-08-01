# Changelog

## 0.1.0 - Unreleased

- Replace the blocking client surface with one Tokio-native `Client`; the
  previous synchronous API is no longer part of the default feature set.
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
