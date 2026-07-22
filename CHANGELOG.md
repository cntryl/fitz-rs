# Changelog

## 0.1.0 - Unreleased

- Rename the package to `cntryl-fitz` and the Rust import to `cntryl_fitz`.
- Add the async `Client` builder, lifecycle state observation, token providers,
  operation options, and reconnect configuration.
- Correct concurrent same-type response routing and retain timeout tombstones.
- Treat routes as opaque strings and remove production JWT behavior.
- Use the canonical versioned Stream filter encoding and remove RPC ACK usage.
