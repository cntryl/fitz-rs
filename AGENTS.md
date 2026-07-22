# Fitz Rust Client Agent Guide

## Scope

- This repository is the Rust Fitz client.
- Keep the public SDK async-first and idiomatic to Tokio and Rust.
- Validate route shape client-side, but leave route existence, permissions, authorization, and ownership to the broker.
- Keep hot-path route validation single-pass, allocation-free on success, and free of regular expressions.

## Test Standards

- Name tests with `should_*` behavior names.
- Tests longer than five lines must contain meaningful `// Arrange`, `// Act`, and `// Assert` sections.
- Keep one behavior per test. Split workflows when multiple independent acts or assertions obscure the behavior.
- Do not satisfy validation mechanically with misleading comments or broad lint suppressions.
- Broker-backed tests must state their runtime prerequisite when ignored.

## Required Gates

Run these before considering a change complete:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings -D clippy::pedantic
cntryl-tools validate-tests
cargo test --all-targets --all-features
```

- `cntryl-tools validate-tests` must report every test compliant.
- Do not weaken Clippy or test-validation policy repository-wide.
- Local lint exceptions require a narrow scope and an explanatory reason.
- If broker-backed tests cannot run, report the exact unavailable endpoint and separately prove all non-broker gates.

## Delivery

- Preserve unrelated changes.
- Commit scoped changes and push `main` only after the relevant gates pass.
- Do not publish packages or create tags unless explicitly requested.
