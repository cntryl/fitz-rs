# Fitz Rust World-Class TODO

You are working in fitz-rs. The crate is already a usable Fitz client, but it does not yet have the parity infrastructure, lifecycle proof, or documentation rigor that the stronger SDKs in this workspace now have. Bring it to world-class status without abandoning Rust-idiomatic design.

## Canonical Sources

- [../fitz/docs/clients/CLIENT_SPEC.md](../fitz/docs/clients/CLIENT_SPEC.md)
- [../fitz/docs/clients/CLIENT_ACCEPTANCE_CRITERIA.md](../fitz/docs/clients/CLIENT_ACCEPTANCE_CRITERIA.md)
- [../fitz/docs/clients/CLIENT_IMPLEMENTATION_GUIDE.md](../fitz/docs/clients/CLIENT_IMPLEMENTATION_GUIDE.md)
- [../fitz/docs/clients/CONNECTION_FLOW.md](../fitz/docs/clients/CONNECTION_FLOW.md)
- [README.md](README.md)
- [docs/README.md](docs/README.md)
- [src/lib.rs](src/lib.rs)
- [src/connection.rs](src/connection.rs)
- [src/error.rs](src/error.rs)

## What Is Still Missing

- There is no repo-local grading report, gap matrix, or conformance harness yet.
- The README still contains aspirational language that does not have parity evidence behind it.
- The connection layer is still a synchronous one-shot abstraction without the explicit lifecycle, reconnect, and disconnect proof that the other SDKs now use.
- Error handling is still coarse and does not yet present a world-class machine-readable contract for retryability and domain failures.
- The current tests are useful, but they do not yet cover the shared Fitz conformance matrix.

## Work In Order

1. Establish an explicit parity baseline.
   - Add a grading or audit doc that states what is pass, partial, fail, and unassessed.
   - Tie that document to the canonical Fitz client contract and keep it current.
2. Add a shared conformance harness.
   - Implement the cross-language suite for `CS-001` through `CS-015`.
   - Exercise both TCP and WebSocket with anonymous and valid JWT modes.
   - Write normalized JSON output and retain the artifacts in CI.
3. Fix lifecycle semantics.
   - Introduce explicit connection state transitions, auth failure surfacing, and clean close behavior.
   - If reconnect is supported, prove state restoration for subscriptions and workers and make the behavior deterministic.
4. Tighten the public API and error model.
   - Keep routes opaque.
   - Make error types machine-readable and add retryability or domain-code classification if the Fitz contract requires it.
   - Remove README drift where it says features are coming soon but the code already claims to support them.
5. Align tests, docs, and release guidance.
   - Add end-to-end tests for real failure modes, not just codec happy paths.
   - Document the actual verification command set and world-class acceptance criteria.

## Concrete Gap Checklist

- `README.md`: remove any aspirational or stale capability claims that are not backed by tests and code.
- `src/lib.rs` and `src/connection.rs`: add the lifecycle and reconnect semantics needed for a contract-grade client.
- `src/error.rs`: expand the error model until it is useful for callers without needing internal knowledge.
- `tests/`: add a shared conformance harness and the supporting broker-backed tests.
- `docs/README.md`: make sure the local docs point to the real parity story, not a placeholder.

## Definition Of Done

- The README matches the implemented surface.
- There is a conformance runner, a parity/audit doc, and CI that exercise the canonical suite.
- Connection, auth, reconnect, and error semantics are proven by tests, not implied by comments.
- The crate can be reviewed against the same world-class bar as the other Fitz SDKs in this workspace.

## Constraints

- Keep the blocking Rust API if that remains the chosen Rust shape; do not add async wrappers just to match other languages.
- Do not invent new abstraction layers unless they are required to express the Fitz contract.
- Keep routes opaque and protocol semantics faithful to the canonical docs.
- Prefer additive, non-breaking changes and targeted tests over broad rewrites.