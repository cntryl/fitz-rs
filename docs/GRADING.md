# Fitz Rust Grading

Status legend:
- PASS: implemented and covered by code or tests
- PARTIAL: implemented, but one or more contract gaps remain
- FAIL: known broken or missing behavior
- NOT IMPLEMENTED: intentionally not supported by the current blocking API
- UNASSESSED: not yet reviewed

## Snapshot

- Transport support: PASS
- Authentication helpers: PASS
- Domain coverage: PASS
- Connection lifecycle: PARTIAL
- Error model: PASS
- Conformance runner: PASS
- Documentation and verification: PASS

## Notes

| Area | Status | Evidence |
| --- | --- | --- |
| Transport matrix | PASS | TCP and WebSocket clients are both supported, and the conformance runner accepts `CONFORMANCE_TRANSPORT=tcp|ws`. |
| Authentication | PASS | JWT connect helpers remain available, and anonymous connect helpers are now exposed for broker modes that do not require a token. |
| Domain clients | PASS | KV, Queue, Notice, RPC, Lease, Stream, and Schedule are all exposed from the public client facade. |
| Error contract | PASS | `FitzErrorKind`, `kind()`, `is_retryable()`, and `domain_message()` provide machine-readable classification without parsing strings. |
| Connection lifecycle | PARTIAL | Clean close and closed-state checks are in place. Reconnect orchestration is not part of the current synchronous Rust surface. |
| Conformance runner | PASS | `tests/conformance.rs` emits normalized JSON artifacts and mirrors the shared `CS-001` through `CS-015` matrix. |
| Verification docs | PASS | The README and local docs now point at the real conformance runner and artifact path. |

## Documented Gaps

- `CS-008` caller cancellation is `NOT IMPLEMENTED` because the current Rust API is intentionally synchronous and does not expose per-call cancellation tokens.
- `CS-010` reconnect and retry behavior is `NOT IMPLEMENTED` because reconnect/state-restoration semantics are not part of the current blocking client shape.

## Verification Commands

```bash
cargo test --lib
cargo test --test conformance -- --ignored --nocapture
CONFORMANCE_TRANSPORT=ws CONFORMANCE_AUTH_MODE=valid_jwt cargo test --test conformance -- --ignored --nocapture
```

Default conformance artifact path:

```text
./artifacts/conformance-results.json
```
