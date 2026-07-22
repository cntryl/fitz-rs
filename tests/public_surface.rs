use std::fs;

fn read_source(path: &str) -> String {
    fs::read_to_string(path).expect("failed to read source file")
}

#[test]
fn should_keep_root_public_surface_narrow() {
    // Arrange
    let source = read_source("src/lib.rs");

    for forbidden in [
        "pub mod auth;",
        "pub mod codec;",
        "pub mod connection;",
        "pub mod protocol;",
        "pub mod transport;",
        "pub use auth::TestTokenGenerator;",
        // Act
    ] {
        // Assert
        assert!(
            !source.contains(forbidden),
            "unexpected public root surface fragment present: {forbidden}"
        );
    }

    for required in [
        "mod auth;",
        "mod codec;",
        "mod connection;",
        "mod protocol;",
        "mod transport;",
        "pub mod domains;",
        "pub use error::{FitzError, FitzErrorKind, Result};",
        "pub use protocol::TransactionMode;",
    ] {
        assert!(
            source.contains(required),
            "missing expected root surface fragment: {required}"
        );
    }
}

#[test]
fn should_keep_rpc_correlation_ids_private() {
    let source = read_source("src/domains/rpc.rs");
    assert!(source.contains("correlation_id: [u8; 16],"));
    assert!(!source.contains("pub correlation_id: [u8; 16],"));
}

#[test]
fn should_keep_queue_reservation_tokens_private() {
    // Arrange
    // Act
    let source = read_source("src/domains/queue.rs");
    // Assert
    assert!(source.contains("id: u64,"));
    assert!(source.contains("token: u64,"));
    assert!(!source.contains("pub id: u64,"));
    assert!(!source.contains("pub token: u64,"));
}
