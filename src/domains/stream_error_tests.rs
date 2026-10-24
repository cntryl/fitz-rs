use super::*;

#[test]
fn should_preserve_append_code() {
    // Arrange
    let mut encoder = PayloadEncoder::new();
    encoder
        .put_u8(1)
        .put_u16(2050)
        .put_u8(0)
        .put_u8(0)
        .put_string("unrelated wording");

    // Act
    let error = decode_stream_response("APPEND", &encoder.finish()).unwrap_err();

    // Assert
    assert!(matches!(error, FitzError::DomainWire { code: 2050, .. }));
}

#[test]
fn should_preserve_commit_code() {
    // Arrange
    let mut encoder = PayloadEncoder::new();
    encoder
        .put_u8(1)
        .put_u16(2050)
        .put_u8(0)
        .put_u8(0)
        .put_string("unrelated wording");

    // Act
    let error = decode_stream_response("COMMIT", &encoder.finish()).unwrap_err();

    // Assert
    assert!(matches!(error, FitzError::DomainWire { code: 2050, .. }));
}

#[test]
fn should_preserve_misleading_wording() {
    // Arrange
    let mut encoder = PayloadEncoder::new();
    encoder
        .put_u8(1)
        .put_u16(2052)
        .put_u8(0)
        .put_u8(0)
        .put_string("concurrency conflict");

    // Act
    let error = decode_stream_response("APPEND", &encoder.finish()).unwrap_err();

    // Assert
    assert!(matches!(error, FitzError::DomainWire { code: 2052, .. }));
}

#[test]
fn should_preserve_infrastructure_code() {
    // Arrange
    let mut encoder = PayloadEncoder::new();
    encoder
        .put_u8(1)
        .put_u16(2006)
        .put_u8(6)
        .put_u8(1)
        .put_string("backend unavailable");

    // Act
    let error = decode_stream_response("COMMIT", &encoder.finish()).unwrap_err();

    // Assert
    assert!(matches!(error, FitzError::DomainWire { code: 2006, .. }));
}

#[test]
fn should_reject_trailing_versioned_error_data() {
    // Arrange
    let mut encoder = PayloadEncoder::new();
    encoder
        .put_u8(1)
        .put_u16(2050)
        .put_u8(0)
        .put_u8(0)
        .put_string("failure")
        .put_u8(0);

    // Act
    let error = decode_stream_response("COMMIT", &encoder.finish()).unwrap_err();

    // Assert
    assert_eq!(error.kind(), crate::error::FitzErrorKind::Protocol);
}

#[test]
fn should_reject_legacy_plain_append_error() {
    // Arrange
    let mut encoder = PayloadEncoder::new();
    encoder.put_u8(1).put_string("concurrency conflict");

    // Act
    let error = decode_stream_response("APPEND", &encoder.finish()).unwrap_err();

    // Assert
    assert_eq!(error.kind(), crate::error::FitzErrorKind::Protocol);
}

#[test]
fn should_reject_legacy_read_error() {
    // Arrange
    let mut encoder = PayloadEncoder::new();
    encoder
        .put_u8(1)
        .put_u32(2001)
        .put_string("unrelated wording");

    // Act
    let error = decode_stream_response("READ", &encoder.finish()).unwrap_err();

    // Assert
    assert_eq!(error.kind(), crate::error::FitzErrorKind::Protocol);
}
