use super::*;

#[test]
fn should_preserve_append_code() {
    // Arrange
    let mut encoder = PayloadEncoder::new();
    encoder
        .put_u8(2)
        .put_u32(2001)
        .put_string("unrelated wording");

    // Act
    let error = decode_stream_response("APPEND", &encoder.finish()).unwrap_err();

    // Assert
    assert!(matches!(error, FitzError::Domain { code: 2001, .. }));
}

#[test]
fn should_preserve_commit_code() {
    // Arrange
    let mut encoder = PayloadEncoder::new();
    encoder
        .put_u8(2)
        .put_u32(2001)
        .put_string("unrelated wording");

    // Act
    let error = decode_stream_response("COMMIT", &encoder.finish()).unwrap_err();

    // Assert
    assert!(matches!(error, FitzError::Domain { code: 2001, .. }));
}

#[test]
fn should_preserve_misleading_wording() {
    // Arrange
    let mut encoder = PayloadEncoder::new();
    encoder
        .put_u8(2)
        .put_u32(2002)
        .put_string("concurrency conflict");

    // Act
    let error = decode_stream_response("APPEND", &encoder.finish()).unwrap_err();

    // Assert
    assert!(matches!(error, FitzError::Domain { code: 2002, .. }));
}

#[test]
fn should_preserve_infrastructure_code() {
    // Arrange
    let mut encoder = PayloadEncoder::new();
    encoder
        .put_u8(2)
        .put_u32(2012)
        .put_string("backend unavailable");

    // Act
    let error = decode_stream_response("COMMIT", &encoder.finish()).unwrap_err();

    // Assert
    assert!(matches!(error, FitzError::Domain { code: 2012, .. }));
}

#[test]
fn should_reject_trailing_versioned_error_data() {
    // Arrange
    let mut encoder = PayloadEncoder::new();
    encoder
        .put_u8(2)
        .put_u32(2001)
        .put_string("failure")
        .put_u8(0);

    // Act
    let error = decode_stream_response("COMMIT", &encoder.finish()).unwrap_err();

    // Assert
    assert!(matches!(error, FitzError::Protocol(_)));
}

#[test]
fn should_preserve_legacy_append_without_inventing_a_code() {
    // Arrange
    let mut encoder = PayloadEncoder::new();
    encoder.put_u8(1).put_string("concurrency conflict");

    // Act
    let error = decode_stream_response("APPEND", &encoder.finish()).unwrap_err();

    // Assert
    assert!(matches!(error, FitzError::Domain { code: 0, .. }));
}

#[test]
fn should_preserve_legacy_read_code() {
    // Arrange
    let mut encoder = PayloadEncoder::new();
    encoder
        .put_u8(1)
        .put_u32(2001)
        .put_string("unrelated wording");

    // Act
    let error = decode_stream_response("READ", &encoder.finish()).unwrap_err();

    // Assert
    assert!(matches!(error, FitzError::Domain { code: 2001, .. }));
}
