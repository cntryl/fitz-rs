use super::*;

fn push_string(buf: &mut Vec<u8>, value: &str) {
    buf.extend_from_slice(&(u32::try_from(value.len()).unwrap()).to_be_bytes());
    buf.extend_from_slice(value.as_bytes());
}

#[test]
fn should_decode_begin_response_with_session_id() {
    // Arrange
    let mut buf = vec![0, 1];
    buf.extend_from_slice(&9u64.to_be_bytes());

    // Act
    let decoded = decode_stream_response("BEGIN", &buf).unwrap();
    // Assert
    assert_eq!(decoded.session_id, Some(9));
    assert!(decoded.data.is_empty());
}

#[test]
fn should_decode_stream_append_offset_payload() {
    // Arrange
    let mut buf = vec![0, 0];
    buf.extend_from_slice(&(8u32).to_be_bytes());
    buf.extend_from_slice(&17u64.to_be_bytes());

    let decoded = decode_stream_response("APPEND", &buf).unwrap();
    // Act
    let mut dec = PayloadDecoder::new(&decoded.data);
    // Assert
    assert_eq!(dec.get_u64().unwrap(), 17);
}

#[test]
fn should_parse_count_prefixed_stream_records() {
    // Arrange
    let mut buf = Vec::new();
    buf.extend_from_slice(&(2u32).to_be_bytes());
    push_string(&mut buf, "stream://realm/area/resource");
    buf.push(0);
    buf.extend_from_slice(&1u64.to_be_bytes());
    buf.push(1);
    buf.extend_from_slice(&10u64.to_be_bytes());
    buf.push(1);
    buf.extend_from_slice(&20u64.to_be_bytes());
    buf.extend_from_slice(&(1u32).to_be_bytes());
    buf.extend_from_slice(b"a");
    buf.push(1);
    buf.extend_from_slice(&(2u32).to_be_bytes());
    buf.extend_from_slice(b"m1");
    buf.extend_from_slice(&111u64.to_be_bytes());
    push_string(&mut buf, "stream://realm/area/resource");
    buf.push(0);
    buf.extend_from_slice(&2u64.to_be_bytes());
    buf.push(0);
    buf.push(1);
    buf.extend_from_slice(&21u64.to_be_bytes());
    buf.extend_from_slice(&(1u32).to_be_bytes());
    buf.extend_from_slice(b"b");
    buf.push(0);
    buf.extend_from_slice(&222u64.to_be_bytes());
    buf.extend_from_slice(&2u64.to_be_bytes());
    buf.push(0);
    buf.push(0);
    buf.push(0);

    let page = parse_stream_read_page(&buf).unwrap();
    // Act
    let records = flatten_stream_read_items(&page.items);
    // Assert
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].route, "stream://realm/area/resource");
    assert_eq!(records[0].offset, 1);
    assert_eq!(records[0].area_offset, Some(10));
    assert_eq!(records[0].realm_offset, Some(20));
    assert_eq!(records[0].metadata.as_deref(), Some(&b"m1"[..]));
    assert_eq!(records[0].timestamp, 111);
    assert_eq!(records[1].body, b"b");
    assert_eq!(records[1].route, "stream://realm/area/resource");
    assert_eq!(records[1].offset, 2);
    assert_eq!(records[1].timestamp, 222);
    assert_eq!(page.cursor.last_resource_offset, 2);
    assert_eq!(page.cursor.last_area_offset, None);
    assert_eq!(page.cursor.last_realm_offset, None);
    assert!(!page.cursor.has_more);
}

#[test]
fn should_parse_filtered_stream_read_page() {
    // Arrange
    let mut buf = Vec::new();
    buf.extend_from_slice(&(3u32).to_be_bytes());
    push_string(&mut buf, "stream://realm/area/resource");
    buf.push(0);
    buf.extend_from_slice(&41u64.to_be_bytes());
    buf.push(1);
    buf.extend_from_slice(&51u64.to_be_bytes());
    buf.push(0);
    buf.extend_from_slice(&(1u32).to_be_bytes());
    buf.extend_from_slice(b"a");
    buf.push(0);
    buf.extend_from_slice(&111u64.to_be_bytes());
    push_string(&mut buf, "stream://realm/area/resource");
    buf.push(1);
    buf.extend_from_slice(&42u64.to_be_bytes());
    buf.push(1);
    push_string(&mut buf, "stream://realm/area/resource");
    buf.push(2);
    buf.extend_from_slice(&43u64.to_be_bytes());
    buf.extend_from_slice(&45u64.to_be_bytes());
    buf.push(2);
    buf.extend_from_slice(&45u64.to_be_bytes());
    buf.push(1);
    buf.extend_from_slice(&52u64.to_be_bytes());
    buf.push(0);
    buf.push(1);

    // Act
    let page = parse_stream_read_page(&buf).unwrap();

    // Assert
    assert_eq!(page.cursor.last_resource_offset, 45);
    assert_eq!(page.cursor.last_area_offset, Some(52));
    assert_eq!(page.cursor.last_realm_offset, None);
    assert!(page.cursor.has_more);
    assert_eq!(page.items.len(), 3);
    assert!(matches!(page.items[0], StreamReadItem::Event(_)));
    assert_eq!(
        page.items[1],
        StreamReadItem::Filtered {
            route: "stream://realm/area/resource".to_string(),
            offset: 42,
            reason: Some(StreamFilteredReason::ServerFilter),
        }
    );
    assert_eq!(
        page.items[2],
        StreamReadItem::FilteredRange {
            route: "stream://realm/area/resource".to_string(),
            from_offset: 43,
            to_offset: 45,
            reason: Some(StreamFilteredReason::Permission),
        }
    );
    let records = flatten_stream_read_items(&page.items);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].offset, 41);
}

#[test]
fn should_parse_concrete_routes_given_wildcard_stream_read() {
    // Arrange
    let mut encoder = PayloadEncoder::new();
    encoder
        .put_u32(1)
        .put_string("stream://acme/cats/cat-1")
        .put_u8(1)
        .put_u64(42)
        .put_u8(1)
        .put_u64(42)
        .put_u8(0)
        .put_u8(0)
        .put_u8(0);

    // Act
    let page = parse_stream_read_page(&encoder.finish()).unwrap();

    // Assert
    assert_eq!(
        page.items,
        vec![StreamReadItem::Filtered {
            route: "stream://acme/cats/cat-1".to_string(),
            offset: 42,
            reason: Some(StreamFilteredReason::ServerFilter),
        }]
    );
}

#[test]
fn should_reject_wildcard_route_given_stream_read_response() {
    // Arrange
    let mut encoder = PayloadEncoder::new();
    encoder
        .put_u32(1)
        .put_string("stream://*/cats/*")
        .put_u8(1)
        .put_u64(42)
        .put_u8(1)
        .put_u64(42)
        .put_u8(0)
        .put_u8(0)
        .put_u8(0);

    // Act
    let result = parse_stream_read_page(&encoder.finish());

    // Assert
    assert!(matches!(
        result,
        Err(FitzError::Protocol(message)) if message.contains("READ response")
    ));
}

#[test]
fn should_parse_full_stream_record() {
    // Arrange
    let mut buf = Vec::new();
    push_string(&mut buf, "stream://realm/area/resource");
    buf.extend_from_slice(&1u64.to_be_bytes());
    buf.push(1);
    buf.extend_from_slice(&2u64.to_be_bytes());
    buf.push(1);
    buf.extend_from_slice(&3u64.to_be_bytes());
    buf.extend_from_slice(&(4u32).to_be_bytes());
    buf.extend_from_slice(b"body");
    buf.push(1);
    buf.extend_from_slice(&(4u32).to_be_bytes());
    buf.extend_from_slice(b"meta");
    buf.extend_from_slice(&5u64.to_be_bytes());

    // Act
    let record = parse_stream_last_response(&buf).unwrap().unwrap();
    // Assert
    assert_eq!(record.route, "stream://realm/area/resource");
    assert_eq!(record.offset, 1);
    assert_eq!(record.area_offset, Some(2));
    assert_eq!(record.realm_offset, Some(3));
    assert_eq!(record.body, b"body");
    assert_eq!(record.metadata.as_deref(), Some(&b"meta"[..]));
    assert_eq!(record.timestamp, 5);
}

#[test]
fn should_reject_wildcard_route_in_stream_last_response() {
    let mut buf = Vec::new();
    push_string(&mut buf, "stream://*/area/resource");

    let result = parse_stream_last_response(&buf);

    assert!(matches!(
        result,
        Err(FitzError::Protocol(message)) if message.contains("LAST response")
    ));
}

#[test]
fn should_decode_stream_metadata_payload() {
    // Arrange
    let mut buf = Vec::new();
    buf.push(1);
    buf.extend_from_slice(&4u64.to_be_bytes());
    buf.push(1);
    buf.extend_from_slice(&9u64.to_be_bytes());
    buf.extend_from_slice(&2u64.to_be_bytes());
    buf.extend_from_slice(&100u64.to_be_bytes());
    buf.extend_from_slice(&200u64.to_be_bytes());
    buf.push(1);
    buf.extend_from_slice(&300u64.to_be_bytes());
    buf.extend_from_slice(&7u64.to_be_bytes());
    buf.extend_from_slice(&8u64.to_be_bytes());

    // Act
    let metadata = decode_stream_metadata(&buf).unwrap();
    // Assert
    assert_eq!(metadata.first_offset, 4);
    assert_eq!(metadata.last_offset, 9);
    assert_eq!(metadata.record_count, 2);
    assert_eq!(metadata.max_batch_events, 100);
    assert_eq!(metadata.max_batch_bytes, 200);
    assert_eq!(metadata.ttl_seconds, Some(300));
    assert_eq!(metadata.area_watermark, 7);
    assert_eq!(metadata.realm_watermark, 8);
}

#[test]
fn should_decode_stream_notify_payload() {
    // Arrange
    let payload = br#"{"event":"committed","first_resource_offset":4,"last_resource_offset":5,"first_area_offset":7,"last_area_offset":8,"first_realm_offset":9,"last_realm_offset":10,"batch_size":2}"#;

    let mut buf = Vec::new();
    buf.extend_from_slice(&42u64.to_be_bytes());
    buf.extend_from_slice(&(21u32).to_be_bytes());
    buf.extend_from_slice(b"stream://realm/area/x");
    buf.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
    buf.extend_from_slice(payload);

    // Act
    let notification = decode_stream_notify(&buf).unwrap();
    // Assert
    assert_eq!(notification.route, "stream://realm/area/x");
    assert_eq!(notification.event, "committed");
    assert_eq!(notification.last_resource_offset, 5);
    assert_eq!(notification.batch_size, 2);
}

#[test]
fn should_decode_stream_error_response() {
    // Arrange
    let mut buf = vec![1];
    buf.extend_from_slice(&2010u32.to_be_bytes());
    buf.extend_from_slice(&(4u32).to_be_bytes());
    buf.extend_from_slice(b"nope");

    // Act
    let err = decode_stream_response("READ", &buf).unwrap_err();
    // Assert
    assert!(err.to_string().contains("nope"));
    assert!(matches!(err, FitzError::Domain { code: 2010, .. }));
}

#[test]
fn should_encode_canonical_stream_filter_set() {
    // Arrange
    let filter = StreamFilterSet {
        clauses: vec![
            StreamFilterClause::Equals("proj.alpha".to_string()),
            StreamFilterClause::NotEquals("audit.beta".to_string()),
            StreamFilterClause::StartsWith("proj.".to_string()),
            StreamFilterClause::AnyOf(vec!["proj.alpha".to_string(), "proj.gamma".to_string()]),
        ],
    };

    // Act
    let encoded = encode_stream_filter_set(&filter).unwrap();
    // Assert
    assert_eq!(&encoded[..2], &[0, 0xF1]);
    assert_eq!(&encoded[2..6], &4_u32.to_be_bytes());
}

#[test]
fn should_match_missing_discriminator_as_empty_string() {
    // Arrange
    let filter = StreamFilterSet {
        clauses: vec![StreamFilterClause::Equals(String::new())],
    };

    // Act
    let matches_missing = filter.matches(None);
    let matches_present = filter.matches(Some("proj.alpha"));

    // Assert
    assert!(matches_missing);
    assert!(!matches_present);
}
