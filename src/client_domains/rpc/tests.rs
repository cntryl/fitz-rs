use super::*;
use futures_util::StreamExt;
use tokio::sync::broadcast;

#[tokio::test]
async fn should_surface_terminal_rpc_error_as_domain_error() {
    // Arrange
    let (sender, receiver) = broadcast::channel(2);
    let (_lifecycle_sender, lifecycle_receiver) = broadcast::channel(2);
    let id = [9_u8; 16];
    let mut body = PayloadEncoder::new();
    body.put_u8(1)
        .put_u32(6004)
        .put_string("No workers registered");
    let mut frame = PayloadEncoder::new();
    frame
        .put_raw(&id)
        .put_u64(0)
        .put_u8(1)
        .put_bytes(&body.finish());
    let mut stream = RpcResponseStream {
        connection: None,
        correlation_id: id,
        receiver: BroadcastStream::new(receiver),
        lifecycle_receiver,
        supports_cancellation: false,
        cancellation_sent: Arc::new(AtomicBool::new(false)),
        cancellation_outcome: None,
        timeout: None,
        parent_cancellation: None,
        cancellation_shutdown: CancellationToken::new(),
        connection_closed: None,
        finished: false,
    };

    // Act
    sender.send(frame.finish()).unwrap();
    let result = stream.next().await.unwrap();

    // Assert
    assert!(matches!(result, Err(FitzError::Domain { code: 6004, .. })));
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn should_enforce_rpc_deadline_locally() {
    // Arrange
    let (_sender, receiver) = broadcast::channel(2);
    let (_lifecycle_sender, lifecycle_receiver) = broadcast::channel(2);
    let mut stream = RpcResponseStream {
        connection: None,
        correlation_id: [7_u8; 16],
        receiver: BroadcastStream::new(receiver),
        lifecycle_receiver,
        supports_cancellation: false,
        cancellation_sent: Arc::new(AtomicBool::new(false)),
        cancellation_outcome: None,
        timeout: Some(Box::pin(tokio::time::sleep_until(
            tokio::time::Instant::now(),
        ))),
        parent_cancellation: None,
        cancellation_shutdown: CancellationToken::new(),
        connection_closed: None,
        finished: false,
    };

    // Act
    let result = stream.next().await;

    // Assert
    assert!(matches!(result, Some(Err(FitzError::Timeout))));
    assert!(stream.next().await.is_none());
}

#[test]
fn should_preserve_empty_terminal_rpc_response_frame() {
    // Arrange
    let mut encoder = PayloadEncoder::new();
    encoder.put_u64(7).put_u8(1).put_bytes(&[]);
    let payload = encoder.finish();
    let mut decoder = PayloadDecoder::new(&payload);

    // Act
    let (sequence, end, body) = decode_rpc_response_frame(&mut decoder).unwrap();

    // Assert
    assert_eq!(sequence, 7);
    assert!(end);
    assert_eq!(body, Vec::<u8>::new());
}

#[test]
fn should_reject_rpc_response_with_unsupported_flags() {
    // Arrange
    let mut encoder = PayloadEncoder::new();
    encoder.put_u64(0).put_u8(2).put_bytes(&[]);
    let payload = encoder.finish();
    let mut decoder = PayloadDecoder::new(&payload);

    // Act
    let result = decode_rpc_response_frame(&mut decoder);

    // Assert
    assert!(matches!(result, Err(FitzError::Protocol(_))));
}

#[test]
fn should_accept_worker_concurrency_at_wire_boundaries() {
    // Arrange
    let valid = [1, 1024];

    // Act
    let results = valid.map(validate_worker_concurrency);

    // Assert
    assert!(results.into_iter().all(|result| result.is_ok()));
}

#[test]
fn should_reject_worker_concurrency_outside_wire_range() {
    // Arrange
    let invalid = [0, 1025, u32::MAX];

    // Act
    let results = invalid.map(validate_worker_concurrency);

    // Assert
    assert!(results.into_iter().all(|result| matches!(
        result,
        Err(FitzError::Protocol(message)) if message == "max_concurrency must be between 1 and 1024"
    )));
}

#[test]
fn should_decode_negotiated_rpc_request_budget() {
    // Arrange
    let mut decoder = PayloadDecoder::new(&[1, 1, 0, 0, 1, 44]);

    // Act
    let budget = decode_rpc_request_budget(&mut decoder).unwrap();

    // Assert
    assert_eq!(budget, Some(Duration::from_millis(300)));
}

#[test]
fn should_reject_rpc_request_budget_with_trailing_bytes() {
    // Arrange
    let mut decoder = PayloadDecoder::new(&[1, 1, 0, 0, 1, 44, 0]);

    // Act
    let result = decode_rpc_request_budget(&mut decoder);

    // Assert
    assert!(matches!(result, Err(FitzError::Protocol(_))));
}

#[test]
fn should_cancel_active_worker_context_given_broker_lifecycle_signal() {
    // Arrange
    let correlation_id = [7_u8; 16];
    let cancellation = CancellationToken::new();
    let active_calls = Mutex::new(HashMap::from([(
        correlation_id,
        ActiveWorkerCall {
            cancellation: cancellation.clone(),
            cancellation_requested: false,
        },
    )]));
    let mut payload = Vec::from([2]);
    payload.extend_from_slice(&correlation_id);
    payload.push(1);

    // Act
    cancel_worker_call(&active_calls, &payload);

    // Assert
    assert!(cancellation.is_cancelled());
    assert!(active_calls.lock()[&correlation_id].cancellation_requested);
}

#[test]
fn should_ignore_unknown_worker_cancellation_without_retaining_state() {
    // Arrange
    let active_calls = Mutex::new(HashMap::new());
    let mut payload = Vec::from([2]);
    payload.extend_from_slice(&[7_u8; 16]);
    payload.push(1);

    // Act
    cancel_worker_call(&active_calls, &payload);

    // Assert
    assert!(active_calls.lock().is_empty());
}

#[test]
fn should_preserve_unrelated_worker_context_given_targeted_cancellation() {
    // Arrange
    let unrelated = CancellationToken::new();
    let active_calls = Mutex::new(HashMap::from([
        (
            [1_u8; 16],
            ActiveWorkerCall {
                cancellation: CancellationToken::new(),
                cancellation_requested: false,
            },
        ),
        (
            [2_u8; 16],
            ActiveWorkerCall {
                cancellation: unrelated.clone(),
                cancellation_requested: false,
            },
        ),
    ]));
    let mut payload = vec![2];
    payload.extend_from_slice(&[1_u8; 16]);
    payload.push(1);

    // Act
    cancel_worker_call(&active_calls, &payload);

    // Assert
    assert!(!unrelated.is_cancelled());
}

#[test]
fn should_map_matching_rpc_cancellation_result_status() {
    // Arrange
    let correlation_id = [8_u8; 16];
    let mut payload = Vec::from([4]);
    payload.extend_from_slice(&correlation_id);
    payload.push(2);

    // Act
    let outcome = decode_cancellation_result(&payload, &correlation_id);

    // Assert
    assert_eq!(outcome, Some(RpcCancellationOutcome::Forwarded));
}
