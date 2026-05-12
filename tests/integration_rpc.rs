mod jwt;

use cntryl::FitzClient;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

static ROUTE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy)]
enum Transport {
    Tcp,
    WebSocket,
}

fn connect_client(transport: Transport) -> FitzClient {
    let token = jwt::make_test_jwt("test-realm", "test-secret-key");
    match transport {
        Transport::Tcp => {
            FitzClient::connect_tcp("127.0.0.1", 4091, &token)
        }
        Transport::WebSocket => {
            FitzClient::connect_ws("ws://127.0.0.1:4090/ws", &token)
        }
    }
    .expect("failed to connect to fitz broker")
}

fn unique_rpc_route(suffix: &str) -> String {
    let counter = ROUTE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock drift")
        .as_nanos();
    format!("rpc://test-realm/{nonce}-{counter}/{suffix}")
}

fn run_single_response_rpc(transport: Transport) {
    let route = unique_rpc_route("echo");
    let (ready_tx, ready_rx) = mpsc::channel();
    let worker_route = route.clone();

    let worker = thread::spawn(move || {
        let worker_client = connect_client(transport);
        let worker_registration = worker_client
            .rpc()
            .register_worker(&worker_route)
            .expect("failed to register worker");
        ready_tx.send(()).expect("failed to signal readiness");

        let mut request = worker_registration
            .next()
            .expect("failed to receive rpc request");
        assert_eq!(request.route, worker_route);
        assert_eq!(request.body, b"ping");

        request
            .respond(b"pong", true)
            .expect("failed to send rpc response");

        worker_registration
            .unregister()
            .expect("failed to unregister worker");
        worker_client
            .close()
            .expect("failed to close worker client");
    });

    ready_rx.recv().expect("worker did not become ready");

    let caller_client = connect_client(transport);
    let mut response_stream = caller_client
        .rpc()
        .call(&route, b"ping")
        .expect("failed to issue rpc call");

    let first = response_stream
        .next()
        .expect("failed to read first rpc response")
        .expect("missing first rpc response");
    assert_eq!(first.sequence, 0);
    assert_eq!(first.body, b"pong");
    assert!(response_stream
        .next()
        .expect("failed to read terminal rpc frame")
        .is_none());

    caller_client
        .close()
        .expect("failed to close caller client");
    worker.join().expect("worker thread panicked");
}

fn run_streaming_rpc(transport: Transport) {
    let route = unique_rpc_route("stream");
    let (ready_tx, ready_rx) = mpsc::channel();
    let worker_route = route.clone();

    let worker = thread::spawn(move || {
        let worker_client = connect_client(transport);
        let worker_registration = worker_client
            .rpc()
            .register_worker(&worker_route)
            .expect("failed to register streaming worker");
        ready_tx.send(()).expect("failed to signal readiness");

        let mut request = worker_registration
            .next()
            .expect("failed to receive rpc request");
        assert_eq!(request.body, b"stream-me");

        request
            .respond(&[0], false)
            .expect("failed to send first response frame");
        request
            .respond(&[1], false)
            .expect("failed to send second response frame");
        request
            .respond(&[2], true)
            .expect("failed to send final response frame");

        worker_registration
            .unregister()
            .expect("failed to unregister worker");
        worker_client
            .close()
            .expect("failed to close worker client");
    });

    ready_rx.recv().expect("worker did not become ready");

    let caller_client = connect_client(transport);
    let mut response_stream = caller_client
        .rpc()
        .call(&route, b"stream-me")
        .expect("failed to issue streaming rpc call");

    let mut sequences = Vec::new();
    let mut bodies = Vec::new();
    while let Some(frame) = response_stream.next().expect("failed to read rpc stream") {
        sequences.push(frame.sequence);
        bodies.push(frame.body);
    }

    assert_eq!(sequences, vec![0, 1, 2]);
    assert_eq!(bodies, vec![vec![0], vec![1], vec![2]]);

    caller_client
        .close()
        .expect("failed to close caller client");
    worker.join().expect("worker thread panicked");
}

#[test]
fn should_execute_single_frame_rpc_over_tcp() {
    run_single_response_rpc(Transport::Tcp);
}

#[test]
fn should_execute_single_frame_rpc_over_websocket() {
    run_single_response_rpc(Transport::WebSocket);
}

#[test]
fn should_stream_rpc_frames_over_tcp() {
    run_streaming_rpc(Transport::Tcp);
}

#[test]
fn should_stream_rpc_frames_over_websocket() {
    run_streaming_rpc(Transport::WebSocket);
}
