use cntryl_fitz::client_domains::rpc::RpcCancellationOutcome;
use cntryl_fitz::{Client, FitzError, Result};
use futures_util::StreamExt;
use std::time::Duration;
use tokio::sync::oneshot;
#[allow(
    dead_code,
    reason = "shared test JWT fixture also supplies invalid and scoped token signers"
)]
mod jwt;

#[tokio::test]
#[ignore = "requires a broker advertising CAP_RPC_CANCELLATION at FITZ_RPC_CHAIN_ADDR"]
async fn should_receive_worker_responses_given_opaque_dispatch_identity() -> Result<()> {
    // Arrange
    let endpoint =
        std::env::var("FITZ_RPC_CHAIN_ADDR").unwrap_or_else(|_| "tcp://127.0.0.1:4191".to_owned());
    let caller = client(&endpoint)?;
    let worker_client = client(&endpoint)?;
    caller.connect().await?;
    worker_client.connect().await?;
    let route = format!("rpc://chain/app/unary-{}", uuid::Uuid::new_v4());
    let mut worker = worker_client.rpc()?.register_worker(&route, 1).await?;
    let serving = tokio::spawn(async move {
        let mut request = worker.next().await.unwrap().unwrap();
        request.respond(b"first", false).await.unwrap();
        request.respond(b"last", true).await.unwrap();
    });

    // Act
    let mut response = caller.rpc()?.call(&route, b"request").await?;
    let first = response.next().await.unwrap()?;
    let last = response.next().await.unwrap()?;
    let ended = response.next().await.is_none();
    serving.await.unwrap();

    // Assert
    assert_eq!((first.sequence, first.body), (0, b"first".to_vec()));
    assert_eq!((last.sequence, last.body), (1, b"last".to_vec()));
    assert!(ended);
    caller.close().await?;
    worker_client.close().await?;
    Ok(())
}

fn client(endpoint: &str) -> Result<Client> {
    if let Ok(secret) = std::env::var("FITZ_BROKER_JWT_HMAC_SECRET") {
        let token = jwt::make_test_jwt("chain", &secret);
        Client::builder(endpoint, move || {
            let token = token.clone();
            async move { Ok(token) }
        })
        .build()
    } else {
        Client::anonymous(endpoint).build()
    }
}

#[tokio::test]
#[ignore = "requires a broker advertising CAP_RPC_CANCELLATION at FITZ_RPC_CHAIN_ADDR"]
async fn should_cancel_real_sdk_call_chain_given_caller_cancellation() -> Result<()> {
    // Arrange
    let chain = Chain::start(Duration::from_secs(3)).await?;

    // Act
    let (outcome, worker_cancelled) = chain.cancel().await?;

    // Assert
    assert_eq!(outcome, RpcCancellationOutcome::Forwarded);
    assert!(worker_cancelled);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a broker advertising CAP_RPC_CANCELLATION at FITZ_RPC_CHAIN_ADDR"]
async fn should_expire_real_sdk_call_chain_within_inherited_deadline() -> Result<()> {
    // Arrange
    let chain = Chain::start(Duration::from_millis(700)).await?;

    // Act
    let (caller_timed_out, worker_cancelled, elapsed) = chain.expire().await?;

    // Assert
    assert!(caller_timed_out);
    assert!(worker_cancelled);
    assert!(elapsed < Duration::from_millis(1500));
    Ok(())
}

struct Chain {
    clients: [Client; 3],
    call: cntryl_fitz::client_domains::rpc::RpcResponseStream,
    workers: [tokio::task::JoinHandle<bool>; 2],
    started: tokio::time::Instant,
}

impl Chain {
    async fn start(budget: Duration) -> Result<Self> {
        let endpoint = std::env::var("FITZ_RPC_CHAIN_ADDR")
            .unwrap_or_else(|_| "tcp://127.0.0.1:4191".to_owned());
        let clients = [client(&endpoint)?, client(&endpoint)?, client(&endpoint)?];
        for client in &clients {
            client.connect().await?;
            assert_ne!(client.server_capabilities().1 & (1 << 3), 0);
        }
        let suffix = uuid::Uuid::new_v4();
        let route_b = format!("rpc://chain/app/b-{suffix}");
        let route_c = format!("rpc://chain/app/c-{suffix}");
        let b_rpc = clients[1].rpc()?;
        let mut b_worker = b_rpc.register_worker(&route_b, 1).await?;
        let mut c_worker = clients[2].rpc()?.register_worker(&route_c, 1).await?;
        let (received, c_received) = oneshot::channel();
        let b = tokio::spawn(async move {
            let parent = b_worker.next().await.unwrap().unwrap();
            tokio::time::sleep(Duration::from_millis(25)).await;
            let mut child = b_rpc
                .call_from_request(&parent, &route_c, b"child")
                .await
                .unwrap();
            while child.next().await.is_some() {}
            parent.cancellation.cancelled().await;
            let cancelled = parent.cancellation.is_cancelled();
            drop(child);
            drop(parent);
            cancelled
        });
        let c = tokio::spawn(async move {
            let child = c_worker.next().await.unwrap().unwrap();
            received.send(child.remaining_time().unwrap()).unwrap();
            child.cancellation.cancelled().await;
            let cancelled = child.cancellation.is_cancelled();
            drop(child);
            cancelled
        });
        let started = tokio::time::Instant::now();
        let call = clients[0]
            .rpc()?
            .call_with_timeout(&route_b, b"parent", Some(budget))
            .await?;
        let remaining = tokio::time::timeout(Duration::from_secs(2), c_received)
            .await
            .unwrap()
            .unwrap();
        assert!(
            remaining < budget,
            "downstream budget must include parent processing time"
        );
        Ok(Self {
            clients,
            call,
            workers: [b, c],
            started,
        })
    }

    async fn cancel(mut self) -> Result<(RpcCancellationOutcome, bool)> {
        let outcome = self.call.cancel().await;
        let cancelled = self.finish_workers().await?;
        Ok((outcome, cancelled))
    }

    async fn expire(mut self) -> Result<(bool, bool, Duration)> {
        let timed_out = matches!(
            self.call.next().await,
            Some(Err(
                FitzError::Timeout | FitzError::Domain { code: 6001, .. }
            ))
        );
        let elapsed = self.started.elapsed();
        let cancelled = self.finish_workers().await?;
        Ok((timed_out, cancelled, elapsed))
    }

    async fn finish_workers(self) -> Result<bool> {
        let mut cancelled = true;
        for worker in self.workers {
            cancelled &= tokio::time::timeout(Duration::from_secs(3), worker)
                .await
                .unwrap()
                .unwrap();
        }
        for client in &self.clients {
            client.close().await?;
        }
        Ok(cancelled)
    }
}
