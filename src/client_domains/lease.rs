use crate::async_connection::{AsyncConnection, RestorableRegistration};
use crate::codec::{PayloadDecoder, PayloadEncoder};
use crate::domains::routes::{validate_fixed_route, validate_registration_pattern};
use crate::protocol::message_type;
use crate::{FitzError, Result};
use futures_core::Stream;
use futures_util::{FutureExt, StreamExt};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio_stream::wrappers::BroadcastStream;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, Default)]
pub struct LeaseExecutionOptions {
    pub wait_seconds: u32,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LeaseAcquireOptions {
    pub wait_seconds: u32,
}

/// Immutable admission authority granted to one managed lease callback.
///
/// `fencing_token` is the token from the final successful ACQUIRE response.
/// It is an admission epoch, not the lease handle's live renewal credential,
/// and remains stable even when renewal rotates that internal credential.
/// Tokens are ordered only across successive ownership of the same lease route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseAuthority {
    /// Broker-issued fencing epoch that admitted this callback invocation.
    pub fencing_token: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum LeaseExecutionError<E> {
    #[error("lease acquisition failed: {0}")]
    Acquisition(FitzError),
    #[error("lease callback failed")]
    Callback(E),
    #[error("lease ownership lost: {0}")]
    OwnershipLost(FitzError),
    #[error("lease release failed: {0}")]
    Release(FitzError),
    #[error("lease lifecycle and callback both failed")]
    Combined { lifecycle: FitzError, callback: E },
}
#[derive(Clone)]
pub struct LeaseClient {
    connection: AsyncConnection,
    acquisition_gate: Arc<tokio::sync::Mutex<()>>,
}
impl LeaseClient {
    pub(crate) fn new(connection: AsyncConnection) -> Self {
        Self {
            connection,
            acquisition_gate: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn acquire(
        &self,
        route: &str,
        owner_id: &str,
        ttl_secs: u64,
        options: LeaseAcquireOptions,
    ) -> Result<LeaseHandle> {
        validate_fixed_route(route, "lease", 3)?;
        let _acquisition_guard = self.acquisition_gate.lock().await;
        let mut deferred = self
            .connection
            .notifications(message_type::LEASE_ACQUIRE, 16);
        let mut e = PayloadEncoder::new();
        e.put_string(route)
            .put_string(owner_id)
            .put_u64(ttl_secs)
            .put_u32(options.wait_seconds);
        let response = self
            .connection
            .request(message_type::LEASE_ACQUIRE, e.finish())
            .await?;
        let mut d = lease_success(&response, "ACQUIRE")?;
        let kind = d.get_u8()?;
        let fencing_token = if kind < 2 {
            d.get_u64()?
        } else if kind <= 3 && options.wait_seconds > 0 {
            let payload = deferred
                .recv()
                .await
                .map_err(|_| FitzError::ConnectionClosed)?;
            let mut completion = lease_success(&payload, "ACQUIRE")?;
            let completion_kind = completion.get_u8()?;
            if completion_kind > 1 {
                return Err(FitzError::Protocol(
                    "deferred ACQUIRE remained queued".into(),
                ));
            }
            completion.get_u64()?
        } else {
            return Err(FitzError::Domain {
                code: 5001,
                message: "lease acquisition queued".into(),
            });
        };
        Ok(LeaseHandle {
            connection: self.connection.clone(),
            generation: self.connection.generation(),
            route: route.into(),
            owner_id: owner_id.into(),
            fencing_token,
            released: false,
        })
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn list(
        &self,
        pattern: &str,
        cursor: Option<LeaseListCursor>,
        limit: u32,
    ) -> Result<LeaseListPage> {
        validate_registration_pattern(pattern, "lease", 3)?;
        let mut e = PayloadEncoder::new();
        e.put_string(pattern);
        match cursor {
            Some(cursor) => {
                e.put_u8(1)
                    .put_u64(cursor.snapshot_id)
                    .put_u32(cursor.offset);
            }
            None => {
                e.put_u8(0);
            }
        }
        e.put_u32(limit);
        let response = self
            .connection
            .request_replayable(message_type::LEASE_LIST, e.finish())
            .await?;
        decode_list_page(lease_success(&response, "LIST")?)
    }
    /// Opens a race-safe, self-healing observer over every Lease route matching
    /// `pattern`.
    ///
    /// The returned [`LeaseObserver`] owns the bootstrap sequence required to
    /// build a correct inventory view (subscribe, buffer, list-to-completion,
    /// install, drain), keeps that view current with complete `LIST`
    /// reconciliations as `LEASE_NOTIFY` events arrive, backstops it with a periodic
    /// jittered full relist, and re-bootstraps from scratch whenever the
    /// underlying connection reconnects. Callers never hand-roll this
    /// sequence themselves.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing
    /// fails while establishing the initial subscription.
    pub async fn observe(&self, pattern: &str, options: ObserveOptions) -> Result<LeaseObserver> {
        validate_registration_pattern(pattern, "lease", 3)?;
        if options.reconciliation_interval.is_zero() {
            return Err(FitzError::Protocol(
                "Lease observer reconciliation interval must be positive".into(),
            ));
        }
        let client = self.clone();
        let subscription = client.subscribe(pattern).await?;

        let view: Arc<RwLock<HashMap<String, LeaseListItem>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let ready = Arc::new(AtomicBool::new(false));
        let shutdown = CancellationToken::new();

        let task = tokio::spawn(observe_loop(
            client,
            pattern.to_string(),
            options,
            subscription,
            Arc::clone(&view),
            Arc::clone(&ready),
            shutdown.clone(),
        ));

        Ok(LeaseObserver {
            view,
            ready,
            shutdown,
            task: Some(task),
        })
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn query(&self, route: &str) -> Result<LeaseInfo> {
        validate_fixed_route(route, "lease", 3)?;
        let mut e = PayloadEncoder::new();
        e.put_string(route);
        let response = self
            .connection
            .request_replayable(message_type::LEASE_QUERY, e.finish())
            .await?;
        let mut d = lease_success(&response, "QUERY")?;
        let held = d.get_u8()? == 1;
        if held {
            Ok(LeaseInfo {
                held,
                owner_id: Some(d.get_string()?),
                ttl_remaining_secs: Some(d.get_u64()?),
                pending_waiters: d.get_u32()?,
            })
        } else {
            Ok(LeaseInfo {
                held,
                owner_id: None,
                ttl_remaining_secs: None,
                pending_waiters: d.get_u32()?,
            })
        }
    }
    /// Subscribes to Lease change notifications for every route matching
    /// `pattern`. Like every other domain's `subscribe`, this accepts the
    /// full registration-pattern grammar (`*`/`**` wildcards), not just
    /// concrete routes: it is the direct-caller counterpart to the internal
    /// bootstrap subscription [`LeaseClient::observe`] uses, and shares its
    /// validation deliberately for consistency across the client surface.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn subscribe(&self, pattern: &str) -> Result<LeaseSubscription> {
        validate_registration_pattern(pattern, "lease", 3)?;
        let receiver = self
            .connection
            .notifications(message_type::LEASE_NOTIFY, 64);
        let mut e = PayloadEncoder::new();
        e.put_string(pattern);
        let payload = e.finish();
        let response = self
            .connection
            .request(message_type::LEASE_SUBSCRIBE, payload.clone())
            .await?;
        let subscription_id = decode_subscription_id(&response)?;
        let registration = self.connection.register_restorable(
            message_type::LEASE_SUBSCRIBE,
            payload,
            subscription_id,
            decode_subscription_id,
        );
        Ok(LeaseSubscription {
            connection: self.connection.clone(),
            route: pattern.into(),
            registration,
            receiver: BroadcastStream::new(receiver),
            closed: false,
        })
    }

    /// Runs a callback while the lease is owned and supervised.
    ///
    /// # Errors
    /// Returns typed acquisition, callback, ownership-loss, release, or combined failures.
    pub async fn with_lease<F, Fut, T, E>(
        &self,
        route: &str,
        owner_id: &str,
        ttl_secs: u64,
        callback: F,
    ) -> std::result::Result<T, LeaseExecutionError<E>>
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        self.with_lease_authority(
            route,
            owner_id,
            ttl_secs,
            move |cancellation, _authority| callback(cancellation),
        )
        .await
    }

    /// Runs a callback while the lease is owned with explicit acquisition behavior.
    ///
    /// # Errors
    /// Returns typed acquisition, callback, ownership-loss, release, or combined failures.
    pub async fn with_lease_with_options<F, Fut, T, E>(
        &self,
        route: &str,
        owner_id: &str,
        ttl_secs: u64,
        callback: F,
        options: LeaseExecutionOptions,
    ) -> std::result::Result<T, LeaseExecutionError<E>>
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        self.with_lease_authority_with_options(
            route,
            owner_id,
            ttl_secs,
            move |cancellation, _authority| callback(cancellation),
            options,
        )
        .await
    }

    /// Runs a callback with immutable admission authority while the lease is supervised.
    ///
    /// # Errors
    /// Returns typed acquisition, callback, ownership-loss, release, or combined failures.
    pub async fn with_lease_authority<F, Fut, T, E>(
        &self,
        route: &str,
        owner_id: &str,
        ttl_secs: u64,
        callback: F,
    ) -> std::result::Result<T, LeaseExecutionError<E>>
    where
        F: FnOnce(CancellationToken, LeaseAuthority) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        self.with_lease_authority_with_options(
            route,
            owner_id,
            ttl_secs,
            callback,
            LeaseExecutionOptions::default(),
        )
        .await
    }

    /// Runs an authority-aware callback with explicit acquisition behavior.
    ///
    /// # Errors
    /// Returns typed acquisition, callback, ownership-loss, release, or combined failures.
    pub async fn with_lease_authority_with_options<F, Fut, T, E>(
        &self,
        route: &str,
        owner_id: &str,
        ttl_secs: u64,
        callback: F,
        options: LeaseExecutionOptions,
    ) -> std::result::Result<T, LeaseExecutionError<E>>
    where
        F: FnOnce(CancellationToken, LeaseAuthority) -> Fut + Send + 'static,
        Fut: Future<Output = std::result::Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        validate_fixed_route(route, "lease", 3).map_err(LeaseExecutionError::Acquisition)?;
        if ttl_secs == 0 || ttl_secs > u64::from(u32::MAX) / 1_000 {
            return Err(LeaseExecutionError::Acquisition(FitzError::Protocol(
                "lease TTL must be positive and schedulable".into(),
            )));
        }
        let mut handle = self
            .acquire(
                route,
                owner_id,
                ttl_secs,
                LeaseAcquireOptions {
                    wait_seconds: options.wait_seconds,
                },
            )
            .await
            .map_err(LeaseExecutionError::Acquisition)?;

        let authority = LeaseAuthority {
            fencing_token: handle.fencing_token,
        };
        let cancellation = CancellationToken::new();
        let callback_task =
            AssertUnwindSafe(callback(cancellation.clone(), authority)).catch_unwind();
        tokio::pin!(callback_task);
        let renewal = tokio::time::sleep(Duration::from_secs(ttl_secs) / 3);
        tokio::pin!(renewal);
        loop {
            tokio::select! {
                biased;
                callback_result = &mut callback_task => {
                    let outcome = callback_result;
                    let release_result = handle.release().await;
                    let callback_result = match outcome {
                        Ok(result) => result,
                        Err(panic) => std::panic::resume_unwind(panic),
                    };
                    return match (callback_result, release_result) {
                        (Ok(value), Ok(())) => Ok(value),
                        (Err(error), Ok(())) => Err(LeaseExecutionError::Callback(error)),
                        (Ok(_), Err(error)) => Err(LeaseExecutionError::Release(error)),
                        (Err(callback), Err(lifecycle)) => Err(LeaseExecutionError::Combined { lifecycle, callback }),
                    };
                }
                () = &mut renewal => {
                    match handle.extend(ttl_secs).await {
                        Ok(()) => renewal.as_mut().reset(tokio::time::Instant::now() + Duration::from_secs(ttl_secs) / 3),
                        Err(error) => {
                            cancellation.cancel();
                            let outcome = callback_task.await;
                            let callback_result = match outcome {
                                Ok(result) => result,
                                Err(panic) => std::panic::resume_unwind(panic),
                            };
                            return match callback_result {
                                Ok(_) => Err(LeaseExecutionError::OwnershipLost(error)),
                                Err(callback) => Err(LeaseExecutionError::Combined { lifecycle: error, callback }),
                            };
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseInfo {
    pub held: bool,
    pub owner_id: Option<String>,
    pub ttl_remaining_secs: Option<u64>,
    pub pending_waiters: u32,
}

/// Opaque continuation token for paging through one `LIST` scan.
///
/// Reusing this cursor with a different pattern, a different `RouteFamily`,
/// after the snapshot is unknown/evicted, or after a broker restart fails
/// with `ERR_INVALID_LIST_CURSOR` (5011).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseListCursor {
    pub snapshot_id: u64,
    pub offset: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseListItem {
    pub route: String,
    /// The logical `owner_id` the caller passed to ACQUIRE; never a raw session id.
    pub owner_id: String,
    /// Opaque; stable for one live session, distinct per session/reconnect.
    pub holder_incarnation: u64,
    /// RFC3339 timestamp string.
    pub acquired_at: String,
    pub expires_in_secs: u64,
    pub renewals: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseListPage {
    pub items: Vec<LeaseListItem>,
    pub next_cursor: Option<LeaseListCursor>,
}

/// Options for [`LeaseClient::observe`].
#[derive(Debug, Clone, Copy)]
pub struct ObserveOptions {
    /// Base interval between full-relist backstop reconciliations. The
    /// interval actually used for each tick is jittered by up to ±20% so a
    /// fleet of observers does not all reconcile in lockstep. For a known
    /// workload, use `clamp(shortest expected lease TTL / 2, 5s, 60s)`:
    /// this targets two backstop passes during the shortest
    /// lease lifetime without polling faster than one bounded full LIST
    /// every five seconds per observer.
    pub reconciliation_interval: Duration,
    /// Page size used while paging LIST to completion during bootstrap and
    /// reconciliation.
    pub list_page_size: u32,
}

impl Default for ObserveOptions {
    fn default() -> Self {
        Self {
            reconciliation_interval: Duration::from_secs(60),
            list_page_size: 200,
        }
    }
}

/// Race-safe, self-healing view over every Lease route matching one
/// selector.
///
/// Returned by [`LeaseClient::observe`]. See that method's documentation for
/// the bootstrap and steady-state contract this type implements.
pub struct LeaseObserver {
    view: Arc<RwLock<HashMap<String, LeaseListItem>>>,
    ready: Arc<AtomicBool>,
    shutdown: CancellationToken,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl LeaseObserver {
    /// Returns whether the observer has completed its initial bootstrap (or
    /// a post-reconnect re-bootstrap) and [`Self::snapshot`] reflects a
    /// coherent view.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    /// Returns a point-in-time snapshot of every currently observed item.
    #[must_use]
    pub fn snapshot(&self) -> Vec<LeaseListItem> {
        self.view.read().values().cloned().collect()
    }

    /// Stops the observer's background work and unsubscribes, waiting for
    /// that teardown to complete.
    pub async fn close(mut self) {
        self.shutdown.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for LeaseObserver {
    fn drop(&mut self) {
        // Best-effort, fire-and-forget teardown: cancel the background task
        // and let it unsubscribe and exit on its own. Callers who need to
        // know teardown has finished should call `close().await` instead.
        self.shutdown.cancel();
    }
}

enum SteadyStateExit {
    Shutdown,
    Reconnected,
    SubscriptionFailed,
}

async fn observe_loop(
    client: LeaseClient,
    pattern: String,
    options: ObserveOptions,
    mut subscription: LeaseSubscription,
    view: Arc<RwLock<HashMap<String, LeaseListItem>>>,
    ready: Arc<AtomicBool>,
    shutdown: CancellationToken,
) {
    'outer: loop {
        if shutdown.is_cancelled() {
            break;
        }

        // Captured before bootstrapping so a reconnect that happens *during*
        // bootstrap (between here and steady_state starting to watch) is
        // still detected, instead of silently being adopted as the new
        // baseline and never noticed.
        let baseline_generation = client.connection.generation();

        // Steps 1-2 (subscription + buffering) are already established: the
        // subscription was created before this task started, so any
        // LEASE_NOTIFY for our pattern is already queued in its broadcast
        // buffer even though nothing has polled it yet.
        //
        // Steps 3-5: list to completion, install, then drain and reconcile
        // whatever arrived while we were listing.
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break 'outer,
                result = bootstrap(&client, &pattern, options.list_page_size, &mut subscription, &view, &ready) => {
                    match result {
                        Ok(()) => break,
                        Err(error) if subscription_must_be_replaced(&error) => {
                            ready.store(false, Ordering::Release);
                            if !replace_observer_subscription(
                                &client,
                                &pattern,
                                &mut subscription,
                                &shutdown,
                            ).await {
                                break 'outer;
                            }
                        }
                        Err(_) => {}
                    }
                }
            }
            tokio::select! {
                () = shutdown.cancelled() => break 'outer,
                () = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
        }

        // A reconnect during bootstrap means whatever we just installed may
        // already be stale (or came from a subscription that hadn't
        // finished replaying yet); redo the whole bootstrap rather than
        // trusting it.
        if client.connection.generation() != baseline_generation {
            ready.store(false, Ordering::Release);
            continue;
        }

        // Steps 6-8: steady state until shutdown or a reconnect is observed.
        match steady_state(
            &client,
            &pattern,
            &view,
            &mut subscription,
            &shutdown,
            options,
            baseline_generation,
        )
        .await
        {
            SteadyStateExit::Shutdown => break,
            SteadyStateExit::Reconnected => {
                ready.store(false, Ordering::Release);
            }
            SteadyStateExit::SubscriptionFailed => {
                ready.store(false, Ordering::Release);
                if !replace_observer_subscription(&client, &pattern, &mut subscription, &shutdown)
                    .await
                {
                    break;
                }
            }
        }
    }
    let _ = subscription.unsubscribe().await;
}

fn subscription_must_be_replaced(error: &FitzError) -> bool {
    matches!(
        error,
        FitzError::Backpressure(_) | FitzError::ConnectionClosed
    )
}

async fn replace_observer_subscription(
    client: &LeaseClient,
    pattern: &str,
    subscription: &mut LeaseSubscription,
    shutdown: &CancellationToken,
) -> bool {
    tokio::select! {
        () = shutdown.cancelled() => return false,
        _ = subscription.unsubscribe_in_place() => {}
    }

    let mut attempts = 0_u32;
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return false,
            replacement = client.subscribe(pattern) => {
                if let Ok(replacement) = replacement {
                    *subscription = replacement;
                    return true;
                }
            }
        }
        backoff_epoch_race(&mut attempts).await;
    }
}

/// Steps 3-5: list the selector to completion and install only after a pass
/// completes without a buffered invalidation.
async fn bootstrap(
    client: &LeaseClient,
    pattern: &str,
    page_size: u32,
    subscription: &mut LeaseSubscription,
    view: &Arc<RwLock<HashMap<String, LeaseListItem>>>,
    ready: &Arc<AtomicBool>,
) -> Result<()> {
    let mut epoch_race_attempts = 0_u32;
    loop {
        let notification_epoch = client
            .connection
            .notification_epoch(message_type::LEASE_NOTIFY);
        let fresh = full_list(client, pattern, page_size).await?;
        if drain_invalidation_buffer(subscription)? {
            epoch_race_attempts = 0;
            continue;
        }
        if client.connection.install_if_notification_epoch(
            message_type::LEASE_NOTIFY,
            notification_epoch,
            || {
                *view.write() = fresh;
                ready.store(true, Ordering::Release);
            },
        ) {
            return Ok(());
        }
        // Defense in depth: notification_epoch is now scoped to
        // message_type::LEASE_NOTIFY, so this branch should only ever be hit
        // by a genuine Lease-notification race, not unrelated traffic on a
        // shared connection. Back off anyway rather than re-listing at full
        // speed, in case that assumption is ever violated.
        backoff_epoch_race(&mut epoch_race_attempts).await;
    }
}

/// Bounded, capped backoff used between epoch-race retries in the bootstrap
/// and reconciliation loops, so a persistent epoch race degrades to a slow
/// steady retry instead of a zero-delay spin against the broker.
async fn backoff_epoch_race(attempts: &mut u32) {
    const BASE: Duration = Duration::from_millis(5);
    const MAX: Duration = Duration::from_millis(200);
    *attempts = attempts.saturating_add(1);
    let delay = BASE.saturating_mul(1_u32 << (*attempts).min(6)).min(MAX);
    tokio::time::sleep(delay).await;
}

/// Drains every notification already buffered for the observer. Returns true
/// when at least one invalidation was present; subscription overflow or closure
/// invalidates the view and is returned as an error.
fn drain_invalidation_buffer(subscription: &mut LeaseSubscription) -> Result<bool> {
    let mut invalidated = false;
    loop {
        match subscription.next().now_or_never() {
            Some(Some(Ok(_))) => invalidated = true,
            Some(Some(Err(error))) => return Err(error),
            Some(None) => return Err(FitzError::ConnectionClosed),
            None => return Ok(invalidated),
        }
    }
}

async fn full_list(
    client: &LeaseClient,
    pattern: &str,
    page_size: u32,
) -> Result<HashMap<String, LeaseListItem>> {
    let mut items = HashMap::new();
    let mut cursor = None;
    loop {
        let page = client.list(pattern, cursor, page_size).await?;
        for item in page.items {
            items.insert(item.route.clone(), item);
        }
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => return Ok(items),
        }
    }
}

async fn steady_state(
    client: &LeaseClient,
    pattern: &str,
    view: &Arc<RwLock<HashMap<String, LeaseListItem>>>,
    subscription: &mut LeaseSubscription,
    shutdown: &CancellationToken,
    options: ObserveOptions,
    generation: u64,
) -> SteadyStateExit {
    let reconciliation = tokio::time::sleep(jittered(options.reconciliation_interval));
    tokio::pin!(reconciliation);
    // Event-driven reconnect detection: `generation_changes()` resolves as
    // soon as the connection's generation is bumped, instead of polling
    // `generation()` on a timer. A change is still double-checked against
    // the baseline `generation` below in case of a stale/duplicate wakeup.
    let mut generation_changes = client.connection.generation_changes();
    // Close the small race between observe_loop's pre-steady-state
    // generation check and subscribing to the watch channel: if a reconnect
    // landed in that window, this receiver starts at the new value and would
    // otherwise wait forever for a second change.
    if client.connection.generation() != generation {
        return SteadyStateExit::Reconnected;
    }
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return SteadyStateExit::Shutdown,
            changed = generation_changes.changed() => {
                if changed.is_err() || client.connection.generation() != generation {
                    return SteadyStateExit::Reconnected;
                }
            }
            notification = subscription.next() => {
                match notification {
                    Some(Ok(_)) => {
                        match reconcile_after_invalidation(
                            client,
                            pattern,
                            options.list_page_size,
                            subscription,
                            view,
                        )
                        .await
                        {
                            Ok(()) => {}
                            Err(error) if subscription_must_be_replaced(&error) => {
                                return SteadyStateExit::SubscriptionFailed;
                            }
                            Err(_) => return SteadyStateExit::Reconnected,
                        }
                    }
                    Some(Err(_)) | None => return SteadyStateExit::SubscriptionFailed,
                }
            }
            () = &mut reconciliation => {
                match reconcile_after_invalidation(
                    client,
                    pattern,
                    options.list_page_size,
                    subscription,
                    view,
                ).await {
                    Err(error) if subscription_must_be_replaced(&error) => {
                        return SteadyStateExit::SubscriptionFailed;
                    }
                    _ => {}
                }
                reconciliation
                    .as_mut()
                    .reset(tokio::time::Instant::now() + jittered(options.reconciliation_interval));
            }
        }
    }
}

/// Reconcile after a live invalidation. QUERY cannot reconstruct a complete
/// `LeaseListItem` (it omits holder incarnation, acquisition time, and renewal
/// count), so stay on LIST and coalesce every notification buffered during the
/// pass into another pass before installing.
async fn reconcile_after_invalidation(
    client: &LeaseClient,
    pattern: &str,
    page_size: u32,
    subscription: &mut LeaseSubscription,
    view: &Arc<RwLock<HashMap<String, LeaseListItem>>>,
) -> Result<()> {
    let mut epoch_race_attempts = 0_u32;
    loop {
        let notification_epoch = client
            .connection
            .notification_epoch(message_type::LEASE_NOTIFY);
        let fresh = full_list(client, pattern, page_size).await?;
        if drain_invalidation_buffer(subscription)? {
            epoch_race_attempts = 0;
            continue;
        }
        if client.connection.install_if_notification_epoch(
            message_type::LEASE_NOTIFY,
            notification_epoch,
            || {
                *view.write() = fresh;
            },
        ) {
            return Ok(());
        }
        backoff_epoch_race(&mut epoch_race_attempts).await;
    }
}

/// Applies deterministic-but-unpredictable ±20% jitter to `base` so a fleet
/// of observers does not all reconcile in lockstep.
fn jittered(base: Duration) -> Duration {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::sync::atomic::AtomicU64;
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let mut hasher = DefaultHasher::new();
    std::time::SystemTime::now().hash(&mut hasher);
    COUNTER.fetch_add(1, Ordering::Relaxed).hash(&mut hasher);
    let unit = f64::from(u32::try_from(hasher.finish() % 10_000).unwrap_or(0)) / 10_000.0; // [0, 1)
    base.mul_f64(0.8 + unit * 0.4) // [0.8, 1.2)
}

pub struct LeaseHandle {
    connection: AsyncConnection,
    generation: u64,
    route: String,
    owner_id: String,
    fencing_token: u64,
    released: bool,
}
impl LeaseHandle {
    fn current(&self) -> Result<()> {
        if self.released || self.connection.generation() != self.generation {
            Err(FitzError::StaleHandle)
        } else {
            Ok(())
        }
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn extend(&mut self, ttl_secs: u64) -> Result<()> {
        self.current()?;
        let mut e = PayloadEncoder::new();
        e.put_string(&self.route)
            .put_string(&self.owner_id)
            .put_u64(self.fencing_token)
            .put_u64(ttl_secs);
        let response = self
            .connection
            .request(message_type::LEASE_RENEW, e.finish())
            .await?;
        let mut d = lease_success(&response, "RENEW")?;
        self.fencing_token = d.get_u64()?;
        Ok(())
    }
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn release(mut self) -> Result<()> {
        self.current()?;
        let mut e = PayloadEncoder::new();
        e.put_string(&self.route)
            .put_string(&self.owner_id)
            .put_u64(self.fencing_token);
        lease_success(
            &self
                .connection
                .request(message_type::LEASE_RELEASE, e.finish())
                .await?,
            "RELEASE",
        )?;
        self.released = true;
        Ok(())
    }
}

impl Drop for LeaseHandle {
    fn drop(&mut self) {
        if self.released || self.connection.generation() != self.generation {
            return;
        }
        self.released = true;
        let connection = self.connection.clone();
        let route = self.route.clone();
        let owner_id = self.owner_id.clone();
        let fencing_token = self.fencing_token;
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let mut e = PayloadEncoder::new();
                e.put_string(&route)
                    .put_string(&owner_id)
                    .put_u64(fencing_token);
                let _ = connection
                    .request(message_type::LEASE_RELEASE, e.finish())
                    .await;
            });
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseChangeNotification {
    pub route: String,
}
pub struct LeaseSubscription {
    connection: AsyncConnection,
    route: String,
    registration: RestorableRegistration,
    receiver: BroadcastStream<Vec<u8>>,
    closed: bool,
}
impl LeaseSubscription {
    /// Performs the operation asynchronously.
    ///
    /// # Errors
    /// Returns an error when validation, transport, or broker processing fails.
    pub async fn unsubscribe(mut self) -> Result<()> {
        self.unsubscribe_in_place().await
    }

    async fn unsubscribe_in_place(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.registration.deactivate();
        let mut e = PayloadEncoder::new();
        e.put_string(&self.route);
        lease_success(
            &self
                .connection
                .request(message_type::LEASE_UNSUBSCRIBE, e.finish())
                .await?,
            "UNSUBSCRIBE",
        )?;
        Ok(())
    }
}
impl Stream for LeaseSubscription {
    type Item = Result<LeaseChangeNotification>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.closed {
            return Poll::Ready(None);
        }
        loop {
            match Pin::new(&mut self.receiver).poll_next(cx) {
                Poll::Ready(Some(Ok(payload))) => {
                    let mut d = PayloadDecoder::new(&payload);
                    let id = match d.get_u64() {
                        Ok(v) => v,
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    };
                    if id != self.registration.wire_id() {
                        continue;
                    }
                    return Poll::Ready(Some(
                        d.get_string()
                            .map(|route| LeaseChangeNotification { route }),
                    ));
                }
                Poll::Ready(Some(Err(_))) => {
                    return Poll::Ready(Some(Err(FitzError::Backpressure(
                        "Lease subscription buffer is full".into(),
                    ))));
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

fn decode_subscription_id(response: &[u8]) -> Result<u64> {
    let mut decoder = lease_success(response, "SUBSCRIBE")?;
    let subscription_id = decoder.get_u64()?;
    if !decoder.is_empty() {
        return Err(FitzError::Protocol(
            "lease SUBSCRIBE response has trailing bytes".into(),
        ));
    }
    Ok(subscription_id)
}
fn decode_list_page(mut d: PayloadDecoder<'_>) -> Result<LeaseListPage> {
    const MIN_ITEM_WIRE_BYTES: usize = 4 + 4 + 8 + 4 + 8 + 4;
    let item_count = d.get_u32()?;
    let declared_count = usize::try_from(item_count).unwrap_or(usize::MAX);
    let plausible_count = d.remaining() / MIN_ITEM_WIRE_BYTES;
    let mut items = Vec::with_capacity(declared_count.min(plausible_count));
    for _ in 0..item_count {
        items.push(LeaseListItem {
            route: d.get_string()?,
            owner_id: d.get_string()?,
            holder_incarnation: d.get_u64()?,
            acquired_at: d.get_string()?,
            expires_in_secs: d.get_u64()?,
            renewals: d.get_u32()?,
        });
    }
    let has_next = d.get_u8()?;
    let next_cursor = match has_next {
        0 => None,
        1 => Some(LeaseListCursor {
            snapshot_id: d.get_u64()?,
            offset: d.get_u32()?,
        }),
        v => {
            return Err(FitzError::Protocol(format!(
                "Lease LIST response has invalid has_next byte {v}"
            )));
        }
    };
    if !d.is_empty() {
        return Err(FitzError::Protocol(
            "Lease LIST response has trailing bytes".into(),
        ));
    }
    Ok(LeaseListPage { items, next_cursor })
}

fn lease_success<'a>(response: &'a [u8], operation: &str) -> Result<PayloadDecoder<'a>> {
    let mut d = PayloadDecoder::new(response);
    match d.get_u8()? {
        0 => Ok(d),
        1 => {
            let code = d.get_u32()?;
            let message = d.get_string()?;
            if !d.is_empty() {
                return Err(FitzError::Protocol(format!(
                    "Lease {operation} error response has trailing bytes"
                )));
            }
            Err(FitzError::Domain { code, message })
        }
        v => Err(FitzError::Protocol(format!(
            "Lease {operation} returned status {v}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::async_connection::AsyncConnectionOptions;
    use crate::{
        ConnectionState, FitzObservability, HeartbeatOptions, ReconnectPolicy, RetryPolicy,
    };
    use std::convert::Infallible;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::{oneshot, watch};
    use tokio::task::JoinHandle;

    async fn read_frame(stream: &mut TcpStream) -> (u16, Vec<u8>) {
        let len = stream.read_u32().await.unwrap() as usize;
        let mut frame = vec![0; len];
        stream.read_exact(&mut frame).await.unwrap();
        let (kind, start) = crate::codec::decode_message_frame(&frame).unwrap();
        (kind, frame[start..].to_vec())
    }
    async fn write_frame(stream: &mut TcpStream, kind: u16, payload: &[u8]) {
        let frame = crate::codec::try_encode_message_frame(kind, payload).unwrap();
        stream
            .write_u32(u32::try_from(frame.len()).unwrap())
            .await
            .unwrap();
        stream.write_all(&frame).await.unwrap();
        stream.flush().await.unwrap();
    }

    async fn read_split(reader: &mut tokio::net::tcp::OwnedReadHalf) -> (u16, Vec<u8>) {
        let len = reader.read_u32().await.unwrap() as usize;
        let mut frame = vec![0; len];
        reader.read_exact(&mut frame).await.unwrap();
        let (kind, start) = crate::codec::decode_message_frame(&frame).unwrap();
        (kind, frame[start..].to_vec())
    }

    async fn write_split(writer: &mut tokio::net::tcp::OwnedWriteHalf, kind: u16, payload: &[u8]) {
        let frame = crate::codec::try_encode_message_frame(kind, payload).unwrap();
        writer
            .write_u32(u32::try_from(frame.len()).unwrap())
            .await
            .unwrap();
        writer.write_all(&frame).await.unwrap();
        writer.flush().await.unwrap();
    }

    async fn connected_lease_client<F, Fut>(
        script: F,
    ) -> (LeaseClient, AsyncConnection, JoinHandle<()>)
    where
        F: FnOnce(TcpStream) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            assert_eq!(read_frame(&mut stream).await.0, message_type::CONNECT);
            script(stream).await;
        });
        let (state, _) = watch::channel(ConnectionState::Disconnected);
        let connection = AsyncConnection::spawn(AsyncConnectionOptions {
            endpoint: format!("tcp://{address}"),
            token_provider: Arc::new(|| async { Ok(String::new()) }),
            timeout: Duration::from_secs(1),
            max_queued: 8,
            reconnect: ReconnectPolicy {
                enabled: false,
                ..ReconnectPolicy::default()
            },
            retry: RetryPolicy::default(),
            heartbeat: HeartbeatOptions::default(),
            observability: FitzObservability::default(),
            state,
        });
        connection.connect().await.unwrap();
        let client = LeaseClient::new(connection.clone());
        (client, connection, server)
    }

    fn acquire_response(kind: u8, fencing_token: u64) -> Vec<u8> {
        let mut response = PayloadEncoder::new();
        response.put_u8(0).put_u8(kind).put_u64(fencing_token);
        response.finish()
    }

    fn token_response(fencing_token: u64) -> Vec<u8> {
        let mut response = PayloadEncoder::new();
        response.put_u8(0).put_u64(fencing_token);
        response.finish()
    }

    fn error_response(code: u32, message: &str) -> Vec<u8> {
        let mut response = PayloadEncoder::new();
        response.put_u8(1).put_u32(code).put_string(message);
        response.finish()
    }

    fn trailing_u64(payload: &[u8]) -> u64 {
        u64::from_be_bytes(payload[payload.len() - 8..].try_into().unwrap())
    }

    fn renewal_token(payload: &[u8]) -> u64 {
        u64::from_be_bytes(
            payload[payload.len() - 16..payload.len() - 8]
                .try_into()
                .unwrap(),
        )
    }

    fn ok_response() -> Vec<u8> {
        let mut e = PayloadEncoder::new();
        e.put_u8(0);
        e.finish()
    }

    fn notify_payload(id: u64, route: &str) -> Vec<u8> {
        let mut e = PayloadEncoder::new();
        e.put_u64(id).put_string(route);
        e.finish()
    }

    async fn wait_until(mut predicate: impl FnMut() -> bool) {
        loop {
            if predicate() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    #[test]
    fn should_preserve_domain_code_given_typed_error_when_decoding_lease_response() {
        // Arrange: build the server's canonical typed Lease error envelope.
        let mut response = PayloadEncoder::new();
        response
            .put_u8(1)
            .put_u32(5001)
            .put_string("HeldByOther: worker-1");

        // Act: decode the response through the shared Lease status parser.
        let Err(error) = lease_success(&response.finish(), "ACQUIRE") else {
            panic!("typed Lease error decoded as success");
        };

        // Assert: retain the numeric domain code used by retry and callers.
        assert!(matches!(error, FitzError::Domain { code: 5001, .. }));
    }

    #[tokio::test]
    async fn should_pass_immediate_acquisition_authority_to_callback() {
        // Arrange: grant token 42 and require managed cleanup to use the same live credential.
        let (client, connection, server) = connected_lease_client(|mut stream| async move {
            assert_eq!(read_frame(&mut stream).await.0, message_type::LEASE_ACQUIRE);
            write_frame(
                &mut stream,
                message_type::LEASE_ACQUIRE,
                &acquire_response(0, 42),
            )
            .await;
            let (kind, payload) = read_frame(&mut stream).await;
            assert_eq!(kind, message_type::LEASE_RELEASE);
            assert_eq!(trailing_u64(&payload), 42);
            write_frame(&mut stream, message_type::LEASE_RELEASE, &[0]).await;
        })
        .await;

        // Act: enter the authority-aware managed lease API.
        let result = client
            .with_lease_authority(
                "lease://realm/area/resource",
                "worker-1",
                30,
                |_, authority| async move { Ok::<u64, Infallible>(authority.fencing_token) },
            )
            .await;

        // Assert: expose exactly the broker-issued admission fence and complete cleanup.
        assert_eq!(result.unwrap(), 42);
        server.await.unwrap();
        connection.close().await;
    }

    #[test]
    fn should_reject_huge_list_count_without_unbounded_preallocation() {
        // Arrange
        let mut payload = PayloadEncoder::new();
        payload.put_u32(u32::MAX);

        // Act
        let result = decode_list_page(PayloadDecoder::new(&payload.finish()));

        // Assert
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn should_pass_already_held_authority_to_callback() {
        // Arrange: return the idempotent AlreadyHeld success with token 43.
        let (client, connection, server) = connected_lease_client(|mut stream| async move {
            assert_eq!(read_frame(&mut stream).await.0, message_type::LEASE_ACQUIRE);
            write_frame(
                &mut stream,
                message_type::LEASE_ACQUIRE,
                &acquire_response(1, 43),
            )
            .await;
            assert_eq!(read_frame(&mut stream).await.0, message_type::LEASE_RELEASE);
            write_frame(&mut stream, message_type::LEASE_RELEASE, &[0]).await;
        })
        .await;

        // Act: run application code under the idempotently reacquired lease.
        let result = client
            .with_lease_authority(
                "lease://realm/area/resource",
                "worker-1",
                30,
                |_, authority| async move { Ok::<u64, Infallible>(authority.fencing_token) },
            )
            .await;

        // Assert: use the token carried by the successful AlreadyHeld response.
        assert_eq!(result.unwrap(), 43);
        server.await.unwrap();
        connection.close().await;
    }

    #[tokio::test]
    async fn should_pass_final_queued_authority_instead_of_provisional_token() {
        // Arrange: queue with provisional token 7, then admit the callback with token 42.
        let (client, connection, server) = connected_lease_client(|mut stream| async move {
            assert_eq!(read_frame(&mut stream).await.0, message_type::LEASE_ACQUIRE);
            write_frame(
                &mut stream,
                message_type::LEASE_ACQUIRE,
                &acquire_response(2, 7),
            )
            .await;
            write_frame(
                &mut stream,
                message_type::LEASE_ACQUIRE,
                &acquire_response(0, 42),
            )
            .await;
            let (kind, payload) = read_frame(&mut stream).await;
            assert_eq!(kind, message_type::LEASE_RELEASE);
            assert_eq!(trailing_u64(&payload), 42);
            write_frame(&mut stream, message_type::LEASE_RELEASE, &[0]).await;
        })
        .await;

        // Act: wait through the broker's deferred acquisition path.
        let result = client
            .with_lease_authority_with_options(
                "lease://realm/area/resource",
                "worker-1",
                30,
                |_, authority| async move { Ok::<u64, Infallible>(authority.fencing_token) },
                LeaseExecutionOptions { wait_seconds: 7 },
            )
            .await;

        // Assert: the final granted fence, never the queued placeholder, admits the callback.
        assert_eq!(result.unwrap(), 42);
        server.await.unwrap();
        connection.close().await;
    }

    #[tokio::test]
    async fn should_not_invoke_authority_callback_when_queued_acquisition_times_out() {
        // Arrange: queue the request, then return the broker's typed FIFO wait timeout.
        let invoked = Arc::new(AtomicBool::new(false));
        let callback_invoked = invoked.clone();
        let (client, connection, server) = connected_lease_client(|mut stream| async move {
            assert_eq!(read_frame(&mut stream).await.0, message_type::LEASE_ACQUIRE);
            write_frame(
                &mut stream,
                message_type::LEASE_ACQUIRE,
                &acquire_response(2, 7),
            )
            .await;
            write_frame(
                &mut stream,
                message_type::LEASE_ACQUIRE,
                &error_response(5006, "lease acquisition wait timed out"),
            )
            .await;
        })
        .await;

        // Act: wait for admission through the broker's bounded FIFO path.
        let result = client
            .with_lease_authority_with_options(
                "lease://realm/area/resource",
                "worker-1",
                30,
                move |_, _| async move {
                    callback_invoked.store(true, Ordering::SeqCst);
                    Ok::<(), Infallible>(())
                },
                LeaseExecutionOptions { wait_seconds: 7 },
            )
            .await;

        // Assert: preserve the typed timeout and never admit application work.
        assert!(matches!(
            result,
            Err(LeaseExecutionError::Acquisition(FitzError::Domain {
                code: 5006,
                ..
            }))
        ));
        assert!(!invoked.load(Ordering::SeqCst));
        server.await.unwrap();
        connection.close().await;
    }

    #[tokio::test]
    async fn should_keep_authority_stable_when_renewal_rotates_live_token() {
        // Arrange: grant token 42, rotate the handle to 99, and notify the held callback.
        let (renewed_tx, renewed_rx) = oneshot::channel();
        let (client, connection, server) = connected_lease_client(|mut stream| async move {
            assert_eq!(read_frame(&mut stream).await.0, message_type::LEASE_ACQUIRE);
            write_frame(
                &mut stream,
                message_type::LEASE_ACQUIRE,
                &acquire_response(0, 42),
            )
            .await;
            let (kind, payload) = read_frame(&mut stream).await;
            assert_eq!(kind, message_type::LEASE_RENEW);
            assert_eq!(renewal_token(&payload), 42);
            write_frame(&mut stream, message_type::LEASE_RENEW, &token_response(99)).await;
            renewed_tx.send(()).unwrap();
            let (kind, payload) = read_frame(&mut stream).await;
            assert_eq!(kind, message_type::LEASE_RELEASE);
            assert_eq!(trailing_u64(&payload), 99);
            write_frame(&mut stream, message_type::LEASE_RELEASE, &[0]).await;
        })
        .await;

        // Act: retain the admission snapshot across one successful managed renewal.
        let result = client
            .with_lease_authority(
                "lease://realm/area/resource",
                "worker-1",
                3,
                |_, authority| async move {
                    renewed_rx.await.unwrap();
                    Ok::<u64, Infallible>(authority.fencing_token)
                },
            )
            .await;

        // Assert: application authority remains 42 while release uses rotated token 99.
        assert_eq!(result.unwrap(), 42);
        server.await.unwrap();
        connection.close().await;
    }

    #[tokio::test]
    async fn should_not_invoke_authority_callback_when_acquisition_fails() {
        // Arrange: reject ACQUIRE and track whether application code ever starts.
        let invoked = Arc::new(AtomicBool::new(false));
        let callback_invoked = invoked.clone();
        let (client, connection, server) = connected_lease_client(|mut stream| async move {
            assert_eq!(read_frame(&mut stream).await.0, message_type::LEASE_ACQUIRE);
            write_frame(
                &mut stream,
                message_type::LEASE_ACQUIRE,
                &error_response(5001, "held by another owner"),
            )
            .await;
        })
        .await;

        // Act: attempt managed execution against the rejected acquisition.
        let result = client
            .with_lease_authority(
                "lease://realm/area/resource",
                "worker-1",
                30,
                move |_, _| async move {
                    callback_invoked.store(true, Ordering::SeqCst);
                    Ok::<(), Infallible>(())
                },
            )
            .await;

        // Assert: return the acquisition error without admitting application work.
        assert!(matches!(
            result,
            Err(LeaseExecutionError::Acquisition(FitzError::Domain {
                code: 5001,
                ..
            }))
        ));
        assert!(!invoked.load(Ordering::SeqCst));
        server.await.unwrap();
        connection.close().await;
    }

    #[tokio::test]
    async fn should_not_invoke_authority_callback_when_acquisition_is_canceled() {
        // Arrange: leave ACQUIRE queued and expose when the client starts waiting.
        let invoked = Arc::new(AtomicBool::new(false));
        let callback_invoked = invoked.clone();
        let (queued_tx, queued_rx) = oneshot::channel();
        let (client, connection, server) = connected_lease_client(|mut stream| async move {
            assert_eq!(read_frame(&mut stream).await.0, message_type::LEASE_ACQUIRE);
            write_frame(
                &mut stream,
                message_type::LEASE_ACQUIRE,
                &acquire_response(2, 7),
            )
            .await;
            queued_tx.send(()).unwrap();
            let mut closed = [0_u8; 1];
            let _ = stream.read(&mut closed).await;
        })
        .await;
        let execution = tokio::spawn(async move {
            client
                .with_lease_authority_with_options(
                    "lease://realm/area/resource",
                    "worker-1",
                    30,
                    move |_, _| async move {
                        callback_invoked.store(true, Ordering::SeqCst);
                        Ok::<(), Infallible>(())
                    },
                    LeaseExecutionOptions { wait_seconds: 30 },
                )
                .await
        });

        // Act: cancel the managed acquisition future after the queued response arrives.
        queued_rx.await.unwrap();
        execution.abort();
        let cancellation = execution.await.unwrap_err();
        connection.close().await;

        // Assert: cancellation stops before the application callback is admitted.
        assert!(cancellation.is_cancelled());
        assert!(!invoked.load(Ordering::SeqCst));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn should_keep_one_argument_managed_lease_callbacks_compatible() {
        // Arrange: serve one default and one options-based legacy managed execution.
        let (client, connection, server) = connected_lease_client(|mut stream| async move {
            for token in [41, 42] {
                assert_eq!(read_frame(&mut stream).await.0, message_type::LEASE_ACQUIRE);
                write_frame(
                    &mut stream,
                    message_type::LEASE_ACQUIRE,
                    &acquire_response(0, token),
                )
                .await;
                assert_eq!(read_frame(&mut stream).await.0, message_type::LEASE_RELEASE);
                write_frame(&mut stream, message_type::LEASE_RELEASE, &[0]).await;
            }
        })
        .await;

        // Act: call both original APIs with their original one-argument closure shape.
        let default_result = client
            .with_lease(
                "lease://realm/area/resource",
                "worker-1",
                30,
                |cancellation| async move { Ok::<bool, Infallible>(cancellation.is_cancelled()) },
            )
            .await;
        let options_result = client
            .with_lease_with_options(
                "lease://realm/area/resource",
                "worker-1",
                30,
                |cancellation| async move { Ok::<bool, Infallible>(cancellation.is_cancelled()) },
                LeaseExecutionOptions::default(),
            )
            .await;

        // Assert: both source-compatible wrappers retain their prior runtime result.
        assert!(!default_result.unwrap());
        assert!(!options_result.unwrap());
        server.await.unwrap();
        connection.close().await;
    }

    #[tokio::test]
    async fn should_accept_wildcard_route_when_subscribing() {
        // Arrange: script the broker accepting a whole-segment wildcard SUBSCRIBE.
        let (client, connection, server) = connected_lease_client(|mut stream| async move {
            let (kind, payload) = read_frame(&mut stream).await;
            assert_eq!(kind, message_type::LEASE_SUBSCRIBE);
            let mut d = PayloadDecoder::new(&payload);
            assert_eq!(d.get_string().unwrap(), "lease://acme/renderers/*");
            write_frame(
                &mut stream,
                message_type::LEASE_SUBSCRIBE,
                &token_response(1),
            )
            .await;
        })
        .await;

        // Act: subscribe using a widened wildcard selector.
        let result = client.subscribe("lease://acme/renderers/*").await;

        // Assert: the client no longer rejects whole-segment wildcards client-side.
        assert!(result.is_ok());
        drop(result);
        server.await.unwrap();
        connection.close().await;
    }

    #[test]
    fn should_reject_malformed_wildcard_routes_when_subscribing() {
        // Arrange: partial wildcards, wrong scheme, and wrong depth are never valid.
        let malformed = [
            "lease://acme/renderers/lock*",
            "notice://acme/renderers/*",
            "lease://acme/*",
            "lease://acme/renderers/*/extra",
        ];

        // Act
        let results = malformed.map(|route| validate_registration_pattern(route, "lease", 3));

        // Assert: every malformed selector is rejected before reaching the wire.
        assert!(results.iter().all(Result::is_err));
    }

    #[test]
    fn should_accept_full_wildcard_matrix_when_subscribing() {
        // Arrange: the complete literal-or-`*` matrix plus the `**` trailing alias.
        let valid = [
            "lease://acme/renderers/doc-1",
            "lease://acme/renderers/*",
            "lease://acme/*/doc-1",
            "lease://*/renderers/doc-1",
            "lease://acme/*/*",
            "lease://*/renderers/*",
            "lease://*/*/doc-1",
            "lease://*/*/*",
            "lease://acme/**",
            "lease://**",
        ];

        // Act
        let results = valid.map(|route| validate_registration_pattern(route, "lease", 3));

        // Assert
        assert!(results.iter().all(Result::is_ok));
    }

    fn list_response(items: &[LeaseListItem], next_cursor: Option<LeaseListCursor>) -> Vec<u8> {
        let mut e = PayloadEncoder::new();
        e.put_u8(0);
        e.put_u32(u32::try_from(items.len()).unwrap());
        for item in items {
            e.put_string(&item.route)
                .put_string(&item.owner_id)
                .put_u64(item.holder_incarnation)
                .put_string(&item.acquired_at)
                .put_u64(item.expires_in_secs)
                .put_u32(item.renewals);
        }
        match next_cursor {
            Some(cursor) => {
                e.put_u8(1)
                    .put_u64(cursor.snapshot_id)
                    .put_u32(cursor.offset);
            }
            None => {
                e.put_u8(0);
            }
        }
        e.finish()
    }

    fn sample_item() -> LeaseListItem {
        LeaseListItem {
            route: "lease://acme/renderers/doc-1".into(),
            owner_id: "worker-1".into(),
            holder_incarnation: 7,
            acquired_at: "2026-08-29T00:00:00Z".into(),
            expires_in_secs: 30,
            renewals: 2,
        }
    }

    #[tokio::test]
    async fn should_round_trip_list_page_without_cursor() {
        // Arrange: script a LIST response with one item and no continuation.
        let item = sample_item();
        let expected_item = item.clone();
        let (client, connection, server) = connected_lease_client(move |mut stream| async move {
            let (kind, payload) = read_frame(&mut stream).await;
            assert_eq!(kind, message_type::LEASE_LIST);
            let mut d = PayloadDecoder::new(&payload);
            assert_eq!(d.get_string().unwrap(), "lease://acme/renderers/*");
            assert_eq!(d.get_u8().unwrap(), 0); // has_cursor = false
            assert_eq!(d.get_u32().unwrap(), 50); // limit
            write_frame(
                &mut stream,
                message_type::LEASE_LIST,
                &list_response(&[item], None),
            )
            .await;
        })
        .await;

        // Act
        let page = client
            .list("lease://acme/renderers/*", None, 50)
            .await
            .unwrap();

        // Assert
        assert_eq!(page.items, vec![expected_item]);
        assert_eq!(page.next_cursor, None);
        server.await.unwrap();
        connection.close().await;
    }

    #[tokio::test]
    async fn should_continue_list_scan_using_returned_cursor() {
        // Arrange: script a paged LIST response carrying a continuation cursor.
        let item = sample_item();
        let cursor = LeaseListCursor {
            snapshot_id: 123,
            offset: 100,
        };
        let (client, connection, server) = connected_lease_client(move |mut stream| async move {
            let (kind, payload) = read_frame(&mut stream).await;
            assert_eq!(kind, message_type::LEASE_LIST);
            let mut d = PayloadDecoder::new(&payload);
            assert_eq!(d.get_string().unwrap(), "lease://**");
            assert_eq!(d.get_u8().unwrap(), 0);
            assert_eq!(d.get_u32().unwrap(), 100);
            write_frame(
                &mut stream,
                message_type::LEASE_LIST,
                &list_response(&[item], Some(cursor)),
            )
            .await;

            let (kind, payload) = read_frame(&mut stream).await;
            assert_eq!(kind, message_type::LEASE_LIST);
            let mut d = PayloadDecoder::new(&payload);
            assert_eq!(d.get_string().unwrap(), "lease://**");
            assert_eq!(d.get_u8().unwrap(), 1); // has_cursor = true
            assert_eq!(d.get_u64().unwrap(), 123);
            assert_eq!(d.get_u32().unwrap(), 100);
            assert_eq!(d.get_u32().unwrap(), 100); // limit
            write_frame(
                &mut stream,
                message_type::LEASE_LIST,
                &list_response(&[], None),
            )
            .await;
        })
        .await;

        // Act: page through the same scan using the returned cursor verbatim.
        let first = client.list("lease://**", None, 100).await.unwrap();
        let cursor = first.next_cursor.expect("first page should carry a cursor");
        let second = client.list("lease://**", Some(cursor), 100).await.unwrap();

        // Assert: the second page continues the same scan and terminates it.
        assert_eq!(second.items, vec![]);
        assert_eq!(second.next_cursor, None);
        server.await.unwrap();
        connection.close().await;
    }

    #[test]
    fn should_decode_invalid_list_cursor_error() {
        // Arrange: the broker's typed cursor-mismatch error envelope.
        let response = error_response(
            crate::error::error_code::LEASE_INVALID_LIST_CURSOR,
            "cursor does not match this scan",
        );

        // Act
        let Err(error) = lease_success(&response, "LIST") else {
            panic!("invalid LIST cursor decoded as success");
        };

        // Assert
        assert!(matches!(error, FitzError::Domain { code: 5011, .. }));
    }

    #[test]
    fn should_decode_invalid_list_pattern_error() {
        // Arrange: the broker's typed pattern-grammar error envelope.
        let response = error_response(
            crate::error::error_code::LEASE_INVALID_LIST_PATTERN,
            "pattern fails the wildcard grammar",
        );

        // Act
        let Err(error) = lease_success(&response, "LIST") else {
            panic!("invalid LIST pattern decoded as success");
        };

        // Assert
        assert!(matches!(error, FitzError::Domain { code: 5012, .. }));
    }

    #[test]
    fn should_reject_malformed_list_pattern_client_side() {
        // Arrange: a partial wildcard is never valid, even for LIST.
        let malformed = "lease://acme/renderers/lock*";

        // Act
        let result = validate_registration_pattern(malformed, "lease", 3);

        // Assert
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn should_converge_bootstrap_despite_continuous_unrelated_notification_traffic() {
        // Arrange: register an unrelated (Queue) notification receiver on the
        // same connection, then flood it with traffic for the whole test.
        // notification_epoch tracking must be scoped to Lease so this
        // unrelated traffic never prevents the observer's bootstrap install
        // from succeeding; a connection-wide epoch would keep invalidating
        // the install check and livelock the observer.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (mut reader, writer) = stream.into_split();
            let writer = Arc::new(tokio::sync::Mutex::new(writer));

            assert_eq!(read_split(&mut reader).await.0, message_type::CONNECT);

            let (kind, payload) = read_split(&mut reader).await;
            assert_eq!(kind, message_type::LEASE_SUBSCRIBE);
            let mut d = PayloadDecoder::new(&payload);
            assert_eq!(d.get_string().unwrap(), "lease://acme/**");
            {
                let mut w = writer.lock().await;
                write_split(&mut w, message_type::LEASE_SUBSCRIBE, &token_response(1)).await;
            }

            // Flood unrelated Queue notifications for the rest of the
            // connection's life, faster than a LEASE_LIST round trip can
            // complete. Started only after authentication has settled so it
            // cannot be mistaken for an authentication response.
            let flood_writer = Arc::clone(&writer);
            let flooding = tokio::spawn(async move {
                loop {
                    let mut w = flood_writer.lock().await;
                    let frame =
                        crate::codec::try_encode_message_frame(message_type::QUEUE_NOTIFY, &[])
                            .unwrap();
                    if w.write_u32(u32::try_from(frame.len()).unwrap())
                        .await
                        .is_err()
                        || w.write_all(&frame).await.is_err()
                        || w.flush().await.is_err()
                    {
                        return;
                    }
                    drop(w);
                    tokio::time::sleep(Duration::from_micros(100)).await;
                }
            });

            let (kind, _) = read_split(&mut reader).await;
            assert_eq!(kind, message_type::LEASE_LIST);
            {
                let mut w = writer.lock().await;
                write_split(&mut w, message_type::LEASE_LIST, &list_response(&[], None)).await;
            }

            let (kind, payload) = read_split(&mut reader).await;
            assert_eq!(kind, message_type::LEASE_UNSUBSCRIBE);
            let mut d = PayloadDecoder::new(&payload);
            assert_eq!(d.get_string().unwrap(), "lease://acme/**");
            {
                let mut w = writer.lock().await;
                write_split(&mut w, message_type::LEASE_UNSUBSCRIBE, &ok_response()).await;
            }

            flooding.abort();
        });

        let (state, _) = watch::channel(ConnectionState::Disconnected);
        let connection = AsyncConnection::spawn(AsyncConnectionOptions {
            endpoint: format!("tcp://{address}"),
            token_provider: Arc::new(|| async { Ok(String::new()) }),
            timeout: Duration::from_secs(2),
            max_queued: 64,
            reconnect: ReconnectPolicy {
                enabled: false,
                ..ReconnectPolicy::default()
            },
            retry: RetryPolicy::default(),
            heartbeat: HeartbeatOptions::default(),
            observability: FitzObservability::default(),
            state,
        });
        connection.connect().await.unwrap();
        // Register interest in the unrelated notification type so the
        // supervisor actually tracks (and, pre-fix, bumps a shared epoch
        // for) it instead of silently dropping it.
        let _unrelated = connection.notifications(message_type::QUEUE_NOTIFY, 64);
        let client = LeaseClient::new(connection.clone());

        // Act
        let observer = client
            .observe("lease://acme/**", ObserveOptions::default())
            .await
            .unwrap();

        // Assert: the observer becomes ready promptly despite the flood.
        tokio::time::timeout(Duration::from_secs(3), wait_until(|| observer.is_ready()))
            .await
            .expect("observer livelocked under unrelated notification traffic");

        observer.close().await;
        server.await.unwrap();
        connection.close().await;
    }

    #[tokio::test]
    async fn should_relist_once_when_notifications_arrive_during_bootstrap() {
        // Arrange: SUBSCRIBE acks, then a notification is delivered before the
        // observer's first LIST response even comes back. Per the bootstrap
        // contract, that buffered notification must force exactly one more
        // full LIST pass before the observer becomes ready.
        let list_calls = Arc::new(AtomicUsize::new(0));
        let calls = Arc::clone(&list_calls);
        let (client, connection, server) = connected_lease_client(move |mut stream| async move {
            let (kind, payload) = read_frame(&mut stream).await;
            assert_eq!(kind, message_type::LEASE_SUBSCRIBE);
            let mut d = PayloadDecoder::new(&payload);
            assert_eq!(d.get_string().unwrap(), "lease://acme/**");
            write_frame(
                &mut stream,
                message_type::LEASE_SUBSCRIBE,
                &token_response(1),
            )
            .await;

            // Delivered before the observer has asked to LIST at all.
            write_frame(
                &mut stream,
                message_type::LEASE_NOTIFY,
                &notify_payload(1, "lease://acme/renderers/doc-1"),
            )
            .await;

            let (kind, _) = read_frame(&mut stream).await;
            assert_eq!(kind, message_type::LEASE_LIST);
            calls.fetch_add(1, Ordering::SeqCst);
            write_frame(
                &mut stream,
                message_type::LEASE_LIST,
                &list_response(&[], None),
            )
            .await;

            // The buffered notification must force a second LIST pass.
            let (kind, _) = read_frame(&mut stream).await;
            assert_eq!(kind, message_type::LEASE_LIST);
            calls.fetch_add(1, Ordering::SeqCst);
            write_frame(
                &mut stream,
                message_type::LEASE_LIST,
                &list_response(&[sample_item()], None),
            )
            .await;

            // Keep the connection open (rather than letting `stream` drop
            // and trigger a spurious disconnect) until the observer is
            // explicitly closed.
            let (kind, payload) = read_frame(&mut stream).await;
            assert_eq!(kind, message_type::LEASE_UNSUBSCRIBE);
            let mut d = PayloadDecoder::new(&payload);
            assert_eq!(d.get_string().unwrap(), "lease://acme/**");
            write_frame(&mut stream, message_type::LEASE_UNSUBSCRIBE, &ok_response()).await;
        })
        .await;

        // Act
        let observer = client
            .observe("lease://acme/**", ObserveOptions::default())
            .await
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(2),
            wait_until(|| observer.is_ready() && !observer.snapshot().is_empty()),
        )
        .await
        .expect("observer never installed the reconciled view");

        // Assert: exactly one extra LIST pass was triggered, and its result
        // (not the first, empty page) is what got installed.
        assert_eq!(list_calls.load(Ordering::SeqCst), 2);
        assert_eq!(observer.snapshot(), vec![sample_item()]);
        observer.close().await;
        server.await.unwrap();
        connection.close().await;
    }

    #[tokio::test]
    async fn should_relist_complete_items_on_steady_state_notification() {
        // Arrange: an empty bootstrap, then two steady-state notifications for
        // the same route: one that resolves held (insert) and one that
        // resolves not-held (remove) — each driven by LIST so the observer
        // never fabricates fields QUERY does not return. A oneshot signal keeps the NOTIFY off the wire until the
        // test has independently confirmed bootstrap already finished, so
        // there is no race with the bootstrap-time notification drain (which
        // would otherwise nondeterministically fold a same-burst NOTIFY into
        // the bootstrap drain).
        let (bootstrap_done_tx, bootstrap_done_rx) = oneshot::channel::<()>();
        let (insert_observed_tx, insert_observed_rx) = oneshot::channel::<()>();
        let (client, connection, server) = connected_lease_client(move |mut stream| async move {
            let (kind, payload) = read_frame(&mut stream).await;
            assert_eq!(kind, message_type::LEASE_SUBSCRIBE);
            let mut d = PayloadDecoder::new(&payload);
            assert_eq!(d.get_string().unwrap(), "lease://acme/**");
            write_frame(
                &mut stream,
                message_type::LEASE_SUBSCRIBE,
                &token_response(1),
            )
            .await;

            let (kind, _) = read_frame(&mut stream).await;
            assert_eq!(kind, message_type::LEASE_LIST);
            write_frame(
                &mut stream,
                message_type::LEASE_LIST,
                &list_response(&[], None),
            )
            .await;

            bootstrap_done_rx.await.unwrap();

            // Steady state: route becomes held.
            write_frame(
                &mut stream,
                message_type::LEASE_NOTIFY,
                &notify_payload(1, "lease://acme/renderers/doc-1"),
            )
            .await;
            let (kind, _) = read_frame(&mut stream).await;
            assert_eq!(kind, message_type::LEASE_LIST);
            write_frame(
                &mut stream,
                message_type::LEASE_LIST,
                &list_response(&[sample_item()], None),
            )
            .await;

            // Wait until the test has observed the insert before releasing,
            // so the transient held->not-held window is never missed.
            insert_observed_rx.await.unwrap();

            // Steady state: same route released.
            write_frame(
                &mut stream,
                message_type::LEASE_NOTIFY,
                &notify_payload(1, "lease://acme/renderers/doc-1"),
            )
            .await;
            let (kind, _) = read_frame(&mut stream).await;
            assert_eq!(kind, message_type::LEASE_LIST);
            write_frame(
                &mut stream,
                message_type::LEASE_LIST,
                &list_response(&[], None),
            )
            .await;
        })
        .await;

        // Act
        let observer = client
            .observe("lease://acme/**", ObserveOptions::default())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), wait_until(|| observer.is_ready()))
            .await
            .expect("observer never became ready");
        bootstrap_done_tx.send(()).unwrap();
        tokio::time::timeout(
            Duration::from_secs(2),
            wait_until(|| !observer.snapshot().is_empty()),
        )
        .await
        .expect("steady-state LIST insert never applied");

        // Assert: inserted with every LIST-only field preserved.
        let inserted = observer.snapshot();
        assert_eq!(inserted, vec![sample_item()]);
        insert_observed_tx.send(()).unwrap();

        // Assert: removed once LIST reports the current inventory empty.
        tokio::time::timeout(
            Duration::from_secs(2),
            wait_until(|| observer.snapshot().is_empty()),
        )
        .await
        .expect("steady-state LIST removal never applied");

        observer.close().await;
        server.await.unwrap();
        connection.close().await;
    }

    #[tokio::test]
    async fn should_retry_replacement_subscription_after_transient_failure() {
        // Arrange
        let (client, connection, server) = connected_lease_client(move |mut stream| async move {
            assert_eq!(
                read_frame(&mut stream).await.0,
                message_type::LEASE_SUBSCRIBE
            );
            write_frame(
                &mut stream,
                message_type::LEASE_SUBSCRIBE,
                &token_response(1),
            )
            .await;

            assert_eq!(
                read_frame(&mut stream).await.0,
                message_type::LEASE_UNSUBSCRIBE
            );
            write_frame(&mut stream, message_type::LEASE_UNSUBSCRIBE, &ok_response()).await;

            assert_eq!(
                read_frame(&mut stream).await.0,
                message_type::LEASE_SUBSCRIBE
            );
            write_frame(
                &mut stream,
                message_type::LEASE_SUBSCRIBE,
                &error_response(5010, "transient subscribe failure"),
            )
            .await;

            assert_eq!(
                read_frame(&mut stream).await.0,
                message_type::LEASE_SUBSCRIBE
            );
            write_frame(
                &mut stream,
                message_type::LEASE_SUBSCRIBE,
                &token_response(2),
            )
            .await;

            assert_eq!(
                read_frame(&mut stream).await.0,
                message_type::LEASE_UNSUBSCRIBE
            );
            write_frame(&mut stream, message_type::LEASE_UNSUBSCRIBE, &ok_response()).await;
        })
        .await;
        let mut subscription = client.subscribe("lease://acme/**").await.unwrap();
        let shutdown = CancellationToken::new();

        // Act
        let replaced = tokio::time::timeout(
            Duration::from_secs(2),
            replace_observer_subscription(&client, "lease://acme/**", &mut subscription, &shutdown),
        )
        .await
        .unwrap();

        // Assert
        assert!(replaced);
        subscription.unsubscribe().await.unwrap();
        server.await.unwrap();
        connection.close().await;
    }

    #[tokio::test]
    async fn should_fire_periodic_reconciliation_on_its_configured_interval() {
        // Arrange: no notifications at all — the second LIST call can only be
        // explained by the periodic reconciliation backstop firing.
        let list_calls = Arc::new(AtomicUsize::new(0));
        let calls = Arc::clone(&list_calls);
        let (client, connection, server) = connected_lease_client(move |mut stream| async move {
            let (kind, _) = read_frame(&mut stream).await;
            assert_eq!(kind, message_type::LEASE_SUBSCRIBE);
            write_frame(
                &mut stream,
                message_type::LEASE_SUBSCRIBE,
                &token_response(1),
            )
            .await;

            let (kind, _) = read_frame(&mut stream).await;
            assert_eq!(kind, message_type::LEASE_LIST);
            calls.fetch_add(1, Ordering::SeqCst);
            write_frame(
                &mut stream,
                message_type::LEASE_LIST,
                &list_response(&[], None),
            )
            .await;

            let (kind, _) = read_frame(&mut stream).await;
            assert_eq!(kind, message_type::LEASE_LIST);
            calls.fetch_add(1, Ordering::SeqCst);
            write_frame(
                &mut stream,
                message_type::LEASE_LIST,
                &list_response(&[sample_item()], None),
            )
            .await;

            // Keep the connection open (rather than letting `stream` drop
            // and trigger a spurious disconnect) until the observer is
            // explicitly closed.
            let (kind, payload) = read_frame(&mut stream).await;
            assert_eq!(kind, message_type::LEASE_UNSUBSCRIBE);
            let mut d = PayloadDecoder::new(&payload);
            assert_eq!(d.get_string().unwrap(), "lease://acme/**");
            write_frame(&mut stream, message_type::LEASE_UNSUBSCRIBE, &ok_response()).await;
        })
        .await;

        // Act: a short reconciliation interval keeps the test fast.
        let observer = client
            .observe(
                "lease://acme/**",
                ObserveOptions {
                    reconciliation_interval: Duration::from_millis(30),
                    ..ObserveOptions::default()
                },
            )
            .await
            .unwrap();

        // Assert: the periodic tick drives a second LIST without any notification.
        tokio::time::timeout(
            Duration::from_secs(2),
            wait_until(|| list_calls.load(Ordering::SeqCst) == 2),
        )
        .await
        .expect("periodic reconciliation never fired a second LIST");
        tokio::time::timeout(
            Duration::from_secs(2),
            wait_until(|| !observer.snapshot().is_empty()),
        )
        .await
        .expect("reconciled view never installed");
        assert_eq!(observer.snapshot(), vec![sample_item()]);

        observer.close().await;
        server.await.unwrap();
        connection.close().await;
    }

    #[tokio::test]
    async fn should_rebootstrap_from_scratch_when_connection_reconnects() {
        // Arrange: a two-phase server. The first connection completes a full
        // bootstrap, then the transport is dropped. The observer must notice
        // the reconnect, invalidate its view, and redo the *entire* bootstrap
        // (a fresh SUBSCRIBE replay followed by a fresh LIST) on the restored
        // connection — not just resume where it left off.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            assert_eq!(read_frame(&mut first).await.0, message_type::CONNECT);
            let (kind, payload) = read_frame(&mut first).await;
            assert_eq!(kind, message_type::LEASE_SUBSCRIBE);
            let mut d = PayloadDecoder::new(&payload);
            assert_eq!(d.get_string().unwrap(), "lease://acme/**");
            write_frame(
                &mut first,
                message_type::LEASE_SUBSCRIBE,
                &token_response(11),
            )
            .await;
            let (kind, _) = read_frame(&mut first).await;
            assert_eq!(kind, message_type::LEASE_LIST);
            write_frame(
                &mut first,
                message_type::LEASE_LIST,
                &list_response(&[], None),
            )
            .await;
            let _ = first.shutdown().await;

            let (mut second, _) = listener.accept().await.unwrap();
            assert_eq!(read_frame(&mut second).await.0, message_type::CONNECT);
            // Restorable registration replay: the same SUBSCRIBE is re-sent
            // automatically by the connection layer, not by the observer.
            let (kind, payload) = read_frame(&mut second).await;
            assert_eq!(kind, message_type::LEASE_SUBSCRIBE);
            let mut d = PayloadDecoder::new(&payload);
            assert_eq!(d.get_string().unwrap(), "lease://acme/**");
            write_frame(
                &mut second,
                message_type::LEASE_SUBSCRIBE,
                &token_response(29),
            )
            .await;
            // The observer itself must issue a fresh LIST after noticing the
            // reconnect.
            let (kind, _) = read_frame(&mut second).await;
            assert_eq!(kind, message_type::LEASE_LIST);
            write_frame(
                &mut second,
                message_type::LEASE_LIST,
                &list_response(&[sample_item()], None),
            )
            .await;

            // Keep this connection open (rather than letting `second` drop
            // and trigger a spurious third disconnect) until the observer is
            // explicitly closed.
            let (kind, payload) = read_frame(&mut second).await;
            assert_eq!(kind, message_type::LEASE_UNSUBSCRIBE);
            let mut d = PayloadDecoder::new(&payload);
            assert_eq!(d.get_string().unwrap(), "lease://acme/**");
            write_frame(&mut second, message_type::LEASE_UNSUBSCRIBE, &ok_response()).await;
        });

        let (state, _) = watch::channel(ConnectionState::Disconnected);
        let connection = AsyncConnection::spawn(AsyncConnectionOptions {
            endpoint: format!("tcp://{address}"),
            token_provider: Arc::new(|| async { Ok(String::new()) }),
            timeout: Duration::from_secs(2),
            max_queued: 8,
            reconnect: ReconnectPolicy {
                base_delay: Duration::from_millis(5),
                maximum_delay: Duration::from_millis(20),
                maximum_attempts: 20,
                ..ReconnectPolicy::default()
            },
            retry: RetryPolicy::default(),
            heartbeat: HeartbeatOptions::default(),
            observability: FitzObservability::default(),
            state,
        });
        connection.connect().await.unwrap();
        let client = LeaseClient::new(connection.clone());

        // Act
        let observer = client
            .observe("lease://acme/**", ObserveOptions::default())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), wait_until(|| observer.is_ready()))
            .await
            .expect("observer never completed the initial bootstrap");

        // Assert: after the transport loss and restore, the fresh LIST result
        // from the second connection is installed.
        tokio::time::timeout(
            Duration::from_secs(5),
            wait_until(|| !observer.snapshot().is_empty()),
        )
        .await
        .expect("observer never re-bootstrapped after reconnect");
        assert_eq!(observer.snapshot(), vec![sample_item()]);
        assert!(observer.is_ready());

        observer.close().await;
        server.await.unwrap();
        connection.close().await;
    }

    #[tokio::test]
    async fn should_unsubscribe_and_stop_background_work_when_closed() {
        // Arrange
        let (client, connection, server) = connected_lease_client(|mut stream| async move {
            let (kind, payload) = read_frame(&mut stream).await;
            assert_eq!(kind, message_type::LEASE_SUBSCRIBE);
            let mut d = PayloadDecoder::new(&payload);
            assert_eq!(d.get_string().unwrap(), "lease://acme/**");
            write_frame(
                &mut stream,
                message_type::LEASE_SUBSCRIBE,
                &token_response(1),
            )
            .await;

            let (kind, _) = read_frame(&mut stream).await;
            assert_eq!(kind, message_type::LEASE_LIST);
            write_frame(
                &mut stream,
                message_type::LEASE_LIST,
                &list_response(&[], None),
            )
            .await;

            let (kind, payload) = read_frame(&mut stream).await;
            assert_eq!(kind, message_type::LEASE_UNSUBSCRIBE);
            let mut d = PayloadDecoder::new(&payload);
            assert_eq!(d.get_string().unwrap(), "lease://acme/**");
            write_frame(&mut stream, message_type::LEASE_UNSUBSCRIBE, &ok_response()).await;
        })
        .await;

        // Act
        let observer = client
            .observe("lease://acme/**", ObserveOptions::default())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), wait_until(|| observer.is_ready()))
            .await
            .expect("observer never became ready");
        observer.close().await;

        // Assert: the server script only completes (without panicking on an
        // unexpected frame) if UNSUBSCRIBE was actually sent, and nothing
        // further arrives after close() returns.
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .expect("server script timed out")
            .unwrap();
        connection.close().await;
    }

    #[tokio::test]
    async fn should_complete_deferred_acquire_and_release_when_handle_dropped() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            assert_eq!(read_frame(&mut stream).await.0, message_type::CONNECT);
            let (kind, payload) = read_frame(&mut stream).await;
            assert_eq!(kind, message_type::LEASE_ACQUIRE);
            assert_eq!(
                u32::from_be_bytes(payload[payload.len() - 4..].try_into().unwrap()),
                7
            );
            let mut queued = PayloadEncoder::new();
            queued.put_u8(0).put_u8(2).put_u64(0);
            write_frame(&mut stream, message_type::LEASE_ACQUIRE, &queued.finish()).await;
            let mut acquired = PayloadEncoder::new();
            acquired.put_u8(0).put_u8(0).put_u64(42);
            write_frame(&mut stream, message_type::LEASE_ACQUIRE, &acquired.finish()).await;
            assert_eq!(read_frame(&mut stream).await.0, message_type::LEASE_RELEASE);
            write_frame(&mut stream, message_type::LEASE_RELEASE, &[0]).await;
        });
        let (state, _) = watch::channel(ConnectionState::Disconnected);
        let connection = AsyncConnection::spawn(AsyncConnectionOptions {
            endpoint: format!("tcp://{address}"),
            token_provider: Arc::new(|| async { Ok(String::new()) }),
            timeout: Duration::from_secs(1),
            max_queued: 8,
            reconnect: ReconnectPolicy {
                enabled: false,
                ..ReconnectPolicy::default()
            },
            retry: RetryPolicy::default(),
            heartbeat: HeartbeatOptions::default(),
            observability: FitzObservability::default(),
            state,
        });
        connection.connect().await.unwrap();
        let client = LeaseClient::new(connection.clone());
        let handle = client
            .acquire(
                "lease://realm/area/resource",
                "worker-1",
                30,
                LeaseAcquireOptions { wait_seconds: 7 },
            )
            .await
            .unwrap();
        drop(handle);
        server.await.unwrap();
        connection.close().await;
    }
}
