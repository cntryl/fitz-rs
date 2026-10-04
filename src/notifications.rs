//! Transport receipt metadata for consumers with monotonic request budgets.

use parking_lot::Mutex;
use std::collections::HashMap;
use tokio::sync::broadcast;
use tokio::time::Instant;

#[derive(Clone)]
pub(crate) struct ReceivedNotification {
    pub payload: Vec<u8>,
    pub received_at: Instant,
    pub generation: u64,
}

#[derive(Default)]
pub(crate) struct Notifications {
    payloads: Mutex<HashMap<u16, broadcast::Sender<Vec<u8>>>>,
    timed: Mutex<HashMap<u16, broadcast::Sender<ReceivedNotification>>>,
}

impl Notifications {
    pub(crate) fn subscribe(&self, kind: u16, capacity: usize) -> broadcast::Receiver<Vec<u8>> {
        self.payloads
            .lock()
            .entry(kind)
            .or_insert_with(|| broadcast::channel(capacity.max(1)).0)
            .subscribe()
    }

    pub(crate) fn subscribe_timed(
        &self,
        kind: u16,
        capacity: usize,
    ) -> broadcast::Receiver<ReceivedNotification> {
        self.timed
            .lock()
            .entry(kind)
            .or_insert_with(|| broadcast::channel(capacity.max(1)).0)
            .subscribe()
    }

    pub(crate) fn publish(&self, kind: u16, payload: Vec<u8>, generation: u64) {
        let received_at = Instant::now();
        if let Some(sender) = self.timed.lock().get(&kind) {
            let _ = sender.send(ReceivedNotification {
                payload: payload.clone(),
                received_at,
                generation,
            });
        }
        if let Some(sender) = self.payloads.lock().get(&kind) {
            let _ = sender.send(payload);
        }
    }
}
