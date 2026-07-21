//! [`WatchPool`] — N [`WatchConn`]s plus a `script → conn` affinity map and
//! per-script ref-counts (the mechanism half of the session-watcher; the app owns
//! the `session → address` map + timing, Q1).
//!
//! - **Affinity:** a given script is always watched/unwatched on the *same*
//!   connection (subscriptions are per-connection, §4). Assigned round-robin on
//!   first watch.
//! - **Ref-counting:** the real `subscribe`/`unsubscribe` fires only at the 0↔1
//!   edge, so one address shared by two sessions isn't double-torn-down.
//! - **Reconnect:** each [`WatchConn`] auto-resubscribes its active set on
//!   reconnect (Q4); all connections publish onto one shared notification stream.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use bitcoin::ScriptBuf;
use tokio::sync::broadcast;

use crate::electrum_client::ScriptStatus;
use crate::error::ElectrumError;
use crate::watch::{ScriptUpdate, WatchConn};

/// Capacity of the pool's merged notification buffer.
const UPDATE_CHANNEL_CAP: usize = 4096;

/// Pure affinity + ref-count bookkeeping — no I/O, unit-tested.
struct Registry {
    /// script → (assigned connection index, refcount)
    map: HashMap<ScriptBuf, (usize, u32)>,
    conns: usize,
    next: usize,
}

impl Registry {
    fn new(conns: usize) -> Self {
        Self {
            map: HashMap::new(),
            conns: conns.max(1),
            next: 0,
        }
    }

    /// Bump the refcount for `spk`, assigning a connection on first use. Returns
    /// `(conn_idx, first)` — `first == true` means the caller must actually
    /// `subscribe` on that connection.
    fn acquire(&mut self, spk: &ScriptBuf) -> (usize, bool) {
        if let Some((idx, cnt)) = self.map.get_mut(spk) {
            *cnt += 1;
            (*idx, false)
        } else {
            let idx = self.next % self.conns;
            self.next = self.next.wrapping_add(1);
            self.map.insert(spk.clone(), (idx, 1));
            (idx, true)
        }
    }

    /// Drop a refcount for `spk`. Returns `(conn_idx, last)` — `last == true` means
    /// the count hit zero and the caller must `unsubscribe`. `None` if `spk` wasn't
    /// tracked.
    fn release(&mut self, spk: &ScriptBuf) -> Option<(usize, bool)> {
        let (idx, cnt) = self.map.get_mut(spk)?;
        let idx = *idx;
        *cnt = cnt.saturating_sub(1);
        if *cnt == 0 {
            self.map.remove(spk);
            Some((idx, true))
        } else {
            Some((idx, false))
        }
    }
}

/// A pool of watch connections with affinity + ref-counted subscriptions and a
/// single merged notification stream.
pub struct WatchPool {
    conns: Vec<WatchConn>,
    registry: Mutex<Registry>,
    update_tx: broadcast::Sender<ScriptUpdate>,
}

impl WatchPool {
    /// Open `conns` watch connections to electrs (minimum 1). All connections
    /// publish notifications onto one shared stream ([`updates`](Self::updates)).
    ///
    /// # Errors
    /// [`ElectrumError::Electrum`] if any connection fails to open.
    pub fn connect(url: &str, conns: usize) -> Result<Self, ElectrumError> {
        let n = conns.max(1);
        let (update_tx, _) = broadcast::channel(UPDATE_CHANNEL_CAP);
        let mut v = Vec::with_capacity(n);
        for _ in 0..n {
            v.push(WatchConn::connect_with(url, update_tx.clone())?);
        }
        Ok(Self {
            conns: v,
            registry: Mutex::new(Registry::new(n)),
            update_tx,
        })
    }

    fn registry(&self) -> std::sync::MutexGuard<'_, Registry> {
        self.registry.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Ref-counted watch. The **first** watcher subscribes on the affinity-assigned
    /// connection and returns the current status (the initial check); subsequent
    /// watchers just bump the count and return `Ok(None)` (the status went to the
    /// first watcher; changes arrive via [`updates`](Self::updates)).
    ///
    /// # Errors
    /// [`ElectrumError`] on subscribe failure (the refcount is rolled back).
    pub async fn watch(&self, spk: &ScriptBuf) -> Result<Option<ScriptStatus>, ElectrumError> {
        let (idx, first) = self.registry().acquire(spk);
        if !first {
            return Ok(None);
        }
        match self.conns[idx].watch(spk.as_script()).await {
            Ok(status) => Ok(status),
            Err(e) => {
                // Roll back so a failed subscribe doesn't leak a phantom refcount.
                self.registry().release(spk);
                Err(e)
            }
        }
    }

    /// Ref-counted unwatch: the real `unsubscribe` fires only when the last holder
    /// releases. A no-op (`Ok`) if `spk` isn't watched.
    ///
    /// # Errors
    /// [`ElectrumError`] on unsubscribe failure.
    pub async fn unwatch(&self, spk: &ScriptBuf) -> Result<(), ElectrumError> {
        let released = self.registry().release(spk);
        if let Some((idx, true)) = released {
            self.conns[idx].unwatch(spk.as_script()).await?;
        }
        Ok(())
    }

    /// A fresh receiver for the merged status-change stream across all connections.
    #[must_use]
    pub fn updates(&self) -> broadcast::Receiver<ScriptUpdate> {
        self.update_tx.subscribe()
    }

    /// Number of underlying watch connections.
    #[must_use]
    pub fn connections(&self) -> usize {
        self.conns.len()
    }
}

#[cfg(test)]
mod tests {
    use super::Registry;
    use bitcoin::ScriptBuf;

    fn spk(n: u8) -> ScriptBuf {
        ScriptBuf::from_bytes(vec![n])
    }

    #[test]
    fn affinity_is_round_robin_and_stable() {
        let mut r = Registry::new(3);
        assert_eq!(r.acquire(&spk(1)), (0, true));
        assert_eq!(r.acquire(&spk(2)), (1, true));
        assert_eq!(r.acquire(&spk(3)), (2, true));
        assert_eq!(r.acquire(&spk(4)), (0, true), "wraps back to conn 0");
        // re-acquiring an existing script stays on its assigned connection.
        assert_eq!(r.acquire(&spk(2)), (1, false));
    }

    #[test]
    fn refcount_subscribes_once_unsubscribes_at_zero() {
        let mut r = Registry::new(2);
        assert_eq!(r.acquire(&spk(9)), (0, true), "first → subscribe");
        assert_eq!(r.acquire(&spk(9)), (0, false), "second → no subscribe");
        assert_eq!(r.release(&spk(9)), Some((0, false)), "one ref left");
        assert_eq!(r.release(&spk(9)), Some((0, true)), "zero → unsubscribe");
        assert_eq!(r.release(&spk(9)), None, "gone");
    }
}
