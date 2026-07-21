//! [`WatchPool`] — N [`WatchConn`]s plus a `scripthash → conn` affinity map and
//! per-script ref-counts (the mechanism half of the session-watcher; the app owns
//! the `session → address` map + timing, Q1).
//!
//! - **Affinity:** a given script is always watched/unwatched on the *same*
//!   connection (subscriptions are per-connection, §4).
//! - **Ref-counting:** the real `subscribe`/`unsubscribe` fires only at the 0↔1
//!   edge, so one address shared by two sessions isn't double-torn-down.
//! - **Reconnect:** each `WatchConn` auto-resubscribes its active set + replays
//!   status diffs (Q4); the pool merges every connection's notifications.

use bitcoin::ScriptBuf;
use tokio::sync::mpsc;

use crate::electrum_client::ScriptStatus;
use crate::error::ElectrumError;
use crate::watch::{ScriptUpdate, WatchConn};

/// A pool of watch connections with affinity + ref-counted subscriptions.
pub struct WatchPool {
    #[allow(dead_code)]
    conns: Vec<WatchConn>,
}

impl WatchPool {
    /// Open `conns` watch connections to electrs.
    ///
    /// # Errors
    /// [`ElectrumError::Electrum`] if any connection fails to open.
    pub fn connect(_url: &str, _conns: usize) -> Result<Self, ElectrumError> {
        todo!("Phase 3: open N WatchConns + build registry + merged notification task")
    }

    /// Ref-counted watch: first watcher subscribes on the affinity-assigned
    /// connection and returns the current status; later watchers just bump the count.
    ///
    /// # Errors
    /// [`ElectrumError`] on subscribe failure.
    pub async fn watch(&self, _spk: &ScriptBuf) -> Result<Option<ScriptStatus>, ElectrumError> {
        todo!("Phase 3: affinity lookup/assign, refcount++, subscribe at 0→1")
    }

    /// Ref-counted unwatch: real `unsubscribe` only when the count hits 0.
    ///
    /// # Errors
    /// [`ElectrumError`] on unsubscribe failure.
    pub async fn unwatch(&self, _spk: &ScriptBuf) -> Result<(), ElectrumError> {
        todo!("Phase 3: refcount--, unsubscribe at 1→0")
    }

    /// The merged notification stream across all connections.
    #[must_use]
    pub fn updates(&self) -> mpsc::Receiver<ScriptUpdate> {
        todo!("Phase 3: hand out a receiver of the merged ScriptUpdate stream")
    }
}
