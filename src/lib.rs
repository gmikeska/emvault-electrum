//! Production, **descriptor-private** electrs/Electrum chain backend for
//! [EmVault](https://github.com/gmikeska) (BDK) wallets.
//!
//! Two surfaces over a self-hosted **romanz/electrs**, so the output descriptor
//! never leaves the app server (the disqualifier for the nodeless
//! esplora/Waterfalls backends):
//!
//! 1. [`ElectrumBackend`] — a BDK `sync` / `rescan` / `broadcast` backend
//!    (wraps [`bdk_electrum::BdkElectrumClient`]), plus [`ElectrumBackend::scan_scripts`],
//!    a lightweight per-address balance/status probe. Mirrors `emvault-esplora`'s
//!    `EsploraBackend`.
//! 2. The **watch layer** — [`WatchConn`] / [`WatchPool`]: a thin async wrapper
//!    over `electrum-client`'s `script_subscribe` / `script_unsubscribe` /
//!    notifications, with connection affinity + ref-counting. The app drives
//!    watch/unwatch as its live cache-invalidation signal.
//!
//! Design: `docs/design/emvault-electrum-design.md`.
//!
//! The `electrum-client` watch surface is reached through
//! [`bdk_electrum::electrum_client`] (re-exported here as [`electrum_client`]) so
//! the sync half and the watch half share one connection/type universe.

pub use bdk_electrum::electrum_client;

/// Socket timeout (seconds) applied to every electrs connection. Bounds a read on
/// a dropped connection so failures surface instead of hanging on the default
/// (unbounded) `TcpStream` behaviour.
const SOCKET_TIMEOUT_SECS: u8 = 10;

/// Internal retry count for the **sync backend** client. `electrum-client`'s retry
/// path transparently rebuilds the socket on failure — fine here, because the sync
/// backend holds **no subscriptions** to lose.
const BACKEND_RETRY: u8 = 3;

/// Internal retry count for **watch** clients: `0` — we *want* a dropped connection
/// to surface as an error so the worker's own reconnect + resubscribe (Q4) runs.
/// (The client's internal reconnect would silently rebuild the socket **without**
/// re-subscribing, so notifications would be lost.)
const WATCH_RETRY: u8 = 0;

fn build_client(url: &str, retry: u8) -> Result<electrum_client::Client, electrum_client::Error> {
    let config = electrum_client::ConfigBuilder::new()
        .timeout(Some(SOCKET_TIMEOUT_SECS))
        .retry(retry)
        .build();
    electrum_client::Client::from_config(url, config)
}

/// Build a sync-backend client (timeout + internal retry).
///
/// # Errors
/// Returns the underlying `electrum_client::Error` if the connection can't be made.
pub(crate) fn connect_backend_client(
    url: &str,
) -> Result<electrum_client::Client, electrum_client::Error> {
    build_client(url, BACKEND_RETRY)
}

/// Build a watch client (timeout, **no** internal retry — see [`WATCH_RETRY`]).
///
/// # Errors
/// Returns the underlying `electrum_client::Error` if the connection can't be made.
pub(crate) fn connect_watch_client(
    url: &str,
) -> Result<electrum_client::Client, electrum_client::Error> {
    build_client(url, WATCH_RETRY)
}

mod backend;
mod error;
mod pool;
mod result;
mod watch;

pub use backend::{ElectrumBackend, ElectrumSyncOpts};
pub use error::ElectrumError;
pub use pool::WatchPool;
pub use result::{AddressState, ElectrumSyncResult};
pub use watch::{ScriptUpdate, WatchConn};
