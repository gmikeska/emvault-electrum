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
