//! [`ElectrumError`] — the crate's error type. Mirrors `emvault-esplora`'s
//! `EsploraSyncError` ergonomics (helper predicates so callers don't match inner
//! types).

use crate::electrum_client;

/// Errors raised while syncing, scanning, broadcasting, or watching through the
/// Electrum backend.
#[derive(Debug, thiserror::Error)]
pub enum ElectrumError {
    /// An underlying `electrum-client` request failed (transport, protocol, or a
    /// decode error). Covers both the sync half (via `bdk_electrum`) and the
    /// watch half.
    #[error("electrum request failed: {0}")]
    Electrum(#[from] electrum_client::Error),

    /// The assembled update couldn't be connected to the wallet's local chain
    /// (usually a reorg below the last persisted tip).
    #[error("failed to connect electrum update to the wallet's local chain")]
    CannotConnect(#[from] bdk_wallet::chain::local_chain::CannotConnectError),

    /// Electrum returned a value that didn't parse into the expected Bitcoin type.
    #[error("electrum returned a malformed {what}: {value}")]
    Malformed {
        /// What we were trying to parse (e.g. `"txid"`).
        what: &'static str,
        /// The offending raw value.
        value: String,
    },

    /// The reorg-recovery rebuild couldn't reconstruct a fresh wallet from the
    /// live wallet's own public descriptors. Should be unreachable (the
    /// descriptors already built a valid wallet); surfaced rather than panicked.
    #[error("failed to rebuild wallet from descriptors during reorg recovery: {0}")]
    Rebuild(String),

    /// The watch worker thread for a connection is gone (channel closed) — the
    /// connection is dead and could not be re-established.
    #[error("watch connection worker is disconnected")]
    Disconnected,

    /// A blocking backend worker (`spawn_blocking`) panicked — should not happen;
    /// surfaced as an error rather than aborting the process.
    #[error("blocking backend worker panicked")]
    WorkerPanicked,
}

impl ElectrumError {
    /// `true` if this error is the watch worker being gone.
    #[must_use]
    pub fn is_disconnected(&self) -> bool {
        matches!(self, Self::Disconnected)
    }
}
