//! Result/DTO types: [`ElectrumSyncResult`] (staged BDK sync outcome, mirrors
//! `emvault-esplora`'s `EsploraSyncResult`) and [`AddressState`] (the lightweight
//! `scan_scripts` probe row).

use bdk_wallet::ChangeSet;
use bitcoin::{ScriptBuf, Txid};

use crate::electrum_client::ScriptStatus;

/// The staged changeset and counters produced by a [`crate::ElectrumBackend::sync`]
/// / `rescan`. Fields mirror `emvault-esplora`'s `EsploraSyncResult` one-for-one so
/// `emvault-core` can `From`-convert into its `chain_sync::SyncResult`.
#[derive(Debug)]
pub struct ElectrumSyncResult {
    /// Staged wallet changeset to persist, or `None` if nothing changed.
    pub changeset: Option<ChangeSet>,
    /// Blocks connected by this sync. `0` for the electrum path (query-per-SPK,
    /// not per-block); present to keep the shape identical to the node backend.
    pub blocks_synced: u32,
    /// Count of transactions newly seen in the mempool during this sync.
    pub new_mempool_txs: u32,
    /// Height of the wallet's local chain tip after applying the update.
    pub tip_height: u32,
    /// Txids that were **confirmed** in the wallet's graph before this sync but
    /// are **absent entirely** from the chain after it (reorged/evicted out — not
    /// merely demoted to mempool, so a re-queued sweep is *not* listed here). Empty
    /// on a normal forward sync; only populated on the reorg-rebuild path
    /// ([`crate::ElectrumBackend::sync`]). The consuming app uses these to drive
    /// migration reconcile.
    pub evicted_txids: Vec<Txid>,
    /// `true` when this sync detected a reorg below the persisted tip and rebuilt
    /// the wallet's tx graph from scratch (the phantom-UTXO fix). A plain forward
    /// sync leaves it `false`. When `true`, `changeset` is the **complete** rebuilt
    /// changeset and the app must **replace** its persisted aggregate, not merge it
    /// (a merge would re-introduce the reorged-out phantom UTXO).
    pub reorg_rebuilt: bool,
}

/// A lightweight per-address probe row returned by
/// [`crate::ElectrumBackend::scan_scripts`]. **No UTXOs, no BDK mutation** — a
/// "did anything change + what's the balance" view (Fork #3, decided A). On a
/// detected change the app runs a targeted `sync`, the sole writer of
/// UTXOs/spendable state into BDK.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddressState {
    /// The script pubkey probed (1:1 with an electrum scripthash).
    pub script: ScriptBuf,
    /// The electrum status hash — the change *signal* (not the data). `None` when
    /// the address has no history yet.
    pub status: Option<ScriptStatus>,
    /// Confirmed balance, satoshis.
    pub confirmed_sats: u64,
    /// Unconfirmed (mempool) balance, satoshis. May be negative on-wire for spends;
    /// clamped to `0` here since this probe only tracks receive-side detection.
    pub unconfirmed_sats: u64,
}
