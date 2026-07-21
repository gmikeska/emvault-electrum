//! [`ElectrumBackend`] — the BDK sync/broadcast surface. Wraps
//! [`bdk_electrum::BdkElectrumClient`] over a blocking `electrum-client` `Client`,
//! and exposes an async facade (blocking calls run on `spawn_blocking`, Q3).
//! Mirrors `emvault-esplora`'s `EsploraBackend`.

use std::sync::Arc;

use bitcoin::hashes::{Hash, HashEngine, sha256};
use bitcoin::{Network, ScriptBuf, Transaction, Txid};

use bdk_electrum::BdkElectrumClient;
use bdk_electrum::electrum_client::{
    Client as ElectrumClient, ElectrumApi, GetHistoryRes, ScriptStatus,
};

use crate::error::ElectrumError;
use crate::result::{AddressState, ElectrumSyncResult};

/// Sync tuning knobs. `Default` = stop-gap 20, batch 20.
#[derive(Debug, Clone, Copy)]
pub struct ElectrumSyncOpts {
    /// `full_scan` stop-gap: stop a keychain after this many consecutive unused
    /// addresses (only used by `rescan` / gap-limit discovery).
    pub stop_gap: usize,
    /// Electrum request batch size.
    pub batch_size: usize,
    /// Fetch previous txouts (needed for fee / `SentAndReceived` accuracy).
    pub fetch_prev_txouts: bool,
}

impl Default for ElectrumSyncOpts {
    fn default() -> Self {
        Self {
            stop_gap: 20,
            batch_size: 20,
            fetch_prev_txouts: true,
        }
    }
}

/// A descriptor-private electrum chain backend: a [`BdkElectrumClient`] plus the
/// target network and sync options.
#[derive(Clone)]
pub struct ElectrumBackend {
    client: Arc<BdkElectrumClient<ElectrumClient>>,
    network: Network,
    opts: ElectrumSyncOpts,
}

impl ElectrumBackend {
    /// Connect to electrs. `url` scheme selects transport: `ssl://host:port`
    /// (production, rustls) or `tcp://host:port` (dev/tunnel).
    ///
    /// # Errors
    /// [`ElectrumError::Electrum`] if the connection can't be established.
    pub fn connect(url: &str, network: Network) -> Result<Self, ElectrumError> {
        let client = ElectrumClient::new(url)?;
        Ok(Self {
            client: Arc::new(BdkElectrumClient::new(client)),
            network,
            opts: ElectrumSyncOpts::default(),
        })
    }

    /// Override the sync options.
    #[must_use]
    pub fn with_opts(mut self, opts: ElectrumSyncOpts) -> Self {
        self.opts = opts;
        self
    }

    /// The network derived keys are stamped with.
    #[must_use]
    pub fn network(&self) -> Network {
        self.network
    }

    /// The underlying `bdk_electrum` client (for cache pre-population / advanced use).
    #[must_use]
    pub fn client(&self) -> &Arc<BdkElectrumClient<ElectrumClient>> {
        &self.client
    }

    /// Revealed-only steady-state sync of `wallet` (Fork #1). Queries electrs for
    /// exactly the wallet's revealed SPKs → stages a BDK `ChangeSet`.
    ///
    /// # Errors
    /// Surfaces electrum + chain-connection failures via [`ElectrumError`].
    pub async fn sync(
        &self,
        wallet: &mut bdk_wallet::Wallet,
    ) -> Result<ElectrumSyncResult, ElectrumError> {
        // Build the request off the wallet (immutable borrow ends here), then run
        // the blocking network call on a blocking thread so the future stays Send.
        let request = wallet.start_sync_with_revealed_spks().build();
        let client = Arc::clone(&self.client);
        let (batch, fetch) = (self.opts.batch_size, self.opts.fetch_prev_txouts);
        let update = tokio::task::spawn_blocking(move || client.sync(request, batch, fetch))
            .await
            .map_err(|_| ElectrumError::WorkerPanicked)??;
        wallet.apply_update(update)?;
        Ok(finish(wallet))
    }

    /// Full gap-limit rescan (recovery/audit path, Fork #1). Rebuilds wallet state
    /// from the descriptor alone.
    ///
    /// # Errors
    /// Surfaces electrum + chain-connection failures via [`ElectrumError`].
    pub async fn rescan(
        &self,
        wallet: &mut bdk_wallet::Wallet,
    ) -> Result<ElectrumSyncResult, ElectrumError> {
        let request = wallet.start_full_scan().build();
        let client = Arc::clone(&self.client);
        let (stop_gap, batch, fetch) = (
            self.opts.stop_gap,
            self.opts.batch_size,
            self.opts.fetch_prev_txouts,
        );
        let update =
            tokio::task::spawn_blocking(move || client.full_scan(request, stop_gap, batch, fetch))
                .await
                .map_err(|_| ElectrumError::WorkerPanicked)??;
        wallet.apply_update(update)?;
        Ok(finish(wallet))
    }

    /// Broadcast a fully-signed transaction; returns its txid.
    ///
    /// # Errors
    /// [`ElectrumError::Electrum`] if the broadcast is rejected.
    pub async fn broadcast(&self, tx: &Transaction) -> Result<Txid, ElectrumError> {
        let client = Arc::clone(&self.client);
        let tx = tx.clone();
        let txid = tokio::task::spawn_blocking(move || client.transaction_broadcast(&tx))
            .await
            .map_err(|_| ElectrumError::WorkerPanicked)??;
        Ok(txid)
    }

    /// Lightweight per-address probe (Fork #3, decided A): `get_balance` +
    /// history-derived status for each script, **no UTXOs, no BDK mutation**. Used
    /// by the app's idle-refresh poller and the session-open diff probe.
    ///
    /// # Errors
    /// [`ElectrumError::Electrum`] on request failure.
    pub async fn scan_scripts(
        &self,
        spks: &[ScriptBuf],
    ) -> Result<Vec<AddressState>, ElectrumError> {
        let client = Arc::clone(&self.client);
        let spks = spks.to_vec();
        tokio::task::spawn_blocking(move || {
            let api = &client.inner;
            let mut out = Vec::with_capacity(spks.len());
            for spk in spks {
                let bal = api.script_get_balance(spk.as_script())?;
                let hist = api.script_get_history(spk.as_script())?;
                out.push(AddressState {
                    script: spk,
                    status: electrum_status(&hist),
                    confirmed_sats: bal.confirmed,
                    unconfirmed_sats: u64::try_from(bal.unconfirmed.max(0)).unwrap_or(0),
                });
            }
            Ok(out)
        })
        .await
        .map_err(|_| ElectrumError::WorkerPanicked)?
    }
}

/// Build the staged result from the wallet after an `apply_update`.
fn finish(wallet: &mut bdk_wallet::Wallet) -> ElectrumSyncResult {
    let tip_height = wallet.latest_checkpoint().height();
    ElectrumSyncResult {
        changeset: wallet.take_staged(),
        blocks_synced: 0,
        new_mempool_txs: 0,
        tip_height,
    }
}

/// Compute an Electrum status hash from a scripthash's history, matching the
/// server-side `blockchain.scripthash.subscribe` status: sha256 over the
/// concatenation of `"{tx_hash}:{height}:"` for each history entry in returned
/// order. `None` for an empty (never-used) history.
fn electrum_status(history: &[GetHistoryRes]) -> Option<ScriptStatus> {
    if history.is_empty() {
        return None;
    }
    let mut eng = sha256::Hash::engine();
    for h in history {
        eng.input(format!("{}:{}:", h.tx_hash, h.height).as_bytes());
    }
    Some(sha256::Hash::from_engine(eng).to_byte_array().into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bdk_electrum::electrum_client::GetHistoryRes;
    use bitcoin::Txid;
    use std::str::FromStr;

    fn hist(txid: &str, height: i32) -> GetHistoryRes {
        GetHistoryRes {
            height,
            tx_hash: Txid::from_str(txid).unwrap(),
            fee: None,
        }
    }

    #[test]
    fn opts_default_is_gap20_batch20() {
        let o = ElectrumSyncOpts::default();
        assert_eq!(o.stop_gap, 20);
        assert_eq!(o.batch_size, 20);
        assert!(o.fetch_prev_txouts);
    }

    #[test]
    fn empty_history_has_no_status() {
        assert!(electrum_status(&[]).is_none());
    }

    #[test]
    fn status_is_deterministic_and_order_sensitive() {
        let a = hist(
            "d1f8d3f1f9c0e6b6f0b2a9a2a3a4a5a6a7a8a9b0b1b2b3b4b5b6b7b8b9c0c1c2",
            100,
        );
        let b = hist(
            "e2f8d3f1f9c0e6b6f0b2a9a2a3a4a5a6a7a8a9b0b1b2b3b4b5b6b7b8b9c0c1c2",
            0,
        );
        let s1 = electrum_status(&[a.clone(), b.clone()]).unwrap();
        let s1_again = electrum_status(&[a.clone(), b.clone()]).unwrap();
        let s2 = electrum_status(&[b, a]).unwrap();
        assert_eq!(s1, s1_again, "same input → same status");
        assert_ne!(s1, s2, "order changes the status hash");
    }
}
