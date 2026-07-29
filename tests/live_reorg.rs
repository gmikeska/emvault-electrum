//! Live regtest reorg test: does `emvault-electrum` survive a
//! **reorg-below-the-persisted-tip**?
//!
//! ## Empirical finding (the reason this fix exists)
//! `wallet.apply_update` does **not** raise [`ElectrumError::CannotConnect`] — the
//! local chain reorgs cleanly to a common ancestor. But a plain sync leaves a
//! **phantom UTXO**: the reorged-out funding tx is demoted to unconfirmed and never
//! dropped, so the wallet reports funds that no longer exist on-chain. No in-place
//! operation (`sync`, same-wallet `full_scan`, `apply_evicted_txs`) clears it —
//! only a from-scratch rebuild does.
//!
//! ## What this now asserts (the regression test for the fix)
//! [`ElectrumBackend::sync`] detects the reorg-below-tip (a checkpoint hash flips
//! at/below the persisted tip), rebuilds the tx graph via `full_scan`, swaps it in,
//! and reports the reorged-out tx via `ElectrumSyncResult::evicted_txids` with
//! `reorg_rebuilt = true`. This test proves the phantom is cleared (balance → 0),
//! `evicted_txids == [D]`, and a follow-up sync stays clean.
//!
//! ## Harness (see `groupvault/deploy/regtest/`)
//! - regtest bitcoind — JSON-RPC `host.docker.internal:18543` (user/pass
//!   `regtest`/`regtest`), wallet `miner`.
//! - Fulcrum Electrum server — TCP `host.docker.internal:60011`.
//!
//! ## Run
//! ```text
//! ELECTRUM_LIVE=1 cargo test --test live_reorg -- --nocapture
//! ```
//! Optional overrides: `ELECTRUM_URL`, `BITCOIND_RPC`, `BITCOIND_RPC_AUTH`,
//! `MINER_WALLET`.
//!
//! The recipe is the plan doc §6.2 "evict via funding double-spend": fund a
//! watch-only wallet address, confirm + bury it, sync (persist a checkpoint
//! above the funding block), then `invalidateblock` the funding block, evict the
//! funding tx by double-spending its input, and mine a strictly longer competing
//! branch. Re-sync and observe.

// Live integration harness: a few pedantic lints are noise for a linear,
// scripted regtest scenario (product names in prose, sats<->BTC display math on
// fixed regtest values, `curl` RPC helpers, one long linear test body).
#![allow(
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::needless_pass_by_value,
    clippy::collapsible_if,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use std::process::Command;
use std::time::{Duration, Instant};

use bdk_wallet::bitcoin::{Network, Txid};
use bdk_wallet::{KeychainKind, Wallet};
use emvault_electrum::ElectrumBackend;
use emvault_electrum::electrum_client::{Client, ElectrumApi};
use serde_json::{Value, json};

// Neutral watch-only wpkh descriptors (no personal data). tpub key material is
// testnet-encoded, which BDK accepts for `Network::Regtest`; addresses render as
// `bcrt1…`. We only ever derive/receive to these on the isolated regtest chain.
const EXT: &str = "wpkh([8c687226/84h/1h/0h]tpubDDA4qW7KBpEGQMonifLDCfewTzaxRynkd874Mm8xBYJ3W4fbFJbWV2fKvAjmBAu1N13mzXcQ7HHN1REJprMT7T2g85SzYnTRWFUXZji9t2o/0/*)#jvvd6yfn";
const INT: &str = "wpkh([8c687226/84h/1h/0h]tpubDDA4qW7KBpEGQMonifLDCfewTzaxRynkd874Mm8xBYJ3W4fbFJbWV2fKvAjmBAu1N13mzXcQ7HHN1REJprMT7T2g85SzYnTRWFUXZji9t2o/1/*)#rcfv83et";

const FUND_SATS: u64 = 500_000_000; // 5 BTC
const EVICT_FEE_SATS: u64 = 1_000_000; // 0.01 BTC — dwarfs the funding tx fee, so D' replaces D via RBF

fn skip() -> bool {
    if std::env::var("ELECTRUM_LIVE").is_err() {
        eprintln!("SKIP: set ELECTRUM_LIVE=1 to run the live regtest reorg test");
        return true;
    }
    false
}

fn electrum_url() -> String {
    std::env::var("ELECTRUM_URL").unwrap_or_else(|_| "tcp://host.docker.internal:60011".to_string())
}
fn rpc_base() -> String {
    std::env::var("BITCOIND_RPC")
        .unwrap_or_else(|_| "http://host.docker.internal:18543".to_string())
}
fn rpc_auth() -> String {
    std::env::var("BITCOIND_RPC_AUTH").unwrap_or_else(|_| "regtest:regtest".to_string())
}
fn miner_path() -> String {
    format!(
        "/wallet/{}",
        std::env::var("MINER_WALLET").unwrap_or_else(|_| "miner".to_string())
    )
}

/// Minimal bitcoind JSON-RPC over `curl` (mirrors `drive.sh`). `wallet` is an
/// optional `/wallet/<name>` path suffix. Panics on transport or RPC error.
fn rpc(method: &str, params: Value, wallet: Option<&str>) -> Value {
    let url = format!("{}{}", rpc_base(), wallet.unwrap_or(""));
    let body = json!({"jsonrpc": "1.0", "id": "reorg-test", "method": method, "params": params});
    let out = Command::new("curl")
        .args([
            "-s",
            "--max-time",
            "60",
            "--user",
            &rpc_auth(),
            "--data-binary",
            &body.to_string(),
            "-H",
            "content-type: text/plain;",
            &url,
        ])
        .output()
        .expect("spawn curl");
    let v: Value = serde_json::from_slice(&out.stdout).unwrap_or_else(|e| {
        panic!(
            "rpc {method}: bad JSON: {e}: {}",
            String::from_utf8_lossy(&out.stdout)
        )
    });
    assert!(
        v.get("error").is_none_or(Value::is_null),
        "rpc {method} error: {}",
        v["error"]
    );
    v["result"].clone()
}

/// Mine `n` blocks to the miner wallet; returns the new tip height.
fn mine(n: u32) -> u64 {
    let addr = rpc("getnewaddress", json!([]), Some(&miner_path()));
    let addr = addr.as_str().expect("getnewaddress");
    rpc("generatetoaddress", json!([n, addr]), None);
    block_count()
}

fn block_count() -> u64 {
    rpc("getblockcount", json!([]), None).as_u64().unwrap()
}
fn block_hash(height: u64) -> String {
    rpc("getblockhash", json!([height]), None)
        .as_str()
        .unwrap()
        .to_string()
}

/// Poll Fulcrum's tip until it reaches `target` (the README lag gotcha: Fulcrum
/// re-indexes on its next poll, so we must wait between a chain change and a sync).
fn wait_fulcrum(target: u64) {
    let url = electrum_url();
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        if let Ok(c) = Client::new(&url) {
            if let Ok(h) = c.block_headers_subscribe() {
                let h = h.height as u64;
                if h >= target {
                    eprintln!("fulcrum caught up: tip={h} (>= {target})");
                    return;
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "fulcrum did not reach height {target} within 90s"
        );
        std::thread::sleep(Duration::from_millis(750));
    }
}

fn btc(sats: u64) -> f64 {
    sats as f64 / 100_000_000.0
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reorg_below_persisted_tip() {
    if skip() {
        return;
    }
    let url = electrum_url();
    let backend = ElectrumBackend::connect(&url, Network::Regtest).expect("connect fulcrum");
    let mut wallet = Wallet::create(EXT.to_string(), INT.to_string())
        .network(Network::Regtest)
        .create_wallet_no_persist()
        .expect("watch-only regtest wallet");

    // --- Phase 1: fund a wallet address, confirm it, bury it below the tip. ---
    let addr = wallet.reveal_next_address(KeychainKind::External).address;
    eprintln!("funding address: {addr}");
    let d_txid = rpc(
        "sendtoaddress",
        json!([addr.to_string(), btc(FUND_SATS)]),
        Some(&miner_path()),
    );
    let d_txid = d_txid.as_str().expect("sendtoaddress txid").to_string();
    // Capture D's funding input U (while D is still in the mempool).
    let d = rpc("getrawtransaction", json!([d_txid, true]), None);
    let u_txid = d["vin"][0]["txid"].as_str().unwrap().to_string();
    let u_vout = d["vin"][0]["vout"].as_u64().unwrap();
    let u = rpc("getrawtransaction", json!([u_txid, true]), None);
    let u_value_btc = u["vout"][u_vout as usize]["value"].as_f64().unwrap();
    eprintln!("D={d_txid}  U={u_txid}:{u_vout} ({u_value_btc} BTC)");

    let h0 = mine(1); // confirm D in B0
    let b0 = block_hash(h0);
    eprintln!("D confirmed in B0 height={h0} hash={b0}");
    let h_pre = mine(2); // bury B0 two deep
    eprintln!("pre-reorg tip height={h_pre}");

    wait_fulcrum(h_pre);
    let r1 = backend.sync(&mut wallet).await.expect("sync #1");
    let bal1 = wallet.balance().total().to_sat();
    eprintln!(
        "sync #1: tip={} balance={} sats (persisted checkpoint at {})",
        r1.tip_height, bal1, r1.tip_height
    );
    assert_eq!(bal1, FUND_SATS, "wallet should see the funding UTXO");
    assert_eq!(
        u64::from(r1.tip_height),
        h_pre,
        "persisted tip == pre-reorg tip"
    );

    // --- Phase 2: reorg below the persisted tip, evicting the funding tx. ---
    rpc("invalidateblock", json!([b0]), None); // rolls back B0..tip; D -> mempool
    eprintln!("invalidated B0; tip now {}", block_count());
    // Evict D by double-spending its input U (higher fee -> RBF replace).
    let dest = rpc("getnewaddress", json!([]), Some(&miner_path()));
    let dest = dest.as_str().unwrap();
    let out_sats = (u_value_btc * 100_000_000.0).round() as u64 - EVICT_FEE_SATS;
    // Dynamic output key (the destination address) must be inserted explicitly —
    // `json!({ dest: .. })` would use the literal string "dest".
    let mut outputs = serde_json::Map::new();
    outputs.insert(dest.to_string(), json!(btc(out_sats)));
    let raw = rpc(
        "createrawtransaction",
        json!([[{"txid": u_txid, "vout": u_vout}], Value::Object(outputs)]),
        None,
    );
    let signed = rpc(
        "signrawtransactionwithwallet",
        json!([raw.as_str().unwrap()]),
        Some(&miner_path()),
    );
    assert!(
        signed["complete"].as_bool().unwrap_or(false),
        "miner should sign the double-spend of U: {signed}"
    );
    let d_prime = rpc(
        "sendrawtransaction",
        json!([signed["hex"].as_str().unwrap()]),
        None,
    );
    eprintln!("broadcast D' (evicts D): {}", d_prime.as_str().unwrap());
    // Mine a strictly longer branch so the wallet must follow the reorg.
    let h_post = mine((h_pre - block_count()) as u32 + 3);
    eprintln!("post-reorg tip height={h_post} (was {h_pre})");
    assert!(h_post > h_pre, "reorg branch must be strictly longer");

    wait_fulcrum(h_post);

    // --- Phase 3: THE OBSERVATION — re-sync; the backend must rebuild. ---
    // sync() detects the reorg-below-tip (a checkpoint hash flipped at/below the
    // persisted tip), rebuilds the wallet's tx graph from scratch, swaps it in, and
    // reports the reorged-out funding tx. It does NOT raise CannotConnect, and the
    // rebuild — unlike any in-place sync/full_scan/evict — clears the phantom UTXO.
    let d: Txid = d_txid.parse().expect("D txid");
    let r2 = backend
        .sync(&mut wallet)
        .await
        .expect("sync() handles a reorg-below-tip without error");
    let bal_after_sync = wallet.balance().total().to_sat();
    eprintln!(
        "post-reorg sync: reorg_rebuilt={} evicted={:?} tip={} balance={bal_after_sync} sats",
        r2.reorg_rebuilt, r2.evicted_txids, r2.tip_height
    );

    // (1) The reorg was detected and a rebuild was performed.
    assert!(
        r2.reorg_rebuilt,
        "sync() must flag a reorg-below-tip rebuild"
    );
    // (2) The phantom UTXO is gone — balance reflects the true post-reorg chain.
    assert_eq!(
        bal_after_sync, 0,
        "rebuild must clear the reorged-out phantom UTXO"
    );
    // (3) The evicted funding tx D is reported — and only it.
    assert_eq!(
        r2.evicted_txids,
        vec![d],
        "the reorged-out funding tx D must be reported as evicted"
    );
    // (4) The wallet still follows the (longer) reorg branch's tip.
    assert_eq!(
        u64::from(r2.tip_height),
        h_post,
        "wallet followed the reorg tip"
    );
    // (5) The rebuild staged a complete replacement changeset for the app to persist
    //     (replace-not-merge; a merge would re-introduce the phantom).
    assert!(
        r2.changeset.is_some(),
        "a rebuild must stage a complete replacement changeset"
    );

    // The local chain really did reorg (not a silent fork): the wallet's hash at
    // the funding height now matches the node's new block, not the orphaned B0.
    let node_hash_h0 = block_hash(h0);
    let wallet_hash_h0 = wallet
        .local_chain()
        .get(u32::try_from(h0).unwrap())
        .map(|cp| cp.hash().to_string());
    eprintln!("DIAG chain@{h0}: node={node_hash_h0} wallet={wallet_hash_h0:?} (orphaned B0={b0})");
    assert_eq!(
        wallet_hash_h0.as_deref(),
        Some(node_hash_h0.as_str()),
        "wallet local-chain reorged to the node's branch"
    );
    assert_ne!(node_hash_h0, b0, "funding block was genuinely replaced");

    // Corroboration that the rebuilt state matches on-wire truth:
    // (a) Fulcrum's direct view of the funding script is empty.
    let states = backend
        .scan_scripts(std::slice::from_ref(&addr.script_pubkey()))
        .await
        .expect("scan_scripts");
    eprintln!(
        "DIAG fulcrum scan_scripts(funding): confirmed={} unconfirmed={}",
        states[0].confirmed_sats, states[0].unconfirmed_sats
    );
    assert_eq!(
        states[0].confirmed_sats, 0,
        "Fulcrum: funding is gone on the wire"
    );
    assert!(
        states[0].status.is_none(),
        "Fulcrum: funding script has no history"
    );
    // (b) The funding tx D is absent from the rebuilt wallet graph entirely (D5:
    //     "evicted" == absent, not merely demoted), and no spendable output remains.
    assert!(
        wallet.get_tx(d).is_none(),
        "rebuilt graph must not contain the reorged-out funding tx"
    );
    assert!(
        wallet.list_unspent().next().is_none(),
        "rebuilt wallet has no spendable outputs"
    );

    // (c) An idempotency check: a follow-up sync on the rebuilt wallet is a normal
    //     forward sync — no phantom returns, no spurious second rebuild.
    let r3 = backend.sync(&mut wallet).await.expect("post-rebuild sync");
    eprintln!(
        "post-rebuild follow-up sync: reorg_rebuilt={} evicted={:?} balance={} sats",
        r3.reorg_rebuilt,
        r3.evicted_txids,
        wallet.balance().total().to_sat()
    );
    assert!(
        !r3.reorg_rebuilt,
        "a follow-up sync must not re-trigger a rebuild"
    );
    assert!(
        r3.evicted_txids.is_empty(),
        "a follow-up sync reports no evictions"
    );
    assert_eq!(
        wallet.balance().total().to_sat(),
        0,
        "balance stays correct after a follow-up sync"
    );

    eprintln!(
        "CONCLUSION: reorg-below-tip is handled — sync() rebuilt the graph, \
         cleared the phantom (0 sats), and reported evicted={:?}.",
        r2.evicted_txids
    );
}
