//! Live integration test against the `GroupVault` testnet4 electrs node.
//!
//! Gated on `ELECTRUM_LIVE=1` (needs reachability to electrs, e.g. an SSH tunnel:
//! `ssh -f -N -L 60011:127.0.0.1:60001 root@<node>` then
//! `ELECTRUM_LIVE=1 ELECTRUM_URL=tcp://127.0.0.1:60011 cargo test --test live_electrum -- --nocapture`).
//!
//! The wallet is the node's `default` wallet (watch-only tpub descriptors, testnet4;
//! funded via faucet to a known 1,037,501 sats confirmed).

use bdk_wallet::Wallet;
use bdk_wallet::bitcoin::Network;
use bitcoin::{Address, ScriptBuf};
use emvault_electrum::ElectrumBackend;
use std::str::FromStr;

const EXT: &str = "wpkh([8c687226/84h/1h/0h]tpubDDA4qW7KBpEGQMonifLDCfewTzaxRynkd874Mm8xBYJ3W4fbFJbWV2fKvAjmBAu1N13mzXcQ7HHN1REJprMT7T2g85SzYnTRWFUXZji9t2o/0/*)#jvvd6yfn";
const INT: &str = "wpkh([8c687226/84h/1h/0h]tpubDDA4qW7KBpEGQMonifLDCfewTzaxRynkd874Mm8xBYJ3W4fbFJbWV2fKvAjmBAu1N13mzXcQ7HHN1REJprMT7T2g85SzYnTRWFUXZji9t2o/1/*)#rcfv83et";

const EXPECTED_SATS: u64 = 1_037_501;
const ADDR_A: &str = "tb1qruevj2jdwgjcj67cld92mz6zz3kn5zze09rvk9"; // received 741_935
const ADDR_B: &str = "tb1q0g64xlwed2hq63xa662pjh45vdy82402esxjsg"; // received 295_566

fn skip() -> bool {
    if std::env::var("ELECTRUM_LIVE").is_err() {
        eprintln!("SKIP: set ELECTRUM_LIVE=1 (+ ELECTRUM_URL) to run live electrs tests");
        return true;
    }
    false
}

fn url() -> String {
    std::env::var("ELECTRUM_URL").unwrap_or_else(|_| "tcp://127.0.0.1:60011".to_string())
}

#[tokio::test]
async fn rescan_discovers_funds_then_revealed_sync_matches() {
    if skip() {
        return;
    }
    let mut wallet = Wallet::create(EXT.to_string(), INT.to_string())
        .network(Network::Testnet4)
        .create_wallet_no_persist()
        .expect("build watch-only wallet");

    let backend = ElectrumBackend::connect(&url(), Network::Testnet4).expect("connect electrs");

    // rescan (full_scan / gap-limit) discovers the used addresses with no pre-reveal.
    let r = backend.rescan(&mut wallet).await.expect("rescan");
    let after_rescan = wallet.balance().total().to_sat();
    eprintln!(
        "rescan: tip={} changeset={} balance={} sats",
        r.tip_height,
        r.changeset.is_some(),
        after_rescan
    );
    assert_eq!(after_rescan, EXPECTED_SATS, "rescan balance");

    // revealed-only sync on the now-revealed spks must preserve the balance.
    backend.sync(&mut wallet).await.expect("sync");
    assert_eq!(
        wallet.balance().total().to_sat(),
        EXPECTED_SATS,
        "sync balance"
    );
}

#[tokio::test]
async fn scan_scripts_reports_per_address_balance() {
    if skip() {
        return;
    }
    let backend = ElectrumBackend::connect(&url(), Network::Testnet4).expect("connect electrs");
    let spks: Vec<ScriptBuf> = [ADDR_A, ADDR_B]
        .iter()
        .map(|a| {
            Address::from_str(a)
                .unwrap()
                .require_network(Network::Testnet4)
                .unwrap()
                .script_pubkey()
        })
        .collect();

    let states = backend.scan_scripts(&spks).await.expect("scan_scripts");
    let total: u64 = states.iter().map(|s| s.confirmed_sats).sum();
    for s in &states {
        eprintln!(
            "{} -> confirmed {} sats, status={:?}",
            s.script,
            s.confirmed_sats,
            s.status.is_some()
        );
        assert!(s.status.is_some(), "funded address should have history");
    }
    assert_eq!(total, EXPECTED_SATS, "scan_scripts total balance");
}
