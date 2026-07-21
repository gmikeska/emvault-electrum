//! Live reconnect/resubscribe test against the testnet4 node's electrs.
//!
//! Gated on `ELECTRUM_LIVE=1`; runs **on the node** (shells out to `bitcoin-cli`
//! and `systemctl restart electrs`, so it needs root there):
//! `ELECTRUM_LIVE=1 ELECTRUM_URL=tcp://127.0.0.1:60001 ./live_reconnect --nocapture`.
//!
//! Proves Q4 auto-resubscribe: watch an address via `WatchPool`, **restart
//! electrs** (dropping the connection), send the address coins, and assert a
//! `ScriptUpdate` still arrives — either from a post-reconnect notification or the
//! resubscribe status-replay.

use bitcoin::{Address, Network};
use emvault_electrum::WatchPool;
use std::process::Command;
use std::str::FromStr;
use std::time::Duration;

fn sh(program: &str, args: &[&str]) -> String {
    let out = Command::new(program).args(args).output().expect("spawn");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn bcli(args: &[&str]) -> String {
    let base = [
        "-datadir=/var/lib/bitcoind",
        "-conf=/etc/bitcoin/bitcoin.conf",
        "-rpcwallet=default",
    ];
    let all: Vec<&str> = base.iter().copied().chain(args.iter().copied()).collect();
    sh("bitcoin-cli", &all)
}

fn url() -> String {
    std::env::var("ELECTRUM_URL").unwrap_or_else(|_| "tcp://127.0.0.1:60001".to_string())
}

#[tokio::test]
async fn reconnect_resubscribes_and_still_delivers() {
    if std::env::var("ELECTRUM_LIVE").is_err() {
        eprintln!("SKIP: set ELECTRUM_LIVE=1 to run the live reconnect test");
        return;
    }

    let addr_str = bcli(&["getnewaddress", "reconnect-test", "bech32"]);
    assert!(
        addr_str.starts_with("tb1"),
        "getnewaddress failed: {addr_str}"
    );
    let script = Address::from_str(&addr_str)
        .unwrap()
        .require_network(Network::Testnet4)
        .unwrap()
        .script_pubkey();

    let pool = WatchPool::connect(&url(), 2).expect("connect pool");
    let mut updates = pool.updates();

    // Watch (ref-counted); fresh address → no status.
    let initial = pool.watch(&script).await.expect("watch");
    assert!(initial.is_none(), "fresh address has no status");

    // Kill the connection out from under the worker.
    eprintln!("restarting electrs...");
    let _ = sh("systemctl", &["restart", "electrs"]);

    // Trigger activity (bitcoind is unaffected by the electrs restart; the tx sits
    // in the mempool until electrs comes back and indexes it).
    let txid = bcli(&["sendtoaddress", &addr_str, "0.0001"]);
    eprintln!("sent txid = {txid}");
    assert!(txid.len() == 64, "sendtoaddress failed: {txid}");

    // The worker must reconnect + resubscribe, then surface the change.
    let got = tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            match updates.recv().await {
                Ok(u) if u.script == script && u.status.is_some() => break u,
                _ => {}
            }
        }
    })
    .await
    .expect("timed out waiting for post-reconnect ScriptUpdate");
    eprintln!(
        "post-reconnect notification fired: status_is_some = {}",
        got.status.is_some()
    );

    // Ref-counted unwatch tears the subscription down.
    pool.unwatch(&script).await.expect("unwatch");
}
