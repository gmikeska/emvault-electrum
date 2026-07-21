//! Live watch-layer smoke test against the testnet4 node's electrs.
//!
//! Gated on `ELECTRUM_LIVE=1`. Runs **on the node** (shells out to `bitcoin-cli`),
//! so ship the compiled binary there:
//! `ELECTRUM_LIVE=1 ELECTRUM_URL=tcp://127.0.0.1:60001 ./live_watch --nocapture`.
//!
//! Exercises the deferred smoke test: subscribe to a fresh address → send it coins
//! → assert a `ScriptUpdate` fires → `unwatch` returns `true`, second returns `false`.

use bitcoin::{Address, Network};
use emvault_electrum::WatchConn;
use std::process::Command;
use std::str::FromStr;
use std::time::Duration;

fn bcli(args: &[&str]) -> String {
    let out = Command::new("bitcoin-cli")
        .args([
            "-datadir=/var/lib/bitcoind",
            "-conf=/etc/bitcoin/bitcoin.conf",
            "-rpcwallet=default",
        ])
        .args(args)
        .output()
        .expect("run bitcoin-cli");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn url() -> String {
    std::env::var("ELECTRUM_URL").unwrap_or_else(|_| "tcp://127.0.0.1:60001".to_string())
}

#[tokio::test]
async fn subscribe_notify_unsubscribe_roundtrip() {
    if std::env::var("ELECTRUM_LIVE").is_err() {
        eprintln!("SKIP: set ELECTRUM_LIVE=1 to run the live watch test");
        return;
    }

    // Fresh, unused address to watch.
    let addr_str = bcli(&["getnewaddress", "watch-test", "bech32"]);
    assert!(
        addr_str.starts_with("tb1"),
        "getnewaddress failed: {addr_str}"
    );
    let script = Address::from_str(&addr_str)
        .unwrap()
        .require_network(Network::Testnet4)
        .unwrap()
        .script_pubkey();

    let conn = WatchConn::connect(&url()).expect("connect electrs");
    let mut updates = conn.updates();

    // Subscribe — fresh address has no history, so the initial status is None.
    let initial = conn.watch(script.as_script()).await.expect("watch");
    assert!(initial.is_none(), "fresh address should have no status");

    // Trigger activity: self-send a small amount (0-conf mempool tx).
    let txid = bcli(&["sendtoaddress", &addr_str, "0.0001"]);
    eprintln!("sent txid = {txid}");
    assert!(txid.len() == 64, "sendtoaddress failed: {txid}");

    // Await the status-change notification (bounded by poll interval + mempool).
    let got = tokio::time::timeout(Duration::from_secs(40), async {
        loop {
            match updates.recv().await {
                Ok(u) if u.script == script => break u,
                _ => {}
            }
        }
    })
    .await
    .expect("timed out waiting for ScriptUpdate");
    eprintln!(
        "notification fired: status_is_some = {}",
        got.status.is_some()
    );
    assert!(
        got.status.is_some(),
        "receive should set a non-empty status"
    );

    // Unwatch is idempotent: first true, second false.
    assert!(
        conn.unwatch(script.as_script()).await.expect("unwatch #1"),
        "first unwatch → true"
    );
    assert!(
        !conn.unwatch(script.as_script()).await.expect("unwatch #2"),
        "second unwatch → false"
    );
}
