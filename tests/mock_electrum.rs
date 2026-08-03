//! In-process mock Electrum server tests for the 0.7.0 cheap-read helpers
//! [`ElectrumBackend::tip_height`] and [`ElectrumBackend::get_tx`].
//!
//! Unlike `live_electrum.rs` (gated on `ELECTRUM_LIVE`), these run in default CI:
//! a `std::net::TcpListener` on `127.0.0.1:0` speaks the newline-delimited
//! JSON-RPC subset `electrum-client` drives for these two calls, so no live
//! electrs/Fulcrum is needed. `ElectrumBackend::connect` only opens a TCP socket
//! (no `server.version` handshake), then:
//!   - `tip_height()`  -> `blockchain.headers.subscribe` (needs `{height, hex}`
//!     with a valid 80-byte header the client deserializes), and
//!   - `get_tx(txid)`  -> `blockchain.transaction.get` (needs raw tx hex).

use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::thread;
use std::time::Duration;

use bitcoin::block::{Header, Version as BlockVersion};
use bitcoin::consensus::encode::serialize_hex;
use bitcoin::hashes::Hash;
use bitcoin::transaction::Version as TxVersion;
use bitcoin::{
    Amount, BlockHash, CompactTarget, Network, OutPoint, ScriptBuf, Sequence, Transaction, TxIn,
    TxMerkleNode, TxOut, Witness, absolute::LockTime,
};
use emvault_electrum::ElectrumBackend;

/// Fixed values the mock serves; both tests share the same tip height.
const HEIGHT: usize = 2500;

/// What a single mock instance answers with.
struct MockConfig {
    height: usize,
    header_hex: String,
    tx_hex: String,
}

/// Bind an in-process mock Electrum server on a random loopback port and return
/// its `tcp://127.0.0.1:<port>` url. The listener backlog is live the moment
/// `bind` returns, so `connect` succeeds regardless of `accept` timing. Every
/// read is bounded by a timeout so an idle/dropped client can never hang the
/// thread, and the process exits the thread with the test binary.
fn spawn_mock(cfg: MockConfig) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock listener");
    let addr = listener.local_addr().expect("mock local_addr");
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
            let Ok(mut writer) = stream.try_clone() else {
                continue;
            };
            let reader = BufReader::new(stream);
            for line in reader.lines() {
                let Ok(line) = line else { break }; // EOF or read timeout: drop this conn
                if line.trim().is_empty() {
                    continue;
                }
                let Ok(req) = serde_json::from_str::<serde_json::Value>(&line) else {
                    break;
                };
                // Echo the request id verbatim so the client's reader thread pairs
                // the response with the correct in-flight call.
                let id = req.get("id").cloned().unwrap_or(serde_json::Value::Null);
                let method = req
                    .get("method")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                let result = match method {
                    "blockchain.headers.subscribe" => {
                        serde_json::json!({ "height": cfg.height, "hex": cfg.header_hex })
                    }
                    "blockchain.transaction.get" => serde_json::Value::String(cfg.tx_hex.clone()),
                    // Not sent by these two calls, but answered defensively.
                    "server.version" => serde_json::json!(["ElectrumMock 1.0", "1.4"]),
                    _ => serde_json::Value::Null,
                };
                let resp = serde_json::json!({ "jsonrpc": "2.0", "id": id, "result": result });
                let Ok(mut bytes) = serde_json::to_vec(&resp) else {
                    break;
                };
                bytes.push(b'\n');
                if writer.write_all(&bytes).is_err() || writer.flush().is_err() {
                    break;
                }
            }
        }
    });
    format!("tcp://{addr}")
}

/// A syntactically valid 80-byte regtest-style header (deserialize only checks
/// structure, not proof-of-work, so arbitrary fields are fine).
fn sample_header() -> Header {
    Header {
        version: BlockVersion::from_consensus(0x2000_0000),
        prev_blockhash: BlockHash::all_zeros(),
        merkle_root: TxMerkleNode::all_zeros(),
        time: 1_700_000_000,
        bits: CompactTarget::from_consensus(0x207f_ffff),
        nonce: 2,
    }
}

/// A minimal, well-formed transaction; its `compute_txid()` is the assertion key.
fn sample_tx() -> Transaction {
    Transaction {
        version: TxVersion::TWO,
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint::null(),
            script_sig: ScriptBuf::new(),
            sequence: Sequence::MAX,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: Amount::from_sat(50_000),
            script_pubkey: ScriptBuf::new(),
        }],
    }
}

#[tokio::test]
async fn tip_height_reads_subscribed_header() {
    let cfg = MockConfig {
        height: HEIGHT,
        header_hex: serialize_hex(&sample_header()),
        tx_hex: serialize_hex(&sample_tx()),
    };
    let url = spawn_mock(cfg);

    let backend = ElectrumBackend::connect(&url, Network::Regtest).expect("connect mock");
    let height = backend.tip_height().await.expect("tip_height");

    assert_eq!(height, u32::try_from(HEIGHT).unwrap());
}

#[tokio::test]
async fn get_tx_fetches_and_decodes() {
    let tx = sample_tx();
    let expected = tx.compute_txid();
    let cfg = MockConfig {
        height: HEIGHT,
        header_hex: serialize_hex(&sample_header()),
        tx_hex: serialize_hex(&tx),
    };
    let url = spawn_mock(cfg);

    let backend = ElectrumBackend::connect(&url, Network::Regtest).expect("connect mock");
    let got = backend.get_tx(expected).await.expect("get_tx");

    assert_eq!(got.compute_txid(), expected, "decoded tx must round-trip");
}
