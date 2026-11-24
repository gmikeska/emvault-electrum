# emvault-electrum

Production, **descriptor-private** electrs/Electrum chain backend for
[EmVault](https://github.com/gmikeska/emvault-core) (BDK) wallets — sync,
broadcast, and *watch* a `bdk_wallet::Wallet` against a self-hosted
[electrs](https://github.com/romanz/electrs) / [Fulcrum](https://github.com/cculianu/Fulcrum)
Electrum server.

See the [CHANGELOG](https://github.com/gmikeska/emvault-electrum/blob/master/CHANGELOG.md) for release notes.

Unlike the nodeless [`emvault-esplora`](https://crates.io/crates/emvault-esplora)
backend, the output **descriptor never leaves the app server**: sync is driven by
per-script queries, not a descriptor upload. That makes this the backend for
privacy-sensitive production, at the cost of running your own Electrum server.

It is normally enabled behind `emvault-core`'s `electrum` feature (which
re-exports it as `emvault_core::electrum`), but it depends only on
`bdk_wallet` / `bitcoin` / [`bdk_electrum`](https://crates.io/crates/bdk_electrum),
so it can be used directly too.

```toml
[dependencies]
emvault-electrum = "0.11"
# or, via the facade:
# emvault-core = { version = "0.10", features = ["electrum"] }
# emvault      = { version = "0.10", features = ["electrum"] }
```

## Two surfaces over one Electrum server

Both halves reach `electrum-client` through
[`bdk_electrum::electrum_client`] (re-exported as `emvault_electrum::electrum_client`),
so the sync half and the watch half share one connection/type universe.

### 1. `ElectrumBackend` — BDK sync / rescan / broadcast

A BDK chain backend wrapping [`bdk_electrum::BdkElectrumClient`], mirroring
`emvault-esplora`'s `EsploraBackend`. Its client is built with a bounded socket
timeout and transparent internal retry (it holds no subscriptions to lose).

```rust
use emvault_electrum::ElectrumBackend;
use bdk_wallet::bitcoin::Network;

// inside an async fn, given a `&mut bdk_wallet::Wallet` and a signed `Transaction`:
let backend = ElectrumBackend::connect("tcp://127.0.0.1:60001", Network::Testnet)?;

let result = backend.sync(&mut wallet).await?;   // gap-limited revealed-range scan
let txid   = backend.broadcast(&signed).await?;  // push a signed tx
# Ok::<(), emvault_electrum::ElectrumError>(())
```

| Method | Purpose |
|---|---|
| `ElectrumBackend::connect(url, network)` | connect (`tcp://` or `ssl://`) |
| `.with_opts(ElectrumSyncOpts)` | tune the gap limit / batch size |
| `.network()` / `.client()` | inspect network, borrow the shared client |
| `.sync(&mut wallet)` | incremental revealed-range sync |
| `.rescan(&mut wallet)` | force a full rescan |
| `.broadcast(&tx)` | push a signed tx, returns `Txid` |
| `.tip_height()` | read the current chain tip (`blockchain.headers.subscribe`), no RPC node |
| `.get_tx(txid)` | fetch a full transaction (`blockchain.transaction.get`) — nodeless prev-tx lookups |
| `.scan_scripts(scripts)` | lightweight per-address balance/status probe → `AddressState` |

`sync` / `rescan` return an [`ElectrumSyncResult`] (staged `ChangeSet` +
`blocks_synced` / `new_mempool_txs` / `tip_height` counters); `emvault-core`
provides `From<ElectrumSyncResult> for chain_sync::SyncResult` so this drops into
the same seam as the Bitcoin Core and Esplora backends.

### 2. Watch layer — `WatchPool` / `WatchConn`

A thin async wrapper over `electrum-client`'s `script_subscribe` /
`script_unsubscribe` / notification stream, with connection affinity and
ref-counting. The app drives `watch` / `unwatch` as its live
cache-invalidation signal — e.g. to push a WebSocket balance update the instant a
watched address changes status, with no polling.

```rust
use emvault_electrum::WatchPool;

let pool = WatchPool::connect("tcp://127.0.0.1:60001", 4)?; // 4 pooled connections
let mut updates = pool.updates();                            // broadcast receiver

pool.watch(&script).await?;                                  // subscribe
while let Ok(u) = updates.recv().await {
    if u.script == script { /* re-sync + push */ }
}
# Ok::<(), emvault_electrum::ElectrumError>(())
```

| Method | Purpose |
|---|---|
| `WatchPool::connect(url, conns)` | open a ref-counted pool of `conns` connections |
| `.watch(spk)` / `.unwatch(spk)` | subscribe / unsubscribe a scriptPubKey |
| `.updates()` | `broadcast::Receiver<ScriptUpdate>` of status changes |
| `.connections()` | pool size |

Watch clients use **zero** internal retry on purpose: a dropped connection must
surface as an error so the worker's own reconnect **and re-subscribe** runs —
`electrum-client`'s silent reconnect would rebuild the socket without
re-subscribing, losing notifications.

## Error handling

Fallible calls return `Result<_, ElectrumError>`:

- `Electrum(electrum_client::Error)` — transport / protocol error.
- `CannotConnect(..)` — the update couldn't connect to the wallet's local chain
  (usually a reorg).
- `Malformed { what, value }` — a server value that didn't parse into a `bitcoin`
  type.
- `Disconnected` — the watch worker's connection is gone.
- `WorkerPanicked` — the watch worker thread died.

## Design notes

- **Descriptor-private.** Sync issues per-scripthash queries; the descriptor is
  never sent to the server. This is the property the nodeless backends can't
  offer.
- **One connection universe.** The sync backend and the watch layer both speak
  `bdk_electrum::electrum_client`, so their types and connections unify.
- **`Send`-safe.** Sync paths hold no borrowed value across an `.await`, so the
  returned futures are `Send` and safe to call from inside `axum` handlers.

Full design: [`docs/design/emvault-electrum-design.md`](docs/design/emvault-electrum-design.md).

## Testing

```bash
cargo test   # unit + control-flow tests
```

## License

MIT OR Apache-2.0.

[`ElectrumSyncResult`]: https://docs.rs/emvault-electrum/latest/emvault_electrum/struct.ElectrumSyncResult.html
[`bdk_electrum::BdkElectrumClient`]: https://docs.rs/bdk_electrum/latest/bdk_electrum/struct.BdkElectrumClient.html
[`bdk_electrum::electrum_client`]: https://docs.rs/bdk_electrum/latest/bdk_electrum/
