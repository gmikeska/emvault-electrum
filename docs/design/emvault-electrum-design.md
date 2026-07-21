# `emvault-electrum` — library design (first draft, 2026-07-21)

Status: **Decisions locked (2026-07-21).** Written by Maggie from the
application-level spec `groupvault/docs/plans/electrs-session-scoped-backend.md`
(the "why" + watcher lifecycle) and by mirroring the sibling crate
`emvault-esplora`. Greg resolved all eight design questions on 2026-07-21; they are
folded into the sections below and summarised in §11. Next open topic: the **scan
procedure** (to be worked out with Greg before implementation).

---

## 1. Purpose & scope

`emvault-electrum` is the **production, descriptor-private** chain backend for
EmVault/GroupVault wallets. It talks to a **self-hosted romanz/electrs** over the
Electrum protocol so that:

- the **app server keeps the output descriptor** (it never leaves the app — the
  disqualifier for Waterfalls/esplora), and
- the app watches/scans **individual addresses (scripthashes)** it derives
  itself.

The crate provides **two distinct surfaces** over the same electrs:

1. **BDK chain backend** — `sync` / `rescan` / `broadcast`, mirroring
   `emvault-esplora`'s `EsploraBackend`. Used for the *initial check* and the
   *idle-refresh* scans, and for broadcasting signed txs.
2. **Watch layer** — a thin, near-passthrough async wrapper over
   `electrum-client`'s `script_subscribe` / `script_unsubscribe` / notifications,
   used as the app's live cache-invalidation signal (the core of the watcher
   lifecycle in the app spec §"Watcher lifecycle").

These are **separate surfaces** on purpose: BDK's sync model is request/response
and has **no** subscription concept, so watch/unwatch cannot be expressed through
a `sync()`-style API. Resolves app-spec open-Q#5 in favour of **Option A**
(this crate owns the electrs connection(s) and exposes both surfaces).

### Explicitly out of scope (lives in the GroupVault server)

The **session-watcher manager** — the mapping `session → user → revealed
addresses` and the *policy* of when to watch/unwatch (session open/close, address
reveal, payment-seen) — is **app domain** (it knows sessions, users, reveals) and
stays in the server. This crate provides the *mechanism* (watch/unwatch/notify +
scripthash ref-counting + connection affinity); the server provides the *policy*.
**Decided (Q1): the crate stops at `WatchPool`** (the mechanism); the server owns
the `session → address` map and the watch/unwatch *timing*.

---

## 2. Version / dependency pinning (must match the workspace)

To interoperate with the rest of emvault + the GroupVault server, types must be
identical versions:

- `bdk_wallet = "3.1.0"`, `bitcoin = "0.32.10"` — same as `emvault-esplora`.
- `bdk_electrum` — the BDK↔Electrum sync adapter, pinned to the release
  compatible with `bdk_wallet 3.1`. It **wraps `electrum-client`** internally and
  re-exports it.
- `electrum-client` — the watch surface (`ElectrumApi`). **Critical:** we must
  use the *same* `electrum-client` version `bdk_electrum` depends on, ideally by
  re-exporting `bdk_electrum::electrum_client`, so the sync half and the watch
  half share one connection/type universe. **Decided (Q2): yes — re-export
  `bdk_electrum::electrum_client`; no separate hand-pinned `electrum-client` dep.**
- `thiserror = "2"`, `hex = "0.4"` — mirror esplora crate.
- `edition = "2024"` (scaffold already set).

No `futures` / `buffer_unordered` (same invariant as `emvault-esplora`: sync
futures stay `Send` for axum).

---

## 3. Upstream facts this design relies on (verified 2026-07-20)

- **romanz/electrs v0.11.1** (deployed on the node) supports
  `blockchain.scripthash.subscribe` **and** `.unsubscribe` (unsubscribe added
  v0.9.12). Unsubscribe returns `true`/`false`.
- **`electrum-client`** `ElectrumApi`:
  - `script_subscribe(&Script) -> Result<Option<ScriptStatus>>` — watch; returns
    the address's **current status immediately** (`Error::AlreadySubscribed` on
    dup) → folds the "initial check" into the watch call.
  - `batch_script_subscribe(..)` — watch many in one round-trip (session open).
  - `script_unsubscribe(&Script) -> Result<bool>` — unwatch.
  - `script_pop(&Script) -> Result<Option<ScriptStatus>>` — drain a queued status
    change for a watched script.
  - `raw::RawClient` exposes a **notification receiver channel** (push) as an
    alternative to polling `script_pop`.
- `ScriptStatus` is the Electrum **status hash** of an address's history — a
  *change signal*, not the data. A change → re-fetch details
  (`script_get_history` / `script_list_unspent` / `get_balance`).

---

## 4. The core constraint that shapes everything: subscriptions are per-connection

Electrum subscription state lives on the **exact TCP connection** that issued the
`subscribe`. Therefore:

- A watched scripthash is owned by **one connection**. The manager needs
  **connection affinity** — subscribe and unsubscribe for a given scripthash must
  hit the same connection. A stateless round-robin pool is wrong.
- On disconnect, **all** of that connection's subscriptions vanish → on reconnect
  we must **re-subscribe the active set** and **replay `get_history`** to backfill
  anything missed during the gap.
- Server CPU scales with the live subscription set (electrs recomputes subscribed
  statuses each block/mempool change) → the app's **aggressive unwatch** policy is
  what bounds it; the mechanism must make unwatch cheap and reliable.

Design consequence: **one watch connection == one unit of subscription
ownership.** We model that directly (§6).

---

## 5. Async model  **(decided, Q3: dedicated blocking worker-thread-per-connection)**

`electrum-client`'s `Client` is **blocking**; `bdk_electrum` is **blocking**; the
GroupVault server is **async (axum)**, and the app-spec invariant requires
sync/broadcast futures to be `Send`. **Decided model (Q3):**

- **Sync/broadcast half** (`ElectrumBackend`, §6.1): async facade that runs the
  blocking `bdk_electrum` call on `tokio::task::spawn_blocking`. Short-lived or
  pooled connection; **no subscription state**, so affinity doesn't apply. This
  mirrors `EsploraBackend` 1:1 in ergonomics.

- **Watch half** (`WatchConn`, §6.2): each watch connection is driven by a
  **dedicated blocking worker thread** that owns the `electrum-client` `Client`
  and its `RawClient` notification receiver. The public API is async and speaks to
  the worker over channels:
  - commands in: `mpsc<WatchCmd>` (`Watch{spk,reply}`, `Unwatch{spk,reply}`,
    `BatchWatch{..}`) — each carries a `oneshot` reply.
  - notifications out: `broadcast`/`mpsc<ScriptUpdate>` the app subscribes to.
  This keeps axum handlers async + `Send`, isolates blocking I/O, and gives us a
  natural place to run the reconnect+resubscribe loop (§7).

  *Alternative considered:* expose the blocking `Client` directly and let the app
  wrap it. Rejected — it leaks the per-connection constraint and the reconnect
  bookkeeping into the app, which is exactly what Option A is meant to own.

---

## 6. Public API sketch

### 6.1 `ElectrumBackend` — BDK sync/broadcast (mirrors `EsploraBackend`)

```rust
pub struct ElectrumBackend { /* bdk_electrum client + network + opts */ }

impl ElectrumBackend {
    /// Connect to electrs. `url` like "ssl://host:50002" or "tcp://host:60001".
    pub fn connect(url: &str, network: Network) -> Result<Self, ElectrumError>;

    #[must_use] pub fn with_opts(self, opts: ElectrumSyncOpts) -> Self;
    #[must_use] pub fn network(&self) -> Network;

    /// Initial/steady-state scan of `wallet` (revealed-range + gap).
    pub async fn sync(&self, wallet: &mut bdk_wallet::Wallet)
        -> Result<ElectrumSyncResult, ElectrumError>;
    /// Full rescan (Rescan action / idle-refresh of a keychain).
    pub async fn rescan(&self, wallet: &mut bdk_wallet::Wallet)
        -> Result<ElectrumSyncResult, ElectrumError>;
    /// Broadcast a fully-signed tx; returns its txid.
    pub async fn broadcast(&self, tx: &Transaction) -> Result<Txid, ElectrumError>;

    /// Batch history/balance for a set of scripts. The idle-refresh poller of
    /// exposed-but-unwatched addresses lives in the **app** (Q5); this is the
    /// primitive it calls (no subscription).
    pub async fn scan_scripts(&self, spks: &[ScriptBuf])
        -> Result<Vec<AddressState>, ElectrumError>;
}
```

`ElectrumSyncResult` mirrors `EsploraSyncResult` exactly (`changeset`,
`blocks_synced`, `new_mempool_txs`, `tip_height`) so `emvault-core` can `From`-
convert it into `chain_sync::SyncResult`, the same seam esplora uses.

### 6.2 Watch layer

```rust
/// One electrs connection that owns a set of live subscriptions.
pub struct WatchConn { /* handle to the worker thread + channels */ }

impl WatchConn {
    pub fn connect(url: &str) -> Result<Self, ElectrumError>;

    /// Subscribe; returns current status immediately (the "initial check").
    pub async fn watch(&self, spk: &Script) -> Result<Option<ScriptStatus>, ElectrumError>;
    pub async fn batch_watch(&self, spks: &[ScriptBuf])
        -> Result<Vec<Option<ScriptStatus>>, ElectrumError>;
    /// Unsubscribe; true if it was watched.
    pub async fn unwatch(&self, spk: &Script) -> Result<bool, ElectrumError>;

    /// Stream of status-change notifications for this connection's scripts.
    pub fn updates(&self) -> tokio::sync::broadcast::Receiver<ScriptUpdate>;
}

pub struct ScriptUpdate { pub scripthash: ScriptHash, pub status: Option<ScriptStatus> }
```

### 6.3 `WatchPool` + `WatchRegistry` — affinity + ref-counting (the mechanism half of the manager)

```rust
/// N WatchConns + a scripthash→conn map (affinity) + refcounts.
pub struct WatchPool { /* Vec<WatchConn>, registry, assignment policy */ }

impl WatchPool {
    pub fn connect(url: &str, conns: usize) -> Result<Self, ElectrumError>;

    /// Ref-counted watch: first watcher subscribes on an assigned conn;
    /// later watchers just bump the count. Returns current status on first watch.
    pub async fn watch(&self, spk: &ScriptBuf) -> Result<Option<ScriptStatus>, ElectrumError>;
    /// Ref-counted unwatch: real `unsubscribe` only when the count hits 0.
    pub async fn unwatch(&self, spk: &ScriptBuf) -> Result<(), ElectrumError>;

    /// Merged notification stream across all conns.
    pub fn updates(&self) -> tokio::sync::mpsc::Receiver<ScriptUpdate>;
}
```

**Decided (Q1): the crate stops here — at `WatchPool`** (affinity + refcount +
notify + reconnect). The server owns the `session → scripthash` map and the
watch/unwatch *timing*. `WatchPool` is deliberately session-agnostic: it keys on
scripthashes, not sessions.

---

## 7. Reconnect & backfill (app-spec open-Q#2, now load-bearing)

Each `WatchConn` worker keeps its **own registry** of currently-subscribed
scripthashes. On disconnect it:

1. reconnects with backoff,
2. **re-subscribes** the whole active set (`batch_script_subscribe`),
3. for each, compares the returned status to the last-known cached status and
   **emits a `ScriptUpdate` on any difference** (so a payment that landed during
   the gap is not missed) — i.e. an implicit `get_history` replay is unnecessary
   because subscribe returns current status; the *app* re-fetches details on the
   emitted change.

**Decided (Q4): auto-resubscribe lives inside the crate.** This is internal
(Option A owns it); the app just sees a possible burst of `ScriptUpdate`s after a
blip, never a "connection reset" it has to handle.

---

## 8. Transport / TLS  **(decided, Q6: TLS electrs)**

electrs is currently `electrum_rpc_addr = 127.0.0.1:60001` (plaintext, localhost).
The app server is on a different host, so the link must be secured. **Decided
(Q6): terminate TLS in front of electrs and connect with `ssl://host:port`.**
Consequences for this crate: enable `electrum-client`'s **rustls** feature; the
`connect` URL scheme is `ssl://`. (electrs itself has no native TLS listener, so
the server-side work is a thin TLS terminator — e.g. a minimal stunnel/nginx-
stream or a rustls proxy — in front of `:60001`; that's infra, tracked in the
node workstream, not this crate.) No SOCKS/proxy feature needed.

---

## 9. Error type

`ElectrumError` (thiserror) mirroring `EsploraSyncError`'s ergonomics:

```rust
pub enum ElectrumError {
    Electrum(#[from] electrum_client::Error),   // transport/protocol
    Bdk(#[from] bdk_electrum::…::Error),        // sync/apply
    CannotConnect(#[from] local_chain::CannotConnectError),
    Malformed { what: &'static str, value: String },
    Disconnected,                                // worker/thread gone
}
```

with helpers like `is_disconnected()` / `is_timeout()` so callers react without
matching inner types (same pattern as esplora's `http_status`/`is_rate_limited`).

---

## 10. Testing plan

- **Unit** — refcount/affinity logic in `WatchPool` (no network); URL scheme
  parsing; error mapping.
- **Mechanism (mock electrs)** — a tiny fake Electrum TCP server (or `wiremock`-
  style) to exercise subscribe/pop/unsubscribe + reconnect-resubscribe without a
  node. Mirrors esplora's offline sync/broadcast tests.
- **Live smoke test** (now runnable — node synced + funded): against the VPS
  electrs, subscribe to a fresh testnet4 address → send coins → assert a
  `ScriptUpdate` fires → `unwatch` returns `true`, second returns `false`. Gated
  like esplora's live signet tests. *(Greg said we can skip running this by hand;
  keep it as the gated integration test.)*

---

## 11. Decisions (locked 2026-07-21)

All eight resolved by Greg; folded into the sections above.

| # | Question | Decision |
|---|----------|----------|
| Q1 | Crate boundary | **Stop at `WatchPool`** — crate = mechanism (affinity + refcount + notify + reconnect); server owns `session → address` map + watch/unwatch timing. |
| Q2 | Shared `electrum-client` | **Depend on `bdk_electrum`, re-export its `electrum_client`** for the watch half — one version, one type universe. No separate pin. |
| Q3 | Async model | **Dedicated blocking worker-thread-per-connection** with a channel API (async facade over the blocking client). |
| Q4 | Reconnect ownership | **Auto-resubscribe inside the crate** — app never handles connection resets. |
| Q5 | Idle-refresh poller | **Lives in the app**; the crate exposes `ElectrumBackend::scan_scripts` as the primitive it calls. |
| Q6 | Transport | **TLS electrs (`ssl://`)** — `electrum-client` rustls feature; server-side TLS terminator is node-infra work. |
| Q7 | Notification transport | **`RawClient` push channel** (not `script_pop` polling) → real-time, no poll-interval latency. |
| Q8 | BDK sync path | **Wrap `bdk_electrum`'s `BdkElectrumClient`** for `sync`/`rescan`/`broadcast`, mirroring how `emvault-esplora` wraps `esplora-rs`. |

## 12. The scan procedure — foundation (2026-07-21)

### 12.1 Mental model: who scans, and what "scan" means here

This is the load-bearing distinction that shapes the whole crate.

- **Full-node backends (Bitcoin Core RPC, Liquid/Elements) scan *blocks*, client-
  side.** `bdk_bitcoind_rpc`'s emitter (and the Elements equivalent) walks the
  chain from the wallet's last height forward, streams each **block** into BDK,
  and **BDK matches every tx against the wallet's SPKs locally.** The node just
  serves raw blocks; the descriptor must be present so the client can derive SPKs
  and match. Real block traversal happens on our side.

- **The electrum backend does NOT scan blocks — electrs does, once, at index
  time.** electrs pre-scans every block and maintains a `scripthash → history`
  index. Client queries (`get_history` / `list_unspent` / `subscribe`) are served
  **from that index; there is no per-query block re-scan, and the client never
  downloads or matches blocks.** The app only ever asks about the **specific
  scripthashes it derived** from the descriptor.

  **Therefore `emvault-electrum` has no "scan blocks" step at all.** Every
  operation is a **per-scripthash query against electrs's prebuilt index**, in
  three access patterns over the *same* index:

  | Pattern | Call(s) | Purpose |
  |---------|---------|---------|
  | **sync** | `bdk_electrum` (`get_history` + `list_unspent` per SPK) | Build/refresh BDK wallet state (balances, UTXOs) for a set of SPKs. |
  | **watch** | `script_subscribe` → push notifications | Live change-signal for the hot set; subscribe returns current status = the initial check. |
  | **idle-refresh** | `scan_scripts` (batch `get_history`/`get_balance`) | Low-freq catch-all poll of exposed-but-unwatched SPKs. |

  Privacy note that motivates the whole design: electrs sees *which* scripthashes
  we ask about (so it must be **our** electrs), but the **descriptor never leaves
  the app** and **no blocks are pulled**.

### 12.2 The idle scanner, precisely

The **live-subscription set is deliberately minimal**: `revealed × open-session ×
not-yet-paid`. We aggressively `unsubscribe` to bound electrs CPU. The **idle
scanner** is the safety-net **poll** (not a subscription) over addresses we still
care about that fall *outside* that hot set:

- an address that **already received a payment** (its watcher was torn down at
  payment-seen) but could receive **another** payment (reuse / shared / donation
  address);
- **(future) donor-interface addresses** handed straight to donors — no user
  session is open to watch them, but funds can land anytime.

It's app-owned (the app knows the exposed set + TTLs), runs on a low-frequency
cadence, calls `ElectrumBackend::scan_scripts`, and **ages addresses out** once a
TTL expires (stop polling entirely). `emvault-electrum` supplies only the
`scan_scripts` primitive.

### 12.3 Scan-procedure decisions & remaining forks

1. **`sync` SPK set — DECIDED (2026-07-21): revealed-only.** `sync` queries
   **exactly the app-revealed SPK set** the app passes in — no gap-limit discovery
   on the hot path. Rationale: GroupVault's app is the sole minter of addresses
   (server-side derivation + reveal tracking), so discovery is wasted electrs
   load. **Paired recommendation (to confirm):** keep **`rescan` = gap-limit
   discovery** as the deliberate recovery/audit path — "rebuild from descriptor
   alone when reveal history is gone/suspect" — mirroring `emvault-esplora`'s
   incremental-`sync` vs full-`rescan` split. So the gap scan exists but is only
   paid when explicitly requested.
2. **Session-open sequence — DECIDED (2026-07-21): B, diff-driven.**
   `batch_script_subscribe` **first** (one round-trip, returns each SPK's current
   status = the free initial check). The app diffs each returned status against
   its **stored last-status**; only for SPKs that **differ or are uncached** does
   it fetch details (targeted `sync` → BDK `ChangeSet`). Subscribe both arms live
   watching *and* serves as the initial check in a single call. **Requires:** the
   app persists the BDK `ChangeSet` **and** per-address last-status across
   sessions; a **cold cache** (first open) degrades to a full revealed-only
   `sync` (no correctness loss). Chosen because GroupVault already persists the
   changeset + reveal state, so session-open latency scales with *what changed*,
   not vault size.
3. **`scan_scripts` return shape — DECIDED (2026-07-21): lightweight view (A).**
   `scan_scripts` returns `Vec<AddressState { scripthash, status,
   confirmed_sats, unconfirmed_sats }>` — **no UTXOs, no BDK mutation**; a cheap
   read-only "did anything change + what's the balance" probe. On a detected
   change the app runs a **targeted `sync`**, which is the **only** writer of
   UTXOs/spendable state into BDK. → exactly one authoritative UTXO source (BDK);
   poller stays cheap.
4. **Reorg / confirmed-vs-unconfirmed — DECIDED (2026-07-21).** No bespoke
   reorg code in the crate: on the authoritative path, **`bdk_electrum`/BDK handle
   reorgs natively** (anchors + `local_chain`; a reorged-out tx moves back to
   unconfirmed on the next sync). Confirmation transitions **and** reorgs both
   change the electrum status hash → detected via watch-notification or
   `scan_scripts` → app triggers a targeted `sync` → BDK reconciles. `scan_scripts`
   stays a pure detector and already exposes the **confirmed vs unconfirmed sats
   split** (`get_balance`) so the app can see 0-conf funds without a sync.
   **Confirmation-finality policy (N-conf before "spendable"/"final") lives in the
   app** — the crate exposes confirmed/unconfirmed + BDK anchors and imposes no
   threshold.
```

---

## 13. Phased build plan (2026-07-21)

**Integration harness:** `~/Projects/test-app-electrum` (a copy of `test-app-xpub`
— axum web app: login, Jade/Trezor onboarding, federations, addresses, proposals,
migrations). It's chain-backed by **bitcoind RPC** today (its `WalletManager` in
`src/wallet.rs`: `new`/`sync`/`sync_lineage`/`rescan`(Emitter)/`broadcast_raw`/
`balance`). The integration **swaps that chain seam to `emvault-electrum`** and
adds the session-watcher wiring. The testnet4 node + electrs are **live and
funded**, so live tests are runnable from Phase 1 on.

**Boundaries:** Phases 0–3 = the `emvault-electrum` crate. Phases 4–6 = app-side
wiring in `test-app-electrum`. Greg commits both repos (agent preps + stages +
shows diffs). Every Rust cycle ending in a human test ends with `cargo fmt` +
`cargo clippy --all-features -- -D warnings -W clippy::pedantic -W rust-2018-idioms`.

### Phase 0 — Scaffold + deps compile
- Resolve exact versions: `bdk_electrum` compatible with `bdk_wallet 3.1` +
  `bitcoin 0.32.10`, and the `electrum_client` it re-exports (enable **rustls**).
- `Cargo.toml` + module layout (`lib.rs`, `backend.rs`, `watch.rs`, `pool.rs`,
  `result.rs`, `error.rs`) + public type/signature skeleton (`ElectrumBackend`,
  `WatchConn`, `WatchPool`, `AddressState`, `ElectrumSyncResult`, `ElectrumError`)
  with `todo!()` bodies. **Deliverable:** `cargo build` green; compiling shell to review.

### Phase 1 — Sync/broadcast half (`ElectrumBackend`, wraps `BdkElectrumClient`)
- `connect(ssl://…)`, `sync` (revealed-only over app-supplied SPKs → BDK `ChangeSet`),
  `rescan` (gap-limit, recovery), `broadcast`, `scan_scripts` (lightweight
  `get_balance`/`get_history` → `AddressState`, no BDK mutation). `ElectrumSyncResult`
  mirrors `EsploraSyncResult`; `ElectrumError` mirrors `EsploraSyncError` ergonomics.
- Tests: offline mock-electrum control-flow (mirror esplora's wiremock suite) +
  **gated live test vs the VPS electrs** (fund an address → assert balance/UTXOs).
- **Deliverable:** BDK sync/broadcast works end-to-end against live testnet4 electrs.

### Phase 2 — Watch half (`WatchConn`)
- Dedicated blocking worker-thread-per-connection; command channel
  (`watch`/`batch_watch`/`unwatch`) + `RawClient` push → `broadcast<ScriptUpdate>`.
  TLS (`ssl://`).
- Test (the deferred smoke test, now an integration test): subscribe a funded
  address → observe a tx → assert `ScriptUpdate` fires; `unwatch` → `true`, second → `false`.
- **Deliverable:** live watch works.

### Phase 3 — `WatchPool` (affinity + refcount + reconnect)
- N `WatchConn`s; `scripthash → conn` affinity; ref-counted watch/unwatch (real
  subscribe/unsubscribe only at the 0↔1 edge); merged notification stream;
  **auto-resubscribe + status-diff replay on reconnect**.
- Tests: unit (refcount/affinity, no network) + live reconnect (drop conn → assert
  resubscribe + missed-delta emit). **Deliverable:** production-grade watch mechanism.

### Phase 4 — Integrate into `test-app-electrum` (chain-seam swap)
- Replace the `WalletManager` bitcoind-RPC/Emitter path with `ElectrumBackend` for
  `sync`/`rescan`/`broadcast`/`balance`. Add config (`ELECTRUM_URL`, TLS). Rename
  internal lib `test_app_xpub` → `test_app_electrum`. Point at the live testnet4
  electrs (via the TLS terminator, or a dev tunnel until that infra lands).
- **Deliverable:** federation balances/addresses/broadcast run through electrum;
  manual e2e: onboard → reveal → fund from faucet → balance shows.

### Phase 4.5 — WebSockets in `test-app-electrum` (live push, no reload)
- Add a WS endpoint + browser client to the test app so a received payment surfaces
  **with no page reload / no user action**. Includes a thin watch→WS bridge (the
  `WatchPool` `ScriptUpdate` stream → server push → DOM update) — a vertical slice
  sufficient to *demonstrate* live receipt; the full session-scoped lifecycle is
  Phase 5.
- **Deliverable:** fund a revealed address → the balance/tx appears live in the
  browser, untouched.

### Phase 5 — Session-watcher wiring (test-app) + `emvault-electrum` release-readiness
- **Session-watcher (test-app):** the diff-driven B flow — session-open →
  `batch_watch` + status-diff vs stored → targeted `sync` on deltas; notification →
  update balance/UTXO + drop watcher on payment-seen; session-close → ref-counted
  `unwatch`. Full lifecycle over `WatchPool`.
- **Release-readiness (`emvault-electrum` crate):** `fmt` + pedantic `clippy`;
  README; gated live-test harness documented (VPS electrs); packaging mirroring
  `emvault-esplora`. TLS-terminator infra noted as a node-workstream item.
- **Deliverable:** live payment drives the app in real time via the full watcher
  lifecycle; the crate is publish-ready.

### ⛔ GATE — Greg reviews the full test-app integration
- Human checkpoint **after Phase 5, before publish**: Greg reviews the completed
  test-app integration end-to-end — electrum chain-seam swap + live-WS receipt +
  the full session-watcher lifecycle — and the crate's release-readiness. Proceed
  to publish only on his go.

### 🚀 PUBLISH — `emvault-electrum` to crates.io (Greg)
- After the gate, Greg publishes `emvault-electrum` (agent preps; Greg is the
  publisher, as with the rest of the emvault suite).

### Phase 6 — Integrate `emvault-electrum` into **GroupVault** + idle-refresh poller
- Wire the published `emvault-electrum` into the real GroupVault server as its
  production chain backend (alongside/replacing esplora). Port the session-watcher
  wiring from the test-app into GroupVault's session/auth layer.
- **Implement the idle-refresh poller directly in GroupVault** (not the test app):
  low-frequency `scan_scripts` sweep over exposed-but-unwatched addresses; TTL
  age-out; detect "second payment" → targeted `sync`.
- **Deliverable:** GroupVault runs on the descriptor-private electrum backend with
  live watch + idle-refresh.

### Phase 7 — Final fmt/clippy sweep + deploy to testnet
- `fmt` + pedantic `clippy` across **GroupVault, `test-app-electrum`, and every
  emvault crate touched** along the way.
- Deploy the finished product to **testnet** (`testnet.groupvault.io`).
- **Deliverable:** GroupVault live on testnet4 over its own electrs.
