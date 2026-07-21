//! The watch layer — [`WatchConn`]: one electrs connection that owns a set of
//! live scripthash subscriptions.
//!
//! Because Electrum subscription state is **per-connection** (§4 of the design),
//! one `WatchConn` == one unit of subscription ownership. It is driven by a
//! dedicated **blocking worker thread** (Q3) that owns the `electrum-client`
//! `Client`; the public API is async and speaks to the worker over channels.
//!
//! **Notification transport (Q7 nuance):** the blocking `electrum-client` `Client`
//! has no public push-notification receiver — notifications are only processed
//! during a `call()` and drained via `script_pop`. So the worker drives them with
//! a periodic `ping()` + `script_pop` poll and fans out changes on a `broadcast`
//! channel. Notification latency is therefore bounded by [`POLL_INTERVAL`] rather
//! than the zero-latency `RawClient` push the design imagined; the async raw
//! client could do true push, but the poll is simple, correct, and sufficient.

use std::collections::HashSet;
use std::sync::mpsc as smpsc;
use std::thread;
use std::time::Duration;

use bitcoin::{Script, ScriptBuf};
use tokio::sync::{broadcast, oneshot};

use crate::electrum_client::{Client as ElectrumClient, ElectrumApi, ScriptStatus};
use crate::error::ElectrumError;

/// How often the worker pings electrs + drains notifications. Bounds the
/// worst-case status-change → `ScriptUpdate` latency.
const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Capacity of the per-connection notification broadcast buffer.
const UPDATE_CHANNEL_CAP: usize = 1024;

/// How many times a dead connection is retried per worker iteration before
/// yielding back to the main loop (so a dropped handle is noticed promptly).
const RECONNECT_ATTEMPTS: u32 = 5;

/// Base backoff between reconnect attempts (multiplied by the attempt number).
const RECONNECT_BACKOFF: Duration = Duration::from_secs(1);

/// A status-change notification for a watched script.
#[derive(Debug, Clone)]
pub struct ScriptUpdate {
    /// The watched script pubkey.
    pub script: ScriptBuf,
    /// Its new electrum status hash (`None` if history cleared).
    pub status: Option<ScriptStatus>,
}

/// Commands sent to a connection's worker thread. (internal)
enum WatchCmd {
    Watch {
        script: ScriptBuf,
        reply: oneshot::Sender<Result<Option<ScriptStatus>, ElectrumError>>,
    },
    BatchWatch {
        scripts: Vec<ScriptBuf>,
        reply: oneshot::Sender<Result<Vec<Option<ScriptStatus>>, ElectrumError>>,
    },
    Unwatch {
        script: ScriptBuf,
        reply: oneshot::Sender<Result<bool, ElectrumError>>,
    },
}

/// One electrs connection with its own live subscription set + notification fan-out.
#[derive(Clone)]
pub struct WatchConn {
    cmd_tx: smpsc::Sender<WatchCmd>,
    update_tx: broadcast::Sender<ScriptUpdate>,
}

impl WatchConn {
    /// Open a watch connection to electrs and spawn its worker thread.
    ///
    /// # Errors
    /// [`ElectrumError::Electrum`] if the connection can't be established.
    pub fn connect(url: &str) -> Result<Self, ElectrumError> {
        let (update_tx, _) = broadcast::channel(UPDATE_CHANNEL_CAP);
        Self::connect_with(url, update_tx)
    }

    /// Like [`connect`](Self::connect) but publishes notifications onto a caller-
    /// supplied broadcast channel, so a [`WatchPool`](crate::WatchPool) can share
    /// one merged stream across all its connections.
    ///
    /// # Errors
    /// [`ElectrumError::Electrum`] if the connection can't be established.
    pub(crate) fn connect_with(
        url: &str,
        update_tx: broadcast::Sender<ScriptUpdate>,
    ) -> Result<Self, ElectrumError> {
        // Establish (and validate) the connection up-front on the caller's thread.
        let client = crate::connect_watch_client(url)?;
        let (cmd_tx, cmd_rx) = smpsc::channel::<WatchCmd>();
        let worker_updates = update_tx.clone();
        let url = url.to_owned();
        thread::spawn(move || worker(client, url, &cmd_rx, &worker_updates));
        Ok(Self { cmd_tx, update_tx })
    }

    /// Subscribe to `script`; returns its current status immediately (the initial
    /// check). `Ok(None)` if the address has no history.
    ///
    /// # Errors
    /// [`ElectrumError::Electrum`] on protocol failure / [`ElectrumError::Disconnected`]
    /// if the worker is gone.
    pub async fn watch(&self, script: &Script) -> Result<Option<ScriptStatus>, ElectrumError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(WatchCmd::Watch {
                script: script.to_owned(),
                reply,
            })
            .map_err(|_| ElectrumError::Disconnected)?;
        rx.await.map_err(|_| ElectrumError::Disconnected)?
    }

    /// Subscribe to many scripts in one round-trip (session-open path). Returns the
    /// current status of each, in input order.
    ///
    /// # Errors
    /// [`ElectrumError::Electrum`] / [`ElectrumError::Disconnected`].
    pub async fn batch_watch(
        &self,
        scripts: &[ScriptBuf],
    ) -> Result<Vec<Option<ScriptStatus>>, ElectrumError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(WatchCmd::BatchWatch {
                scripts: scripts.to_vec(),
                reply,
            })
            .map_err(|_| ElectrumError::Disconnected)?;
        rx.await.map_err(|_| ElectrumError::Disconnected)?
    }

    /// Unsubscribe from `script`; `Ok(true)` if it was watched.
    ///
    /// # Errors
    /// [`ElectrumError::Electrum`] / [`ElectrumError::Disconnected`].
    pub async fn unwatch(&self, script: &Script) -> Result<bool, ElectrumError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(WatchCmd::Unwatch {
                script: script.to_owned(),
                reply,
            })
            .map_err(|_| ElectrumError::Disconnected)?;
        rx.await.map_err(|_| ElectrumError::Disconnected)?
    }

    /// A fresh receiver for this connection's status-change notifications.
    #[must_use]
    pub fn updates(&self) -> broadcast::Receiver<ScriptUpdate> {
        self.update_tx.subscribe()
    }
}

/// The blocking worker loop: owns the `Client`, applies commands, and drives
/// notifications (ping + `script_pop`) on a [`POLL_INTERVAL`] cadence. Exits when
/// every [`WatchConn`] handle (and thus every command sender) is dropped.
// The worker thread must OWN the client for its entire lifetime; by-value is
// correct even though the body only ever needs `&mut client`.
#[allow(clippy::needless_pass_by_value)]
fn worker(
    mut client: ElectrumClient,
    url: String,
    cmd_rx: &smpsc::Receiver<WatchCmd>,
    updates: &broadcast::Sender<ScriptUpdate>,
) {
    let mut watched: HashSet<ScriptBuf> = HashSet::new();
    loop {
        match cmd_rx.recv_timeout(POLL_INTERVAL) {
            Ok(cmd) => handle_cmd(&client, &mut watched, cmd),
            Err(smpsc::RecvTimeoutError::Timeout) => {}
            Err(smpsc::RecvTimeoutError::Disconnected) => break,
        }
        // A ping failure means the connection is dead → reconnect + resubscribe
        // (Q4: auto-resubscribe lives in the crate). Bounded per iteration so a
        // dropped `WatchConn` (cmd_rx disconnect) is still noticed promptly.
        if drain_notifications(&client, &watched, updates).is_err()
            && let Some(fresh) = reconnect(&url, &watched, updates)
        {
            client = fresh;
        }
    }
}

/// Rebuild the connection and re-subscribe the whole active set, emitting the
/// current status of each (so any change missed during the gap is re-checked by
/// the app). A few bounded attempts with backoff; `None` if still down.
fn reconnect(
    url: &str,
    watched: &HashSet<ScriptBuf>,
    updates: &broadcast::Sender<ScriptUpdate>,
) -> Option<ElectrumClient> {
    for attempt in 0..RECONNECT_ATTEMPTS {
        thread::sleep(RECONNECT_BACKOFF * (attempt + 1));
        let Ok(client) = crate::connect_watch_client(url) else {
            continue;
        };
        let scripts: Vec<ScriptBuf> = watched.iter().cloned().collect();
        if let Ok(statuses) =
            client.batch_script_subscribe(scripts.iter().map(ScriptBuf::as_script))
        {
            for (spk, status) in scripts.iter().zip(statuses) {
                let _ = updates.send(ScriptUpdate {
                    script: spk.clone(),
                    status,
                });
            }
            return Some(client);
        }
    }
    None
}

/// Apply one command against the client, updating the local watched set and
/// replying to the async caller.
fn handle_cmd(client: &ElectrumClient, watched: &mut HashSet<ScriptBuf>, cmd: WatchCmd) {
    match cmd {
        WatchCmd::Watch { script, reply } => {
            let r = client
                .script_subscribe(script.as_script())
                .map_err(ElectrumError::from);
            if r.is_ok() {
                watched.insert(script);
            }
            let _ = reply.send(r);
        }
        WatchCmd::BatchWatch { scripts, reply } => {
            let r = client
                .batch_script_subscribe(scripts.iter().map(ScriptBuf::as_script))
                .map_err(ElectrumError::from);
            if r.is_ok() {
                watched.extend(scripts);
            }
            let _ = reply.send(r);
        }
        WatchCmd::Unwatch { script, reply } => {
            // Make unwatch idempotent: a never/already-unsubscribed script reports
            // `Ok(false)` rather than a `NotSubscribed` error, so the ref-counted
            // `WatchPool` (and callers) get a clean true/false contract.
            let r = match client.script_unsubscribe(script.as_script()) {
                Ok(was) => {
                    watched.remove(&script);
                    Ok(was)
                }
                // The retrying `Client` wraps the underlying error in
                // `AllAttemptsErrored`, so unwrap to spot a `NotSubscribed`.
                Err(e) if is_not_subscribed(&e) => {
                    watched.remove(&script);
                    Ok(false)
                }
                Err(e) => Err(ElectrumError::from(e)),
            };
            let _ = reply.send(r);
        }
    }
}

/// Whether an electrum error is (or wraps) a `NotSubscribed` — the retrying
/// `Client` nests the real cause inside `AllAttemptsErrored`.
fn is_not_subscribed(e: &crate::electrum_client::Error) -> bool {
    use crate::electrum_client::Error;
    match e {
        Error::NotSubscribed(_) => true,
        Error::AllAttemptsErrored(errs) => errs.iter().any(is_not_subscribed),
        _ => false,
    }
}

/// Ping to drive a socket read (processing spontaneous notifications), then drain
/// every watched script's notification queue onto the broadcast channel.
fn drain_notifications(
    client: &ElectrumClient,
    watched: &HashSet<ScriptBuf>,
    updates: &broadcast::Sender<ScriptUpdate>,
) -> Result<(), ()> {
    // `ping` forces a round-trip so the client processes any queued server
    // notifications; a failure here means the connection is dead → signal reconnect.
    client.ping().map_err(|_| ())?;
    for spk in watched {
        while let Ok(Some(status)) = client.script_pop(spk.as_script()) {
            // Send failures mean "no live receivers" — safe to ignore.
            let _ = updates.send(ScriptUpdate {
                script: spk.clone(),
                status: Some(status),
            });
        }
    }
    Ok(())
}
