# Changelog

All notable changes to `emvault-electrum` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> Entries for 0.5.0 and earlier were reconstructed from git history.
> This crate's first release was 0.4.0 (there is no 0.3.0 or earlier).

## [0.8.0] - 2026-08-16

### Added
- Expanded test coverage: a new `tests/mock_electrum.rs` mock-Electrum suite and
  additional `src/backend.rs` test cases.

### Changed
- Released in lockstep with the suite-wide v0.8.0 update.

## [0.7.0] - 2026-08-03

### Added
- `ElectrumBackend::tip_height()` — read the current chain tip over Electrum
  (`blockchain.headers.subscribe`) without a bitcoind RPC node.
- `ElectrumBackend::get_tx(txid)` — fetch a full transaction over Electrum
  (`blockchain.transaction.get`). Enables nodeless previous-transaction lookups
  (e.g. Trezor sign-data prev-tx fetch), so no consumer code path needs RPC when
  running in electrum mode.

### Changed
- Released in lockstep with the suite-wide v0.7.0 update.

## [0.6.0] - 2026-07-29

### Added
- Reorg-reconciliation support in the Electrum sync path.

### Changed
- Documentation updates.

## [0.5.0] - 2026-07-27

### Changed
- Dependency and lockfile refresh; version realigned across the emvault suite.

## [0.4.0] - 2026-07-22

Initial release.

### Added
- Descriptor-private Electrum chain backend for emvault (phases 0–2 and phase 4).
- Crate `README.md`.
