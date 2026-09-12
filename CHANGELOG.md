# Changelog

All notable changes to this project are documented here. Format: [Keep a
Changelog](https://keepachangelog.com/) — versions follow [semver](https://semver.org).

## [Unreleased]

## [0.2.0] - 2026-09-12

### Added
- **Zero-copy loans** (`loan` module, on-by-default): `claim(reader_id)`
  reserves the next message as a `Loan<'_, T>` — a `Deref`-able view into
  the mapped slot, no copy. Resolved by `commit` (advance cursor, the
  `try_pop` reclaim edge), `abort` (release without consuming, pipeline
  rewind), or drop (commit-at-cursor; never corrupts). Up to `capacity`
  overlapping loans per reader; FIFO commits (`LoanNotAtCursor` otherwise);
  `try_pop` gated with `LoanOutstanding` while a loan pins a slot. ABI
  constraint documented: fixed-layout (`#[repr(C)]` POD) records only.
- **Eventfd notification** (feature `notify`, Linux): `EventNotify`
  (`eventfd(2)`, `EFD_CLOEXEC | EFD_NONBLOCK`) plus `push_notified`
  (publish-then-signal) and `pop_blocking` / `pop_blocking_timeout`
  (check-then-park). Accumulating-counter handshake: no lost wakeups by
  construction. Spin/park remains available without the feature.
- New errors: `ShmRingError::LoanOutstanding`, `ShmRingError::LoanNotAtCursor`.
- Loom models: loan pins cursor against producer (rejects out-of-order
  commits); notify handshake (woken ⇒ message visible).
- Tests: `tests/loan.rs` (16), `tests/notify.rs` (7), 2 new loom models.
- Benches: `loan_bench` (loan vs copy, 8 B and 1 KiB records),
  `notify_bench` (notified vs spin 2-thread ping-pong, wall + CPU).

### Notes
- On-disk format unchanged from 0.1.x (`VERSION` stays 1); both additions
  are call-site opt-ins. The `try_pop` hot path gains one `Relaxed` load
  (per-reader loan gate); `try_push` is byte-identical to 0.1.1.
- Handle-local loan state (`claim_base`, `loan_depth`) lives in the
  `SpmcRingBuffer` struct, not the file — a mapping shared between handles
  of one process keeps per-handle pipelines; cross-handle cursor discipline
  is the documented operator contract, as before.

## [0.1.1] - 2026-09-07

### Added
- File-backed POD status block with magic + version validation.

## [0.1.0] - 2026-09-07

### Added
- Initial public release — lock-free SPMC shared-memory ring.


### Added
- Initial public release.
