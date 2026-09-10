# Threat Model — shm-rings

Reference: STRIDE. Scope: the crate's public API surface (`SpmcRingBuffer`
create/open/push/pop, `status` module, `RingHeader`) across process
boundaries. Trust boundaries: (1) the mapped file's contents (any process
with filesystem access can write it), (2) file identity during
create-vs-open races, (3) the dependency tree (libc/mmap via memmap2).

This crate's threat model is unusual: **the shared memory itself is hostile
territory**. Peer processes are outside the trust domain; the ring defends
structurally (validation, atomics, bounds) but cannot authenticate them.

## Assets

| ID | Asset | Example |
|----|-------|---------|
| A1 | Memory safety of both processes sharing the ring | A corrupted header causing OOB access or type confusion in the mapper |
| A2 | Availability of the stream (producer can make progress) | An un-advanced reader cursor pinning the ring at capacity |
| A3 | Format integrity across versions | A new build reading a stale/incompatible mapping |

## STRIDE Analysis

| # | Threat | Category | Surface | Mitigation | Verifying test |
|---|--------|----------|---------|------------|----------------|
| T1 | Corrupted header (magic/version) accepted | Tampering | `open_existing`, `status::read` | `RingHeader::validate` checks MAGIC and VERSION via Acquire loads before use; mismatches fail with `ShmRingError::VersionMismatch`/magic error | `header_corruption_magic_detected`, `read_rejects_corrupted_magic`, `read_rejects_version_mismatch`, `version_mismatch_detected` |
| T2 | Type confusion / OOB via crafted capacity or layout | Tampering/Elevation | `open_existing`, `slot_ptr` | Capacity is read from the header and masked (power-of-two enforced at create, mask applied at access); file length checked ≥ `HEADER_SIZE + capacity * slot` before mapping; alignment asserted in both constructors; every unsafe site carries a documented invariant | `non_power_of_two_capacity_rejected`, `zero_capacity_rejected`, `capacity_mask`; fuzz targets `fuzz_open.rs`, `fuzz_ring.rs`; SAFETY inventory in crate docs |
| T3 | Squatter/race on the ring file (fake mapping swapped in) | Spoofing | `create_new`, `open_existing` | `create_new` uses exclusive-create semantics and rejects an existing path (`InvalidAlreadyExists`); no TOCTOU-proof identity check (no inode pinning) exists beyond open-time validation — documented residual risk | `create_new_rejects_existing_path`, `create_overwrites_existing_file` |
| T4 | Reader-cursor poisoning by a rogue reader | DoS | `try_pop(reader_id)` | Reader IDs are bounds-checked (`InvalidReaderId`); cursors live in shared memory so a hostile reader can lie, but backpressure computes `min` over all cursors — a poisoned cursor degrades to the documented "permanently slow reader" stall, never memory unsafety | `invalid_reader_id_rejected`, `capacity_boundary_backpressure_then_recover`, `capacity_boundary_single_reader_ring_recovers_after_one_pop` |
| T5 | Data race on ring indices across processes | Tampering | `try_push`/`try_pop` | Acquire/Release protocol with zero `SeqCst`; the identical ordering protocol is re-run under loom with zero unsafe — exhaustively model-checked interleavings | `loom_backpressure_boundary_never_crossed_without_reader_advance`, `loom_fanout_two_readers_no_overwrite_before_slowest_read` (`tests/loom.rs`); proptests in `tests/proptest.rs` |
| T6 | Payload confidentiality across processes | Info disclosure | mapped file | **Not mitigated** — any process with file-read permission reads every message. Documented: filesystem permissions (`/dev/shm` mode bits) are the access control | Code review |
| T7 | Stale/uninitialized slots read as valid messages | Spoofing | `try_pop` | `T: Copy + FromBytes`-style bounds mean any bit pattern is a *valid* value; freshness is conveyed by the index protocol, not slot contents. Callers needing frame validity must embed their own sequence/parity | Documented single-producer contract in crate docs; `value_integrity` behavior via ring roundtrip proptests |

## Repudiation

Not applicable — shared memory has no inherent audit trail; the `status`
module publishes counters/heartbeats but nothing attributable.

## Out of Scope

- Peer-process compromise: a hostile process with write access to the file
  can corrupt payloads at will; the ring detects header/format damage (T1)
  but cannot authenticate payload provenance.
- Filesystem security (mode bits, ownership of `/dev/shm` files): OS domain.
- Network/IPC transport beyond the shared file.

## Residual Risks

- **R1 (Medium, accepted):** No inode/identity pinning at open (T3):
  between `create_new` and `open_existing`, the file can in principle be
  replaced; header validation bounds the damage to format errors, but a
  same-format imposter mapping is undetectable. Use directory permissions
  to restrict who can swap files.
- **R2 (Medium, accepted):** Unused reader slots pin the ring forever
  (documented in crate docs): provisioning `reader_count` larger than the
  real consumer count is a self-inflicted DoS (T4). Capacity planning is
  the operator's duty.
- **R3 (Low, accepted):** `status` module POD values are written with plain
  atomic stores from any writer; there is no CAS discipline for compound
  updates — last-writer-wins by contract.
- **R4 (Low, accepted):** `unsafe` surface is inherent (mmap + volatile);
  mitigated by the documented SAFETY inventory, `deny(unsafe_op_in_unsafe_fn)`,
  `warn(undocumented_unsafe_blocks)`, loom double, and fuzz targets — but the
  mmap mechanics themselves are test-covered, not proof-covered.
