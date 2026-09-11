# Requirements — shm-rings

Numbered, testable requirements. Every requirement maps to at least one named
test; every security-relevant test cites at least one requirement. Threat
IDs reference `THREAT-MODEL.md`.

Scope note: `shm-rings` provides a lock-free SPMC ring buffer over a
memory-mapped file — versioned `RingHeader` validation, power-of-two masked
indices, per-reader cursors, backpressure-only push, and a POD `status`
block for cross-process diagnostics.

## Functional

| ID | Requirement | Priority |
|----|-------------|----------|
| REQ-SR-001 | `SpmcRingBuffer::create_new` creates an exclusive, file-backed ring with the requested power-of-two capacity and reader count; an existing path is rejected | MUST |
| REQ-SR-002 | `try_push` publishes a message iff the slowest reader cursor leaves room (backpressure-only); readers advance independently and never observe torn or partially written slots | MUST |
| REQ-SR-003 | `try_pop(reader_id)` consumes FIFO-order messages for each reader; `peek` observes without consuming | MUST |
| REQ-SR-004 | `open_existing` maps a previously created ring; repeated opens share state and validation applies on every open | MUST |
| REQ-SR-005 | `RingHeader::validate` accepts only headers whose MAGIC and VERSION match the current build | MUST |
| REQ-SR-006 | The `status` module persists a POD status block: `read`/`write` round-trip any `PodStatus` value, `update` mutates in place, foreign files are rejected | SHOULD |
| REQ-SR-007 | `cleanup` removes the ring file and is idempotent | SHOULD |
| REQ-SR-008 | `capacity`, `len`, `is_empty`, `reader_count`, `slowest_lag`, and `total_written` report consistent diagnostics while the ring is in use | SHOULD |

## Security

| ID | Requirement | Priority |
|----|-------------|----------|
| REQ-SR-100 | A file whose header fails magic or version validation is never mapped into a usable ring (T1) | MUST |
| REQ-SR-101 | Non-power-of-two and zero capacities are rejected at create time; all index arithmetic is masked so a hostile index cannot reach outside the slot array (T2) | MUST |
| REQ-SR-102 | Ring indices across processes coordinate with Acquire/Release atomics only (zero SeqCst); the same protocol is model-checked under loom with no double-publish before the slowest read (T5) | MUST |
| REQ-SR-103 | Reader IDs are bounds-checked before use; an invalid ID is an error, not a pointer offset (T4) | MUST |
| REQ-SR-104 | `create_new` refuses to attach to an existing file (exclusive-create semantics), preventing silent adoption of an untrusted mapping (T3) | MUST |

## Robustness

| ID | Requirement | Priority |
|----|-------------|----------|
| REQ-SR-200 | A file shorter than `HEADER_SIZE` (or shorter than the header + capacity layout) is rejected with a typed error rather than truncating or panicking | MUST |
| REQ-SR-201 | Ring behavior under an arbitrary operation sequence matches a `VecDeque` oracle (model-based property test) | MUST |
| REQ-SR-202 | A reader that stops consuming pins the ring at capacity (documented stall); it recovers exactly when the reader advances — no data is dropped silently | MUST |
| REQ-SR-203 | `SpmcRingBuffer` handles are `Send + Sync` as documented | SHOULD |

## Traceability Matrix

| Requirement | Test (fn, file) | Property class |
|-------------|-----------------|----------------|
| REQ-SR-001 | `create_new_rejects_existing_path`, `create_overwrites_existing_file`, `non_power_of_two_capacity_rejected`, `zero_capacity_rejected` (`tests/*.rs`) | unit |
| REQ-SR-002 | `capacity_boundary_backpressure_then_recover`, `capacity_boundary_single_reader_ring_recovers_after_one_pop`, `loom_backpressure_boundary_never_crossed_without_reader_advance` | unit/property/loom |
| REQ-SR-003 | `roundtrip_push_pop`, `peek_is_non_consuming`, `readers_are_independent` | unit |
| REQ-SR-004 | `open_existing_is_idempotent_and_shared` | unit |
| REQ-SR-005 | `header_corruption_magic_detected`, `read_rejects_corrupted_magic`, `read_rejects_version_mismatch`, `version_mismatch_detected`, `file_too_short_detected` | unit |
| REQ-SR-006 | `round_trip_status_a`, `round_trip_status_b`, `update_persists_mutation`, `update_rejects_foreign_file`, `set_magic_stamps_fresh_value` (`src/status.rs` tests) | unit |
| REQ-SR-007 | `cleanup_removes_and_is_idempotent` | unit |
| REQ-SR-008 | `diagnostics_track_state` | unit |
| REQ-SR-100 | `header_corruption_magic_detected`, `version_mismatch_detected` | unit |
| REQ-SR-101 | `non_power_of_two_capacity_rejected`, `zero_capacity_rejected`; fuzz targets `fuzz_open.rs`, `fuzz_ring.rs` | unit/fuzz |
| REQ-SR-102 | `loom_backpressure_boundary_never_crossed_without_reader_advance`, `loom_fanout_two_readers_no_overwrite_before_slowest_read`, `model_based_ops_match_vecdeque_oracle` (`tests/loom.rs`, `tests/proptest.rs`) | loom/property |
| REQ-SR-103 | `invalid_reader_id_rejected` | unit |
| REQ-SR-104 | `create_new_rejects_existing_path` | unit |
| REQ-SR-200 | `read_rejects_short_file`, `file_too_short_detected` | unit |
| REQ-SR-201 | `model_based_ops_match_vecdeque_oracle`, `generated_sequences_have_useful_shape`, `op_strategy` (`tests/proptest.rs`) | property |
| REQ-SR-202 | `capacity_boundary_backpressure_then_recover`, `capacity_boundary_single_reader_ring_recovers_after_one_pop` | unit |
| REQ-SR-203 | `handles_are_send_and_sync` | unit |

## Test Count

- 29 `#[test]` functions across unit, integration, proptest, and loom suites.
- All-features suite (including loom model checking) passes with 0 failures;
  no-default-features suite passes.
