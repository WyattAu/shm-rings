# shm-rings — claims inventory

Every verifiable numeric / behavioral performance claim in README.md, mapped
to its proof artifact. Generated as part of the perf-claims proof-back pass
(0.2.1).

Status legend:

- **backed** — an existing bench/test asserts the claim; linked below.
- **proven** — unproven at survey time; a bench/test was added by this pass.
- **reworded** — claim adjusted to what the artifacts actually prove.

## Wall-clock numbers (README "Benchmarks" table)

Criterion numbers are indicative (shared CI runner, x86-64). The
deterministic per-binary regression gate is `benches/iai_ring.rs`
(iai-callgrind; CI-only, requires valgrind).

| Claim | Proof artifact | Status |
|---|---|---|
| `push/fill_4096_no_readers` ~18.5 ns/push | `benches/ring_bench.rs::push` + instruction-count gate `benches/iai_ring.rs::push_single` / `::push_batch_4096` | backed (instruction gate added this pass) |
| `push_pop/pingpong_1_reader` ~18.9 ns/message | `benches/ring_bench.rs::push_pop::pingpong_1_reader` | backed |
| `push_pop/fanout_4_readers` ~141 ns/message sustained | `benches/ring_bench.rs::push_pop::fanout_4_readers` | backed |
| `consume/copy_u64` ~33 ns/message | `benches/loan_bench.rs::consume::copy_u64` | backed |
| `consume/loan_u64` ~68 ns/message | `benches/loan_bench.rs::consume::loan_u64` | backed |
| `consume_1kib/copy_1kib_record` ~364 ns/message | `benches/loan_bench.rs::consume_1kib::copy_1kib_record` | backed |
| `consume_1kib/loan_1kib_record` ~186 ns/message (≈1.96× faster than copy) | `consume_1kib` copy vs loan groups in the same file | backed |
| 2-thread ping-pong spin ~2–5 µs/round-trip, 60–90% CPU | `benches/notify_bench.rs` (spin variant) | backed |
| 2-thread ping-pong notified ~15–20 µs/round-trip, 10–25% CPU | `benches/notify_bench.rs` (notified variant) | backed |
| "< 100 ns/push target holds with an order of magnitude to spare" | `benches/ring_bench.rs::push` + `benches/iai_ring.rs::push_single` | backed |
| Steady-state `try_push` / `try_pop` / `claim` / `commit` perform zero heap allocations (mmap ring: slots, header, cursors all shared memory) | `tests/zero_alloc_ring_ops.rs` (counting global allocator) | **proven** |

## Concurrency / ordering claims

| Claim | Proof artifact | Status |
|---|---|---|
| Zero `SeqCst` anywhere | `rg SeqCst src/` → no matches; orderings documented in README "Memory ordering" | backed |
| Publish edge / reclaim edge protocol (release/acquire, no overwrite before the slowest read) | `tests/loom.rs` models + `tests/proptest.rs` (op sequences vs `VecDeque` oracle) + `fuzz/` (libfuzzer vs oracle) | backed |
| Loans: up to `capacity` outstanding, FIFO commits, `try_pop` gated with `LoanOutstanding`, producer cannot overwrite a loaned slot | `tests/loan.rs` (16 tests) + `tests/loom.rs` loan models | backed |
| Notify: no lost wakeups (accumulating eventfd counter) | `tests/notify.rs` (7 tests incl. 4k-message stress) + `tests/loom.rs::model_notify_no_lost_wakeup` | backed |
| Header validated on every open: magic, version, mask, file length → typed errors | `tests/integration.rs` (corruption/version/short-file rejection) + `fuzz/` constructor fuzzing | backed |
| Power-of-two masked indices — no modulo on the hot path | structural (`src/ring.rs` mask arithmetic) + `benches/iai_ring.rs::push_single` instruction count | backed |

## Historical statements

| Statement | Status |
|---|---|
| "The `try_push` path is byte-identical to 0.1.1" (0.2.0 release note) | **reworded** — historical release-comparison; not re-verifiable today. Going forward the iai instruction gate (`benches/iai_ring.rs`) pins the push path's shape so any change is visible in CI. |

## Summary

- Backed by existing artifacts: 15
- Proven by artifacts added in this pass: 1 (steady-state zero-allocation
  push/pop/loan), plus an instruction-count gate backing the 18.5 ns/push
  claim against regressions
- Reworded: 1 (byte-identical-to-0.1.1 → historical note)

New proof artifacts added in 0.2.1:

- `benches/iai_ring.rs` — iai-callgrind instruction-count gate for
  `try_push` (single + 4096 batch), `try_pop`, and `claim`/`commit`.
- `tests/zero_alloc_ring_ops.rs` — counting-allocator proof that
  steady-state ring operations (including the backpressure rejection path)
  allocate nothing.
