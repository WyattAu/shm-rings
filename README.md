# shm-rings

[![docs.rs](https://docs.rs/shm-rings/badge.svg)](https://docs.rs/shm-rings)
[![crates.io](https://img.shields.io/crates/v/shm-rings.svg)](https://crates.io/crates/shm-rings)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)

Lock-free **SPMC** ring buffer over **shared memory**. One producer publishes
`Copy` messages; up to 8 independent consumers walk the stream concurrently —
same process or across processes mapping the same file. Backpressure-only:
the producer stalls when the slowest reader lags by a full capacity. No
overwrite mode, no journal, no mirrors. Just the ring.

```
producer ──try_push(&mut)──▶ [ ring file / mmap ] ──try_pop(&self)──▶ reader 0
                            │                       ├─────────▶ reader 1
                            │                       └─────────▶ reader 7
                            │
                            ├──claim/commit──▶ zero-copy Loan (v0.2)
                            └──signal(eventfd)──▶ pop_blocking (v0.2, `notify`)
```

- 256-byte versioned header (4 cache lines, never shares a line with slot data)
- power-of-two masked indices — no modulo on the hot path
- `try_push -> bool` under backpressure; **zero `SeqCst`** anywhere
- **zero-copy loans** (v0.2): `claim` → `Loan` (borrow the mapped slot) →
  `commit` / `abort` / drop — no byte leaves shared memory until you decide
- **eventfd notification** (v0.2, feature `notify`, Linux): producers
  `push_notified`, consumers `pop_blocking` — kernel parking, no lost wakeups
- header validated on every open: magic, version, mask, file length
- `#![deny(missing_docs)]`, `#![deny(unsafe_op_in_unsafe_fn)]`,
  `#![warn(clippy::undocumented_unsafe_blocks)]`

## Quickstart

```rust
use shm_rings::SpmcRingBuffer;

// Producer (exactly one, anywhere):
let mut ring = SpmcRingBuffer::<u64>::create_new("/dev/shm/demo.ring", 1024)?;
ring.try_push(&42);

// Consumers (any number of threads/processes, reader_id 0..8):
let reader = SpmcRingBuffer::<u64>::open_existing("/dev/shm/demo.ring")?;
if let Some(v) = reader.try_pop(0)? {
    assert_eq!(v, 42);
}
```

`T: Copy + zerocopy::FromBytes + zerocopy::Immutable`. The bounds are the
safety proof, not ceremony:

| bound       | why it is load-bearing                                                  |
|-------------|-------------------------------------------------------------------------|
| `Copy`      | slots are volatile-overwritten in place; `Drop` could not survive that  |
| `FromBytes` | slots may hold *any* bit pattern (fresh page or lapped old message)     |
| `Immutable` | consumers read through `&self` while the producer writes — no `UnsafeCell` |

## Zero-copy loans (v0.2)

`try_pop` always copies `size_of::<T>()` bytes out of the mapping. For large
fixed-layout records that copy is the hot path. The loan API replaces it with
a borrow:

```rust
// 1. Claim the next message (zero-copy reservation).
if let Some(mut loan) = reader.claim(0)? {
    // 2. Read the mapped bytes in place — nothing was copied.
    if loan.seq >= start {
        // 3. Commit: publish the cursor advance (or `loan.abort()` to
        //    leave the message for a later pass; dropping commits too).
        loan.commit()?;
    }
}
```

Semantics: up to `capacity` loans may be outstanding per reader; commits are
strictly FIFO (`LoanNotAtCursor` otherwise); `try_pop` fails with
`LoanOutstanding` while a loan is open (it would un-pin the loaned slot);
dropping a loan commits it if it is at the cursor — a drop can never corrupt
the pipeline. An open loan pins its slot by keeping the reader's cursor in
place, which is exactly the producer's backpressure invariant: **the
producer cannot overwrite a loaned slot**.

### ABI constraint: fixed-layout records only

A loaned record is read *in place* by another process, so `T` must be a
**fixed-layout record**: `#[repr(C)]` POD — no pointers, no `Drop`, no
padding-dependent semantics, no variable-length or versioned payloads. Both
sides must compile the *same type* (same size, alignment ≤ 64, same field
offsets). Embed an explicit version/sequence field in the record if the
schema can evolve. This is the same ABI discipline as the ring header
itself.

## Notification (v0.2, feature `notify`, Linux)

The ring is polling-only by default (`try_pop` returns `None` when empty;
spin or park as you choose). The optional `notify` feature adds an
`eventfd(2)`-based `EventNotify` and two handshake helpers:

```text
producer ──try_push──▶ [ ring file / mmap ] ──try_pop──▶ consumer
    │                                                    ▲
    └── signal(): write(eventfd) ────────── wait(): read(eventfd)
```

- producer: `push_notified(&v, &notify)` — publish (`Release`) **then**
  signal; backpressure publishes and signals nothing
- consumer: `pop_blocking(reader_id, &notify)` — empty-check (`Acquire`)
  **then** block in `poll(2)`; `pop_blocking_timeout` bounds the park;
  `EventNotify::as_fd()` integrates with `epoll`/waitsets

No lost wakeups: the eventfd is an accumulating counter, so a signal landing
between the consumer's empty-check and its `read` is consumed by that `read`
instead of being missed (unlike a compare-based futex wait). One `EventNotify`
serves any number of consumers; threads share it via `Arc`, processes via
`fork` or `SCM_RIGHTS` — the descriptor is the one handle you pass
out-of-band; the ring file stays the only path-addressed artifact.

## Memory ordering (the core argument)

Two protocols, four orderings, no `SeqCst`:

```text
Producer                          Consumer
--------                          --------
1. write slot bytes   (volatile)
2. write_idx += 1     (Release)   3. read write_idx   (Acquire)
                                  4. read slot bytes  (volatile)
                                  5. read_idx  += 1   (Release)
6. min(read_indices)  (Acquire)
```

- **Publish edge (1→4).** The `Release` store of `write_idx` in step 2 makes
  every byte written before it — including the volatile slot write — visible
  to any thread whose `Acquire` load (step 3) observes the new value. This is
  the standard release/acquire message-passing pattern (C++ §32.4, the
  promotion of release-consume).
- **Reclaim edge (5→6).** Reader cursor bumps are `Release`; the producer's
  min-read scan is `Acquire`. When the producer *sees* a cursor advance, all
  of that reader's volatile slot reads are ordered before any subsequent
  producer slot write. Combined with the rule *push only if
  `write_idx - min(read_idx) < capacity`*, the producer can never overwrite
  the slot under any reader's un-advanced cursor. This is the core safety
  property: **no overwrite before the slowest read**.
- **Why zero `SeqCst`.** `SeqCst` earns its cost only when several
  independently-ordered atomics must agree on one global interleaving
  (Dekker-style flags). Here there is a single producer index (written only
  by its owner) and one cursor per reader (ditto). Every cross-thread edge
  that matters is a single store→load pair on a single atomic — the exact
  shape acquire/release fully orders.
- **Dat3 (mixed orderings across different atomics).** Mixing orderings is
  hazardous only when correctness depends on the *relative* order of two
  unrelated atomics. Index atomics are the only synchronization points here;
  payload bytes are never accessed atomically and their visibility is always
  borrowed from the index protocol. The `Relaxed` uses are confined to
  single-writer values whose ordering cannot matter (the producer reading its
  own `write_idx`; the `total_written` diagnostic).

**v0.2 — loans** re-use the observe protocol unchanged and defer the reclaim
edge: `claim` performs step 3 (Acquire publish edge) and step 4 happens
lazily through the `Loan`'s `Deref`; `commit` performs exactly step 5. The
loan's safety is the core invariant applied to a pinned cursor: the producer
may overwrite slot `i` only when *every* cursor is past `i`, and while a
loan at `i` is open its reader's cursor stays ≤ `i` (FIFO commits, `try_pop`
gated off). One new handle-local quantity (`loan_depth`) gates pops; it is
`Relaxed` because it guards a same-handle sequencing rule, not a
cross-thread invariant.

**v0.2 — notification** chains a third message-passing edge after publish:

```text
producer: 1. write slot  2. write_idx += 1 (Release)  3. write(eventfd)
consumer: a. read write_idx (Acquire) → empty?  b. read(eventfd)  c. goto 3–5
```

Signal-*after*-publish and check-*before*-wait are program order on each
side; the kernel's read/write ordering gives happens-before from (3) to
everything after (c). A signal landing between (a) and (b) is accumulated by
the counter and consumed by (b) — there is no window in which it can be
missed. The loom model `model_notify_no_lost_wakeup` proves the user-space
skeleton (woken ⇒ message visible) exhaustively.

The loom models (`--features loom`) explore every interleaving of exactly
these protocols on an in-memory double, asserting per-reader FIFO and the
no-overwrite property exhaustively.

## Single-producer contract

Exactly one producer at a time. Within a process this is compiler-enforced:
`try_push(&mut self, ...)` cannot be called concurrently on one handle.
Across processes, operators guarantee it (one producer per file). Readers
need no coordination: `try_pop(&self, reader_id)` — share one handle across
threads or open one per process; each reader cursor is independent.

## Reader participation

Backpressure tracks the **slowest participating reader** (the `min` over
cursors `0..reader_count()`). `create_new` provisions all 8 cursors as
participants; `create_new_with_readers(path, capacity, n)` provisions `n` —
matching the readers you actually run. Every cursor that participates but is
never advanced is a permanently slow reader and will stall the producer at
capacity once the lag against it fills the ring. This is the contract that
makes the core safety property unconditional: the producer never overwrites
a slot that *any* participant may still read.

## `create_new` vs `open_existing`

`create_new` refuses to overwrite (`O_EXCL` semantics, plus an upfront
existence check that yields a typed `AlreadyExists`). The alternative —
`File::create` + re-init — is the classic shared-memory truncation footgun:
it would silently zero a ring that other processes still hold mapped,
teleporting every cursor into invalid state. Opening an existing ring is
always `open_existing`, which validates magic/version/mask/file-length
before trusting a byte. Files too short fail with `FileTooShort`; foreign or
damaged files fail with `HeaderCorruption`; version skew fails with
`VersionMismatch`. Clean-break v1: no byte compatibility with any prior
format.

## Benchmarks

Measured (criterion, `cargo bench`, this repo's CI runner, x86-64):

| bench                          | shape                                        | result |
|--------------------------------|----------------------------------------------|--------|
| `push/fill_4096_no_readers`    | cold ring fill, no readers                   | **~18.5 ns/push** |
| `push_pop/pingpong_1_reader`   | push + pop round-trip, one reader            | ~18.9 ns/message |
| `push_pop/fanout_4_readers`    | producer + 4 draining reader threads         | ~141 ns/message sustained |
| `consume/copy_u64`             | push + `try_pop`, 8-byte record              | ~33 ns/message |
| `consume/loan_u64`             | push + `claim`/`commit`, 8-byte record       | ~68 ns/message |
| `consume_1kib/copy_1kib_record`| push + `try_pop`, 1 KiB record               | ~364 ns/message |
| `consume_1kib/loan_1kib_record`| push + `claim`/`commit`, 1 KiB record        | **~186 ns/message (≈ 1.96× faster)** |
| `pingpong_2threads/spin`       | 2-thread handoff, busy-wait consume          | ~2–5 µs/round-trip, 60–90% CPU |
| `pingpong_2threads/notified`   | 2-thread handoff, eventfd blocking           | ~15–20 µs/round-trip, 10–25% CPU |

Reading the v0.2 rows:

- **Loans pay off at record sizes where the copy matters.** At 1 KiB the
  loan path is ~2× faster end-to-end (the push is shared, so the consume
  delta is larger still). At 8 bytes the copy is cheaper than the loan's
  extra atomic bookkeeping — use `try_pop` for small records, `claim` for
  large ones. The `try_push` path is byte-identical to 0.1.1; the push
  target (~18.5 ns) is unaffected. (`try_pop` gains one `Relaxed` load of
  the per-reader loan gate.)
- **Notification trades latency for CPU.** On an idle 2-thread ping-pong,
  spinning wins on round-trip latency (no syscall); blocking costs one
  `write` + one `poll`/`read` per handoff but uses roughly 3–6× less CPU —
  and unlike spinning it scales to idle-heavy traffic without burning a
  core. Numbers above are from a shared machine and are indicative; the
  CPU-utilization split is the robust signal.

The < 100 ns/push target holds with an order of magnitude to spare on the
uncontended path; 4-reader fan-out pays cross-core cache-line traffic on the
header and stays well under 150 ns. `try_push` is one Relaxed load, an
N-wide Acquire min-scan, one volatile store, one Release store, one Relaxed
fetch_add — all L1-resident single-core.

## Verification coverage

| layer   | what it proves                                                              |
|---------|------------------------------------------------------------------------------|
| loom    | exhaustive interleavings of the ordering protocol: per-reader FIFO, no overwrite before slowest read, backpressure boundary, loan pins cursor + rejects out-of-order commits, notify handshake (woken ⇒ message visible) (`cargo test --features loom`) |
| fuzz    | `libfuzzer` op-interleaving vs oracle (never panic, FIFO per reader, lag ≤ capacity); constructor fuzz (arbitrary paths/capacities → typed errors, never panic) (`cargo fuzz run`) |
| proptest| 500-case model-based op sequences vs `VecDeque` oracle with per-step invariants |
| integration | full lifecycle on real mmaps: exclusive create, roundtrip, boundary, reader independence, corruption/version/short-file rejection; loan commit/abort/drop semantics, overlapping loans to capacity, producer pinning, concurrent claim/commit + abort stress; eventfd wakeup real-thread tests incl. a 4k-message no-lost-wakeup stress and a ping-pong handoff |
| miri    | design is miri-oriented (no transmutes, no ptr-to-int, atomics-only shared state); the syscall-backed file-mmap path is outside miri's model ("Miri does not support file-backed memory mappings") — safety is carried by the crate-level `# Safety` audit + the zero-unsafe loom double |

## Positioning

| crate       | scope                                   | vs `shm-rings`                          |
|-------------|-----------------------------------------|-----------------------------------------|
| `crossbeam` | in-process channels (MPMC etc.)         | no shared memory, no cross-process       |
| `iceoryx2`  | full IPC framework (services, zero-copy, waitsets) | the minimal middle: just the ring |
| `rtrb`      | in-process SPSC ring                    | single-consumer, in-process only         |

`shm-rings` is the deliberately small piece none of them is: a versioned,
lock-free, cross-process SPMC ring with one page of semantics.

## License

MIT OR Apache-2.0
