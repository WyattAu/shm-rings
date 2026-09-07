# shm-rings

Lock-free **SPMC** ring buffer over **shared memory**. One producer publishes
`Copy` messages; up to 8 independent consumers walk the stream concurrently —
same process or across processes mapping the same file. Backpressure-only:
the producer stalls when the slowest reader lags by a full capacity. No
overwrite mode, no journal, no mirrors. Just the ring.

```
producer ──try_push(&mut)──▶ [ ring file / mmap ] ──try_pop(&self)──▶ reader 0
                                                          ├─────────▶ reader 1
                                                          └─────────▶ reader 7
```

- 256-byte versioned header (4 cache lines, never shares a line with slot data)
- power-of-two masked indices — no modulo on the hot path
- `try_push -> bool` under backpressure; **zero `SeqCst`** anywhere
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

| bench                        | shape                                   | result |
|------------------------------|-----------------------------------------|--------|
| `push/fill_4096_no_readers`  | cold ring fill, no readers              | **~18.5 ns/push** |
| `push_pop/pingpong_1_reader` | push + pop round-trip, one reader       | ~18.9 ns/message |
| `push_pop/fanout_4_readers`  | producer + 4 draining reader threads    | ~141 ns/message sustained |

The < 100 ns/push target holds with an order of magnitude to spare on the
uncontended path; 4-reader fan-out pays cross-core cache-line traffic on the
header and stays well under 150 ns. `try_push` is one Relaxed load, an
N-wide Acquire min-scan, one volatile store, one Release store, one Relaxed
fetch_add — all L1-resident single-core.

## Verification coverage

| layer   | what it proves                                                              |
|---------|------------------------------------------------------------------------------|
| loom    | exhaustive interleavings of the ordering protocol: per-reader FIFO, no overwrite before slowest read, backpressure boundary (`cargo test --features loom`) |
| fuzz    | `libfuzzer` op-interleaving vs oracle (never panic, FIFO per reader, lag ≤ capacity); constructor fuzz (arbitrary paths/capacities → typed errors, never panic) (`cargo fuzz run`) |
| proptest| 500-case model-based op sequences vs `VecDeque` oracle with per-step invariants |
| integration | full lifecycle on real mmaps: exclusive create, roundtrip, boundary, reader independence, corruption/version/short-file rejection |
| miri    | design is miri-oriented (no transmutes, no ptr-to-int, atomics-only shared state); the syscall-backed mmap path itself is outside miri's model — safety is carried by the crate-level `# Safety` audit + loom double |

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
