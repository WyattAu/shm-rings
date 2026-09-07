//! Lock-free SPMC ring buffer backed by a shared-memory-mapped file.
//!
//! `shm-rings` provides two shared-memory primitives over `mmap`-backed
//! files:
//!
//! - [`SpmcRingBuffer`] — a fixed-capacity, power-of-two masked,
//!   cache-line-aligned ring for streaming messages: a single producer
//!   publishes `Copy` messages with `try_push(&mut self)`; up to
//!   [`MAX_READERS`] independent consumers walk the stream concurrently
//!   with `try_pop(&self)`, in the same process or across processes mapping
//!   the same file. Flow control is backpressure-only: the producer stops
//!   when the slowest reader lags by a full capacity. No overwrite mode, no
//!   journal, no mirrors — just the ring.
//! - [`status`] — the state counterpart: a generic validated file-backed
//!   POD status (`create`/`read`/`update`/`cleanup` around a
//!   [`status::PodStatus`] impl), for publishing counters, flags, and
//!   heartbeats rather than streaming events.
//!
//! # Quickstart
//!
//! ```no_run
//! use shm_rings::SpmcRingBuffer;
//!
//! # fn main() -> Result<(), shm_rings::ShmRingError> {
//! // Producer side (one process / one thread):
//! let mut ring = SpmcRingBuffer::<u64>::create_new("/dev/shm/demo.ring", 1024)?;
//! ring.try_push(&42);
//!
//! // Consumer side (any number of threads/processes):
//! let reader = SpmcRingBuffer::<u64>::open_existing("/dev/shm/demo.ring")?;
//! if let Some(v) = reader.try_pop(0)? {
//!     assert_eq!(v, 42);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Single-producer contract
//!
//! Exactly one producer at a time, enforced within a process by
//! `try_push(&mut self, ...)`. Readers need no coordination:
//! `try_pop(&self, reader_id)` gives each `reader_id` its own independent
//! cursor into the same broadcast stream.
//!
//! # Reader participation
//!
//! Backpressure is computed against the **slowest participating reader**:
//! the `min` over cursors `0..reader_count()`. `create_new` provisions all
//! [`MAX_READERS`] cursors, and every cursor that has not been advanced
//! pins the ring exactly as a slow consumer would — pushing stalls once the
//! lag against it reaches capacity. Provision capacity for the readers you
//! will actually run: an unused reader slot is a permanently slow reader.
//! Calls with `reader_id >= reader_count()` fail with
//! [`ShmRingError::InvalidReaderId`].
//!
//! # Memory ordering
//!
//! The full acquire/release argument (publish edge, reclaim edge, why zero
//! `SeqCst` suffices, and why mixed orderings across different atomics are
//! safe here) is documented in the [`ring`] module — it is the intellectual
//! core of this crate.
//!
//! # Safety
//!
//! This crate contains inherent `unsafe` (mmap management, volatile slot
//! access, raw-pointer arithmetic into the mapping). `unsafe_code` is
//! therefore *not* forbidden crate-wide. Every unsafe site, with its
//! invariant:
//!
//! 1. **`create_new`: header pointer cast** (`ring.rs`) — `mmap.as_mut_ptr().cast::<RingHeader>()`.
//!    *Invariant:* the mapping is `HEADER_SIZE + capacity * size_of::<T>()`
//!    bytes (≥ `size_of::<RingHeader>()` = 256) and page-aligned, so a
//!    64-byte-aligned header fits at offset 0.
//! 2. **`create_new`: `header.write(RingHeader::new(..))`** — plain
//!    (non-atomic) initialization write.
//!    *Invariant:* the file was created microseconds ago by this call via
//!    `create_new(true)`; no other thread or process can have mapped it
//!    yet, so there is no concurrent access to order against. Later openers
//!    synchronize through the `Acquire` loads in `RingHeader::validate`.
//! 3. **`open_existing`: header pointer cast + `&*header`** — shared
//!    reference to the mapped header.
//!    *Invariant:* file length was checked ≥ `HEADER_SIZE` *before*
//!    mapping; all header fields are atomics, so a shared reference is
//!    aliasing-sound even while other processes write through their own
//!    mappings.
//! 4. **`slot_ptr`: byte offset arithmetic** —
//!    `base + DATA_OFFSET + (idx & mask) * size_of::<T>()`.
//!    *Invariant:* `idx & mask < capacity` and the mapping covers exactly
//!    `HEADER_SIZE + capacity * size_of::<T>()` bytes, so the byte range is
//!    in-bounds; `DATA_OFFSET` (256) and the slot stride are multiples of
//!    `align_of::<T>()` (≤ 64, asserted in both constructors), so the
//!    resulting `*mut T` is aligned.
//! 5. **`try_push`: `write_volatile(slot, *value)`** — publishes
//!    `size_of::<T>()` payload bytes.
//!    *Invariant:* in-bounds/aligned per (4); `T: Copy` makes the bitwise
//!    move well-defined; `T: FromBytes` means any bit pattern previously in
//!    the slot (fresh zero page or lapped old message) is a valid `T` for
//!    future readers; the write is ordered before the `Release` store of
//!    `write_idx` (module docs of [`ring`]).
//! 6. **`try_pop`: `read_volatile(slot)`** — consumes payload bytes.
//!    *Invariant:* in-bounds/aligned per (4); the backpressure protocol
//!    guarantees the producer cannot have overwritten the slot under this
//!    reader's cursor (overwriting requires *all* cursors — including this
//!    one, still un-advanced — to be past the slot), so no concurrent write
//!    exists; `T: Immutable` rules out interior mutability, so the read
//!    cannot race with writes into a `Cell` reachable via `&T`.
//! 7. **`unsafe impl Send`** — mapping ownership + all shared state atomic;
//!    no non-atomic shared mutable state.
//! 8. **`unsafe impl Sync`** — all `&self` access is atomics, ordered
//!    volatile payload access, or immutable fields; `T: Immutable` closes
//!    the interior-mutability hole.
//!
//! The loom double ([`loom_ring`], `--features loom`) re-runs the identical
//! ordering protocol with zero `unsafe`, so the *interleaving logic* is
//! exhaustively model-checked while the *mmap mechanics* are covered by
//! integration tests, property tests, and fuzzing.
//!
//! # Format stability
//!
//! Clean-break v1 ([`VERSION`]): the 256-byte header is versioned from day
//! one and validated on open. No byte compatibility with any prior format
//! is attempted; a mismatch fails with [`ShmRingError::VersionMismatch`].

#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]
#![warn(clippy::undocumented_unsafe_blocks)]

pub mod error;
pub mod header;
#[cfg(feature = "loom")]
pub mod loom_ring;
pub mod ring;
pub mod status;

pub use error::ShmRingError;
pub use header::{RingHeader, HEADER_SIZE, MAGIC, MAX_READERS, VERSION};
pub use ring::SpmcRingBuffer;
