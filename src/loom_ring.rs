//! Loom model double of the ring (compiled only with `--features loom`).
//!
//! Loom cannot model `mmap`/syscalls, so this module re-implements the ring
//! in memory with [`loom::cell::UnsafeCell`] slots and
//! [`loom::sync::atomic::AtomicU64`] indices, preserving the *identical*
//! ordering discipline of the real implementation:
//!
//! * producer: slot write (cell `set`, loom-tracked) → `Release` `write_idx`
//! * consumer: `Acquire` `write_idx` → cell `get` → `Release` own cursor
//! * producer backpressure: `Acquire` min over cursors vs `write_idx`
//!
//! The algorithmic code contains no `unsafe` at all — loom's cell type is a
//! safe, scheduler-tracked `UnsafeCell`. The real mmap path cannot be
//! modeled by loom; its safety argument lives in the crate-level `# Safety`
//! audit and is exercised end-to-end by the integration, proptest, and fuzz
//! suites. What loom buys here is exhaustive exploration of *interleavings*
//! for the ordering protocol itself.
//!
//! Capacity is fixed at 2 and message counts are tiny so the state space
//! stays tractable.

use loom::cell::UnsafeCell;
use loom::sync::atomic::{
    AtomicU64,
    Ordering::{Acquire, Relaxed, Release},
};
use loom::sync::Arc;

use crate::header::MAX_READERS;

/// Modeled capacity (kept tiny: loom explores every interleaving).
const LOOM_CAPACITY: u64 = 2;

/// In-memory loom double of [`crate::SpmcRingBuffer`] (u64 payloads).
pub struct LoomRing {
    write_idx: AtomicU64,
    read_indices: [AtomicU64; MAX_READERS],
    /// Participating reader count (mirrors the header's `num_readers`).
    num_readers: usize,
    total_written: AtomicU64,
    slots: [UnsafeCell<u64>; LOOM_CAPACITY as usize],
}

// SAFETY: mirrors the real ring's argument. All cross-thread state is
// either loom atomics (write_idx, read_indices, total_written) or loom's
// tracked UnsafeCell slots, whose access is ordered by the same
// acquire/release index protocol the real ring uses. No other shared
// mutable state exists.
unsafe impl Send for LoomRing {}

// SAFETY: same argument as `Send` — every `&self` access is an atomic on
// the index words or a loom-tracked cell access ordered by the acquire/
// release index protocol; `num_readers` is immutable after construction.
unsafe impl Sync for LoomRing {}

impl LoomRing {
    /// Fresh ring with all [`MAX_READERS`] cursors participating.
    pub fn new() -> Self {
        Self::with_readers(MAX_READERS)
    }

    /// Fresh ring with only the first `num_readers` cursors participating in
    /// backpressure — mirrors how the real header's `num_readers` field
    /// scopes the producer's min-read scan.
    pub fn with_readers(num_readers: usize) -> Self {
        assert!((1..=MAX_READERS).contains(&num_readers));
        Self {
            write_idx: AtomicU64::new(0),
            read_indices: std::array::from_fn(|_| AtomicU64::new(0)),
            num_readers,
            total_written: AtomicU64::new(0),
            slots: std::array::from_fn(|_| UnsafeCell::new(0)),
        }
    }

    /// Backpressure-only publish; identical protocol to the real
    /// [`crate::SpmcRingBuffer::try_push`].
    pub fn try_push(&self, value: u64) -> bool {
        // Relaxed: this thread is write_idx's only writer.
        let w = self.write_idx.load(Relaxed);
        // Acquire: reclaim edge — see a cursor advance ⇒ that reader's slot
        // reads are done. Only participating readers count.
        let min_read = self.read_indices[..self.num_readers]
            .iter()
            .map(|r| r.load(Acquire))
            .fold(u64::MAX, u64::min);
        if w - min_read >= LOOM_CAPACITY {
            return false;
        }
        // Loom cell write: `with_mut` is scheduler-tracked; slot index
        // < LOOM_CAPACITY by the mask. Stands in for the real path's
        // audited `write_volatile`.
        // SAFETY: loom guarantees the pointer handed to `with_mut` is valid
        // for writes and exclusively ours for the closure's duration.
        self.slots[(w & (LOOM_CAPACITY - 1)) as usize].with_mut(|s| unsafe { *s = value });
        // Release: publish edge — slot write is visible to whoever acquires
        // the new write_idx.
        self.write_idx.store(w + 1, Release);
        self.total_written.fetch_add(1, Relaxed);
        true
    }

    /// Consume one message as reader `reader_id`; identical protocol to the
    /// real [`crate::SpmcRingBuffer::try_pop`].
    pub fn try_pop(&self, reader_id: usize) -> Option<u64> {
        assert!(reader_id < self.num_readers);
        // Acquire: publish edge — slot bytes for all indices < w are
        // visible.
        let w = self.write_idx.load(Acquire);
        let r = self.read_indices[reader_id].load(Acquire);
        if r == w {
            return None;
        }
        // Loom cell read; the backpressure protocol guarantees the slot
        // under OUR cursor was not overwritten.
        // SAFETY: loom guarantees the pointer handed to `with` is valid for
        // reads for the closure's duration.
        let v = self.slots[(r & (LOOM_CAPACITY - 1)) as usize].with(|s| unsafe { *s });
        // Release: reclaim edge — we are done reading index r.
        self.read_indices[reader_id].store(r + 1, Release);
        Some(v)
    }

    /// Total successful pushes.
    pub fn total_written(&self) -> u64 {
        self.total_written.load(Relaxed)
    }
}

impl Default for LoomRing {
    fn default() -> Self {
        Self::new()
    }
}

/// Model 1 — one producer, two consumers, capacity 2, three messages.
///
/// Asserts the core safety property under *every* interleaving loom can
/// generate: per-reader FIFO — each reader observes the global stream
/// `1, 2, 3, ...` in order, with no gaps, duplicates, or stale values (a
/// stale value would mean the producer overwrote a slot before the slowest
/// reader passed it — the property the backpressure protocol exists to
/// guarantee).
///
/// Attempts are deliberately bounded: unbounded spin loops explode loom's
/// schedule exploration, and liveness is not what loom is proving here —
/// the ordering protocol is.
pub fn model_fanout_two_readers() {
    const MESSAGES: u64 = 3;
    const ATTEMPTS: usize = 3;
    loom::model(move || {
        let ring = Arc::new(LoomRing::with_readers(2));
        let mut handles = Vec::new();

        for id in 0..2usize {
            let ring = Arc::clone(&ring);
            handles.push(loom::thread::spawn(move || {
                let mut seen: u64 = 0;
                for _ in 0..ATTEMPTS {
                    if let Some(v) = ring.try_pop(id) {
                        // Per-reader FIFO against the global stream: the
                        // next value this reader may legally observe is
                        // `seen + 1`. Anything else is a torn/stale/lapped
                        // read — the bug class this crate must make
                        // impossible.
                        assert_eq!(v, seen + 1, "reader {id}: out-of-stream value");
                        seen += 1;
                    }
                }
                seen
            }));
        }

        {
            let ring = Arc::clone(&ring);
            handles.push(loom::thread::spawn(move || {
                let mut pushed = 0;
                'next: for v in 1..=MESSAGES {
                    for _ in 0..ATTEMPTS {
                        if ring.try_push(v) {
                            pushed += 1;
                            continue 'next;
                        }
                        // Backpressure: retry within bounds; giving up is a
                        // legal outcome (flow control, not a bug).
                    }
                    break 'next;
                }
                pushed
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
        // The first push always succeeds on a fresh ring regardless of
        // scheduling; FIFO safety above is the property under proof.
        assert!(ring.total_written() >= 1);
    });
}

/// Model 2 — backpressure boundary.
///
/// One producer attempts exactly three pushes (capacity 2); one consumer
/// performs exactly one pop. The assertion: **the third push may only
/// succeed if the consumer advanced its cursor before it** — i.e. the
/// producer never crosses the capacity boundary without an observed
/// reader-side release. This is the invariant that makes
/// "no overwrite before the slowest read" true.
pub fn model_backpressure_boundary() {
    loom::model(move || {
        let ring = Arc::new(LoomRing::with_readers(2));

        let rp = Arc::clone(&ring);
        let producer = loom::thread::spawn(move || {
            let mut v = 1;
            while v <= 2 {
                if rp.try_push(v) {
                    v += 1;
                }
            }
            rp.try_push(3)
        });

        let rc = Arc::clone(&ring);
        let consumer = loom::thread::spawn(move || rc.try_pop(0));

        let third = producer.join().unwrap();
        let popped = consumer.join().unwrap();

        // A single pop of a fresh-cap-2 ring can only see nothing or the
        // first message.
        assert!(popped.is_none() || popped == Some(1));
        // Backpressure honored: push #3 succeeded ⇒ the reader had already
        // published its cursor advance (Release) before the producer's
        // Acquire min-read scan observed it.
        if third {
            assert_eq!(
                popped,
                Some(1),
                "push crossed the boundary without a reader advance"
            );
        }
    });
}
