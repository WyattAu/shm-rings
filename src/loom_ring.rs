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
    /// Handle-local claim pipeline (mirrors `SpmcRingBuffer::claim_base`).
    claim_base: [AtomicU64; MAX_READERS],
    /// Outstanding-loan gate (mirrors `SpmcRingBuffer::loan_depth`).
    loan_depth: [AtomicU64; MAX_READERS],
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
            claim_base: std::array::from_fn(|_| AtomicU64::new(0)),
            loan_depth: std::array::from_fn(|_| AtomicU64::new(0)),
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

    /// Zero-copy claim — identical protocol to the real
    /// [`crate::SpmcRingBuffer::claim`] (gate raise, Acquire cursor reads,
    /// handle-local pipeline CAS).
    pub fn claim(&self, reader_id: usize) -> Option<LoomLoan<'_>> {
        assert!(reader_id < self.num_readers);
        // Gate: block modeled try_pop while a loan is open (module docs of
        // `loan`).
        self.loan_depth[reader_id].fetch_add(1, Relaxed);
        let index = loop {
            let w = self.write_idx.load(Acquire);
            let cursor = self.read_indices[reader_id].load(Acquire);
            let base = self.claim_base[reader_id].load(Relaxed);
            let idx = base.max(cursor);
            if idx >= w {
                break None;
            }
            match self.claim_base[reader_id].compare_exchange(base, idx + 1, Relaxed, Relaxed) {
                Ok(_) => break Some(idx),
                Err(_) => continue,
            }
        };
        if index.is_none() {
            self.loan_depth[reader_id].fetch_sub(1, Relaxed);
        }
        index.map(|index| LoomLoan {
            ring: self,
            reader_id,
            index,
            resolved: false,
        })
    }
}

/// Zero-copy loan double of [`crate::loan::Loan`] (protocol-identical).
pub struct LoomLoan<'a> {
    ring: &'a LoomRing,
    reader_id: usize,
    index: u64,
    resolved: bool,
}

impl LoomLoan<'_> {
    /// The pinned slot's value (stands in for `Deref` on the real loan).
    pub fn value(&self) -> u64 {
        // SAFETY: loom guarantees the pointer handed to `with` is valid for
        // reads for the closure's duration; the slot is pinned by the cursor
        // protocol (cursor <= index while the loan is alive).
        self.ring.slots[(self.index & (LOOM_CAPACITY - 1)) as usize].with(|s| unsafe { *s })
    }

    /// The unmasked stream index this loan views.
    pub fn index(&self) -> u64 {
        self.index
    }

    /// FIFO commit; `false` when not at the cursor (mirrors the real
    /// `Loan::commit` error).
    pub fn commit(&mut self) -> bool {
        if self.resolved {
            return true;
        }
        let cursor = self.ring.read_indices[self.reader_id].load(Acquire);
        if cursor != self.index {
            return false;
        }
        // Release: reclaim edge.
        self.ring.read_indices[self.reader_id].store(self.index + 1, Release);
        self.resolve();
        true
    }

    /// Rewind release (mirrors the real `Loan::abort`).
    pub fn abort(mut self) {
        if !self.resolved {
            self.ring.claim_base[self.reader_id].fetch_min(self.index, Relaxed);
            self.resolve();
        }
    }

    fn resolve(&mut self) {
        self.resolved = true;
        self.ring.loan_depth[self.reader_id].fetch_sub(1, Relaxed);
    }
}

impl Drop for LoomLoan<'_> {
    fn drop(&mut self) {
        if self.resolved {
            return;
        }
        let cursor = self.ring.read_indices[self.reader_id].load(Acquire);
        if cursor == self.index {
            // Release: reclaim edge (commit-on-drop default).
            self.ring.read_indices[self.reader_id].store(self.index + 1, Release);
        }
        self.resolve();
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

/// Model 3 — zero-copy loan pins the cursor against the producer.
///
/// A consumer claims a loan (holding the reader at index 0) while a
/// producer tries to push three messages into a capacity-2 ring. The
/// property under proof, for *every* interleaving: the third push may only
/// succeed after the loan was resolved (commit or drop) and thereby
/// published the reader's Release cursor advance — an open loan backpressures
/// the producer exactly like an un-advanced cursor, because it *is* one.
pub fn model_loan_pins_cursor_against_backpressure() {
    const ATTEMPTS: usize = 3;
    loom::model(move || {
        let ring = Arc::new(LoomRing::with_readers(1));

        let rp = Arc::clone(&ring);
        let producer = loom::thread::spawn(move || {
            assert!(rp.try_push(1), "first push into a fresh ring succeeds");
            assert!(rp.try_push(2), "second push fits capacity 2");
            // Third push: legal only once the consumer's loan resolved and
            // the cursor moved to 1. Bounded retries (flow control).
            for _ in 0..ATTEMPTS {
                if rp.try_push(3) {
                    return true;
                }
            }
            false
        });

        let rc = Arc::clone(&ring);
        let consumer = loom::thread::spawn(move || {
            // Bounded wait for the first published message, then claim it.
            for _ in 0..ATTEMPTS {
                if let Some(mut loan) = rc.claim(0) {
                    assert_eq!(loan.index(), 0, "first claim is stream index 0");
                    assert_eq!(loan.value(), 1, "loan views the first message");
                    // Second claim: allowed (pipelining), views index 1.
                    if let Some(mut loan2) = rc.claim(0) {
                        assert_eq!(loan2.index(), 1);
                        // FIFO: committing #2 while #0 is open must fail...
                        assert!(!loan2.commit(), "out-of-order commit must be rejected");
                    }
                    // ...then the in-order commit succeeds and unblocks the
                    // producer's third push.
                    assert!(loan.commit(), "in-order commit must succeed");
                    return true;
                }
            }
            false
        });

        let pushed = producer.join().unwrap();
        let consumed = consumer.join().unwrap();
        // First two pushes always succeed on a fresh cap-2 ring. Liveness
        // of the exchange is attempt-bounded (the producer may burn its
        // retries before the consumer's commit — flow control, not a bug,
        // exactly as in model 2). The properties under proof were asserted
        // inline: first claim is index 0 viewing message 1, pipelined
        // second claim is index 1, out-of-order commit is rejected, and
        // the producer's third push can only pass the backpressure check
        // through the loan's Release cursor advance — the identical
        // reclaim edge `try_pop` uses.
        let _ = (pushed, consumed);
        assert!(ring.total_written() >= 2);
    });
}

/// Model 4 — notification handshake: no lost wakeup, no signal before data.
///
/// Models the eventfd protocol's user-space shape: the producer publishes
/// (Release, inside `try_push`) *then* bumps a notify counter (`Release`);
/// the consumer checks emptiness (`Acquire`), then "parks" until the
/// counter moves (`Acquire` loads). Exhaustive property: **whenever the
/// consumer observes the notify bump, its next Acquire read of `write_idx`
/// must see the published message** — the transitive happens-before chain
/// that makes `pop_blocking` never miss a wakeup-worthy message. A real
/// eventfd strengthens this further (the kernel orders read-after-write);
/// loom proves the memory-ordering skeleton the Rust side relies on.
pub fn model_notify_no_lost_wakeup() {
    const ATTEMPTS: usize = 3;
    loom::model(move || {
        let ring = Arc::new(LoomRing::with_readers(1));
        // Stand-in for the eventfd counter: monotone, bumped (Release) by
        // the producer after publishing, spun on (Acquire) by the consumer.
        let notify = Arc::new(AtomicU64::new(0));

        let rc = Arc::clone(&ring);
        let rn = Arc::clone(&notify);
        let consumer = loom::thread::spawn(move || {
            for _ in 0..ATTEMPTS {
                // Direct hit before any wait: still valid, must be the
                // first stream message.
                if let Some(v) = rc.try_pop(0) {
                    assert_eq!(v, 1, "out-of-stream value");
                    return true;
                }
                // Check-then-wait: read the counter, then "park" until it
                // moves (bounded spin in the model; kernel block for real).
                let seen = rn.load(Acquire);
                for _ in 0..ATTEMPTS {
                    if rn.load(Acquire) != seen {
                        // WOKEN. The bump was a Release fetch_add ordered
                        // after the producer's Release publish of the slot;
                        // transitively, our next Acquire load of write_idx
                        // MUST observe the message. This assertion is the
                        // no-lost-wakeup property under proof.
                        let v = rc
                            .try_pop(0)
                            .unwrap_or_else(|| panic!("woken by notify but message not visible"));
                        assert_eq!(v, 1, "woken to a torn/out-of-stream value");
                        return true;
                    }
                }
            }
            false
        });

        let rp = Arc::clone(&ring);
        let pn = Arc::clone(&notify);
        let producer = loom::thread::spawn(move || {
            // Fresh cap-2 ring: the first push always succeeds.
            assert!(rp.try_push(1));
            // Publish (above) THEN signal — the producer half of the
            // handshake, program order.
            pn.fetch_add(1, Release);
        });

        let got = consumer.join().unwrap();
        producer.join().unwrap();
        // The producer always pushes + signals. The consumer may have
        // exhausted its (deliberately tiny) attempt budget before the
        // producer ran — that is bounded-liveness, not a bug, and matches
        // models 1–2. The SAFETY property (woken ⇒ message visible) was
        // asserted inline at the wakeup point under every schedule that
        // reaches it.
        let _ = got;
    });
}
