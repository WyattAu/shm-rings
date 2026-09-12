//! Zero-copy loans: [`Loan`] — a pinned view into a ring slot without
//! copying.
//!
//! [`crate::SpmcRingBuffer::try_pop`] always copies `size_of::<T>()` bytes
//! out of the mapping. For large fixed-layout records that copy is the hot
//! path. The loan API replaces it with a borrow:
//!
//! ```no_run
//! use shm_rings::SpmcRingBuffer;
//!
//! # fn main() -> Result<(), shm_rings::ShmRingError> {
//! # let mut ring = SpmcRingBuffer::<u64>::create_new("/dev/shm/demo.ring", 1024)?;
//! # ring.try_push(&42);
//! let reader = SpmcRingBuffer::<u64>::open_existing("/dev/shm/demo.ring")?;
//!
//! // 1. Claim the next message (zero-copy reservation).
//! if let Some(mut loan) = reader.claim(0)? {
//!     // 2. Read the mapped bytes in place — nothing was copied.
//!     if *loan == 42 {
//!         // 3a. Commit: advance this reader's cursor past the slot.
//!         loan.commit()?;
//!     }
//!     // 3b. Dropping without commit() also commits (RAII default);
//!     // use `abort` instead to release the claim without consuming.
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Semantics
//!
//! * `claim(reader_id)` reserves the next stream index for that reader
//!   (`max(handle pipeline head, shared cursor)`) and returns a [`Loan`]
//!   viewing the slot, or `None` when no message is published at that index.
//!   Up to `capacity` loans may be outstanding at once (producer
//!   backpressure bounds the pipeline naturally).
//! * [`Loan::commit`] advances the reader's shared cursor to `index + 1`
//!   with a `Release` store — the exact reclaim edge of `try_pop`. Commits
//!   are **strictly FIFO**: a loan whose index is not the reader's cursor
//!   fails with [`ShmRingError::LoanNotAtCursor`] (a sequencing error —
//!   nothing is written, the loan stays alive and keeps pinning its slot).
//! * Dropping a loan commits it if it is at the cursor (the RAII default);
//!   if the pipeline was rewound under it, the drop is a no-op. A drop can
//!   therefore never corrupt the pipeline.
//! * [`Loan::abort`] releases the claim **without consuming**: the claim
//!   pipeline rewinds to the aborted index, the message becomes claimable
//!   again, and any loans claimed *after* it simply re-derive their
//!   positions once the cursor climbs back (their slots stay pinned the
//!   whole time; a later commit re-attempt fails with
//!   [`ShmRingError::LoanNotAtCursor`] until the cursor reaches them again).
//!
//! # Why this is safe (the two-line argument)
//!
//! 1. The producer may overwrite slot `i` only when *every* reader cursor
//!    is past `i` (the crate's core backpressure invariant).
//! 2. While a loan at index `i` is alive, its reader's cursor stays ≤ `i`:
//!    commits require `cursor == index` (FIFO), `try_pop` for that reader is
//!    rejected with [`ShmRingError::LoanOutstanding`] while a loan is open,
//!    and nothing else moves the cursor.
//!
//! Therefore the bytes the loan views cannot be concurrently written, and
//! the `&T` dereference is a plain, race-free read of pinned shared memory.
//!
//! # Contract: one logical consumer per `reader_id`
//!
//! Loans tighten the existing reader convention: while a loan is open on
//! reader `r`, *no other handle* may pop, claim, commit, or in any way
//! advance reader `r`'s cursor. Cursor advance past a live loan's index
//! un-pins the slot and dereferencing the loan afterwards is undefined
//! behavior — the same class of misuse as caching a [`crate::SpmcRingBuffer::peek`]
//! pointer across pops, but now lifetime-bound and blocked on the common
//! (same-handle) path. The per-handle guard is best-effort across threads:
//! racing `try_pop` against `claim` on the *same* `reader_id` is outside the
//! contract, exactly as it is today for `try_pop` itself.
//!
//! # ABI constraints on `T`
//!
//! A loaned record is read *in place* by another process, so `T` must be a
//! fixed-layout record: `Copy + zerocopy::FromBytes + zerocopy::Immutable`
//! (already required by the ring) plus the operator contract that both
//! sides compile the *same type* — same size, same alignment (≤ 64), same
//! field offsets. In practice: `#[repr(C)]` POD, no pointers, no `Drop`, no
//! padding-dependent semantics, no versioned/variable-length payloads.
//! Embed an explicit version/sequence field in the record if the schema can
//! evolve.

use std::fmt;
use std::mem::size_of;
use std::ops::Deref;
use std::slice;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

use zerocopy::{FromBytes, Immutable};

use crate::error::ShmRingError;
use crate::ring::SpmcRingBuffer;

/// A zero-copy view of one ring slot, pinned against producer overwrites
/// until it is resolved.
///
/// Created by [`SpmcRingBuffer::claim`]; resolved by [`Loan::commit`],
/// [`Loan::abort`], or `drop` (commit-at-cursor / no-op, never corruption).
///
/// See the [module documentation](self) for the semantics, the safety
/// argument, and the ABI constraints on `T`.
pub struct Loan<'a, T: Copy + FromBytes + Immutable> {
    ring: &'a SpmcRingBuffer<T>,
    reader_id: usize,
    /// The unmasked stream index this loan views (`cursor ≤ index` is the
    /// pinning invariant that keeps the slot stable).
    index: u64,
    /// Pointer to the slot inside the mapping (`index` masked). Derived from
    /// the ring's mapping, which outlives `&'a Self`.
    ptr: *const T,
    /// `true` once this loan has been resolved (committed or aborted), so
    /// `Drop` performs no further shared-state updates.
    resolved: bool,
}

// SAFETY: Loan is Send/Sync because every field is: `&SpmcRingBuffer<T>` is
// Send + Sync (impls on the ring), and the raw `ptr` targets mapped shared
// memory whose byte range is pinned against concurrent writes for the loan's
// lifetime by the cursor protocol (module docs) — a property that holds no
// matter which thread dereferences or resolves the loan. `index`/`resolved`
// are plain immutable-after-construction / owner-visible flags; resolution
// goes through the ring's atomics, which are cross-thread sound.
unsafe impl<T: Copy + FromBytes + Immutable> Send for Loan<'_, T> {}
// SAFETY: see the `Send` impl; all shared access is atomics plus reads of
// pinned, concurrently-unwritten mapped bytes of an `Immutable` type.
unsafe impl<T: Copy + FromBytes + Immutable> Sync for Loan<'_, T> {}

impl<T: Copy + FromBytes + Immutable> SpmcRingBuffer<T> {
    /// Reserves this reader's next message as a zero-copy [`Loan`].
    ///
    /// Returns `Ok(None)` when no message is published at the claim position
    /// (the reader is caught up). The claim does **not** advance the shared
    /// cursor — it reserves a position in the handle-local pipeline; the
    /// cursor moves only on [`Loan::commit`] (or commit-on-drop).
    ///
    /// Up to `capacity` loans may be outstanding; beyond that the producer
    /// is backpressured and further claims return `Ok(None)` once the
    /// published stream is exhausted.
    ///
    /// While a loan is outstanding for `reader_id`, [`Self::try_pop`] fails
    /// with [`ShmRingError::LoanOutstanding`] (popping would un-pin the
    /// loaned slot).
    pub fn claim(&self, reader_id: usize) -> Result<Option<Loan<'_, T>>, ShmRingError> {
        self.check_reader(reader_id)?;
        let hdr = self.hdr();
        // Raise the loan gate BEFORE touching the cursor: from here until
        // resolution, try_pop for this reader is rejected, so the cursor
        // cannot advance past the slot this loan will view (same-handle
        // protection; cross-handle is an operator contract, module docs).
        self.loan_depth[reader_id].fetch_add(1, Relaxed);
        let loan = loop {
            // Acquire: publish edge — if the index is below `w`, its slot
            // bytes are visible to this thread's subsequent reads.
            let w = hdr.write_idx(Acquire);
            // Acquire on our own cursor (uniform with try_pop; we are its
            // only logical writer under the reader contract).
            let cursor = hdr.read_idx(reader_id, Acquire);
            let base = self.claim_base[reader_id].load(Relaxed);
            // Realign the handle-local pipeline when it lagged the cursor
            // (external pops) or rewound (abort).
            let idx = base.max(cursor);
            if idx >= w {
                // Caught up: nothing published at the claim position.
                break None;
            }
            match self.claim_base[reader_id].compare_exchange_weak(base, idx + 1, Relaxed, Relaxed)
            {
                Ok(_) => {
                    // SAFETY: `idx` is in-bounds and the pointer aligned
                    // (see `slot_ptr`); the backpressure protocol pins slot
                    // `idx` because every reader cursor — including this
                    // one, gate-raised above — is ≤ `idx` (the slot's
                    // content was published before `w`, which we Acquire'd,
                    // and no producer write can reach it until all cursors
                    // pass `idx`). No dereference happens here; the raw
                    // pointer is only stored.
                    let ptr = self.slot_ptr(idx).cast_const();
                    break Some(Loan {
                        ring: self,
                        reader_id,
                        index: idx,
                        ptr,
                        resolved: false,
                    });
                }
                // Another thread on this handle claimed concurrently: retry
                // with fresh state.
                Err(_) => continue,
            }
        };
        if loan.is_none() {
            // Failed claim: release the gate immediately.
            self.loan_depth[reader_id].fetch_sub(1, Relaxed);
        }
        Ok(loan)
    }
}

impl<T: Copy + FromBytes + Immutable> Loan<'_, T> {
    /// The unmasked stream index this loan views.
    pub fn index(&self) -> u64 {
        self.index
    }

    /// The reader id this loan was claimed for.
    pub fn reader_id(&self) -> usize {
        self.reader_id
    }

    /// The loaned record as raw mapped bytes (`size_of::<T>()` long).
    pub fn as_bytes(&self) -> &[u8] {
        // SAFETY: `self.ptr` is in-bounds and aligned for `T` (claim-time
        // invariant), the byte range `size_of::<T>()` long is pinned against
        // concurrent writes for the loan's lifetime (module docs), and a
        // `u8` slice imposes no alignment or validity requirements beyond
        // initialized memory — mapped file-backed pages are initialized.
        unsafe { slice::from_raw_parts(self.ptr.cast::<u8>(), size_of::<T>()) }
    }

    /// Advances the reader's cursor past this slot (`Release` store — the
    /// reclaim edge), consuming the loan.
    ///
    /// Fails with [`ShmRingError::LoanNotAtCursor`] when this loan is not at
    /// the cursor: commits are strictly FIFO, so resolve earlier loans (or
    /// [`abort`](Self::abort) to rewind the pipeline) first. On error the
    /// loan is **not** consumed and keeps pinning its slot.
    ///
    /// Committing twice (or after an abort path rewound the pipeline) is a
    /// no-op returning `Ok(())` — idempotent by design so `commit()?` before
    /// `drop` is always safe to write.
    pub fn commit(&mut self) -> Result<(), ShmRingError> {
        if self.resolved {
            return Ok(());
        }
        let hdr = self.ring.hdr();
        // Acquire: see the freshest cursor (also ours to advance under the
        // reader contract).
        let cursor = hdr.read_idx(self.reader_id, Acquire);
        if cursor != self.index {
            return Err(ShmRingError::LoanNotAtCursor {
                index: self.index,
                cursor,
            });
        }
        // Release: reclaim edge — publishes "this reader is done with index
        // `self.index`" to the producer's Acquire min-read scan, exactly as
        // try_pop's cursor bump does. Ordered before the loan-gate decrement
        // below.
        hdr.set_read_idx(self.reader_id, self.index + 1, Release);
        self.resolve();
        Ok(())
    }

    /// Releases the claim without consuming, rewinding the claim pipeline to
    /// this loan's index: the message becomes claimable again.
    ///
    /// Loans claimed after this one stay alive and their slots stay pinned,
    /// but their commits fail with [`ShmRingError::LoanNotAtCursor`] until
    /// the cursor climbs back over their indices (re-claim + commit in
    /// order). This is the "skip nothing" release — use it when a consumer
    /// inspected a record and decided to leave it for a later pass.
    pub fn abort(mut self) {
        if !self.resolved {
            // Handle-local rewind: the next claim realigns with the cursor
            // and re-issues this index. Relaxed: no shared invariant depends
            // on the order of this store (the shared cursor never moved).
            self.ring.claim_base[self.reader_id].fetch_min(self.index, Relaxed);
            self.resolve();
        }
        // `Drop` sees `resolved` and performs no further updates.
    }

    /// Marks the loan resolved and releases the loan gate. Call exactly once
    /// per loan, after any shared-state update it implies.
    fn resolve(&mut self) {
        self.resolved = true;
        self.ring.loan_depth[self.reader_id].fetch_sub(1, Relaxed);
    }
}

impl<T: Copy + FromBytes + Immutable> Deref for Loan<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: `self.ptr` is in-bounds and aligned (claim-time
        // invariant); `T: FromBytes` makes any bit pattern in the pinned
        // slot a valid `T`; `T: Immutable` rules out interior mutability;
        // and the cursor protocol guarantees no concurrent write to this
        // slot while the loan is alive (`cursor <= index` pinning, module
        // docs) — so a shared reference to the mapped bytes is race-free.
        unsafe { &*self.ptr }
    }
}

impl<T: Copy + FromBytes + Immutable> Drop for Loan<'_, T> {
    fn drop(&mut self) {
        if self.resolved {
            return;
        }
        let hdr = self.ring.hdr();
        // RAII default (commit-on-drop): if this loan is at the cursor,
        // dropping it commits — mirroring try_pop's protocol exactly
        // (Acquire load, reads already done through `Deref` in program
        // order, Release cursor bump).
        let cursor = hdr.read_idx(self.reader_id, Acquire);
        if cursor == self.index {
            // Release: reclaim edge (see `commit`).
            hdr.set_read_idx(self.reader_id, self.index + 1, Release);
        }
        // Otherwise the pipeline was rewound under this loan (abort) or the
        // cursor moved past it through an out-of-contract path: no-op — a
        // drop can never corrupt the pipeline.
        self.resolve();
    }
}

impl<T: Copy + FromBytes + Immutable + fmt::Debug> fmt::Debug for Loan<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Loan")
            .field("reader_id", &self.reader_id)
            .field("index", &self.index)
            .field("record", &**self)
            .finish()
    }
}
