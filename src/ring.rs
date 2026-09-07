//! The ring itself: [`SpmcRingBuffer`].
//!
//! # Memory ordering — the complete argument
//!
//! This crate contains exactly two synchronization protocols and zero
//! `SeqCst` operations. Everything is acquire/release on two families of
//! index atomics; slot payloads move with plain volatile (non-atomic) loads
//! and stores. The protocols:
//!
//! **Publish (producer, [`SpmcRingBuffer::try_push`]):**
//!
//! ```text
//! 1. write slot bytes        volatile store, plain program order
//! 2. write_idx += 1          Release store
//! ```
//!
//! **Observe (consumer, [`SpmcRingBuffer::try_pop`]):**
//!
//! ```text
//! 3. read write_idx          Acquire load
//! 4. read slot bytes         volatile load
//! 5. read_idx += 1           Release store (to the reader's own cursor)
//! ```
//!
//! Steps 1–4 are the textbook release/acquire message-passing pattern
//! (C++ §32.4, "promotion" of release-consume to release-acquire): the
//! `Release` store in step 2 guarantees that *every* byte written by the
//! program before it — including the volatile slot write in step 1 — is
//! visible to any thread whose `Acquire` load of `write_idx` (step 3) reads
//! the stored value or anything ordered after it. The consumer therefore
//! never observes a partially written or stale slot for an index it has
//! legitimately acquired.
//!
//! **Reclaim protection (the other half):** the producer computes
//! `min(read_indices)` with `Acquire` loads (step 6) and refuses to push
//! when `write_idx - min >= capacity`. The reader's cursor bump in step 5 is
//! a `Release` store, so the pair (5, 6) is a second message-passing edge in
//! the opposite direction: when the producer *sees* a cursor advance, every
//! volatile slot read that reader performed *before* the bump is ordered
//! before the producer's subsequent slot write. Concretely: the producer can
//! never overwrite the slot under a reader's cursor until that reader has
//! published (via its own `Release` bump) that it is done reading it. This
//! is the core safety property: **no overwrite before the slowest read**.
//!
//! **Why no `SeqCst` is needed:** `SeqCst` is required when multiple
//! *independently ordered* atomics must agree on one global interleaving
//! (e.g. Dekker-style store/load flags). Here there is only *one* producer
//! (its `write_idx` is `Relaxed`-loaded/mutated by itself alone and
//! `Release`-published) and each reader owns exactly one cursor (again
//! written only by its owner). Every cross-thread happens-before edge that
//! matters is a single store→load pair on a single atomic, which
//! acquire/release fully orders. Independent per-reader cursors create no
//! composite invariant requiring a global order.
//!
//! **Dat3 (different atomics, different orderings) safety:** mixed ordering
//! across *different* atomic variables is a classic hazard only when the
//! algorithm derives a guarantee from the *relative* order of two unrelated
//! atomics. That never happens here: index atomics are the only
//! synchronization points, and slot data is *never* accessed atomically —
//! its visibility is always borrowed from the index protocol above. The
//! `Relaxed` uses (producer-side `write_idx` read, `total_written` counter)
//! are confined to values with a single writer whose ordering is irrelevant
//! to correctness: `write_idx` is read `Relaxed` only by its sole writer (to
//! read its own last store, which program order already guarantees), and
//! `total_written` is a monotonically growing diagnostic.
//!
//! # Single-producer contract
//!
//! Exactly one thread (in one process) may push at a time. This is enforced
//! in Rust by `try_push(&mut self, ...)` — the borrow checker makes a second
//! concurrent producer impossible for handles within one process, and
//! operators must ensure one producer per file across processes. Readers are
//! the opposite: [`SpmcRingBuffer::try_pop`] takes `&self`, so any number of
//! reader handles — shared across threads or processes — consume
//! concurrently and independently.
//!
//! # Cross-process notes
//!
//! All state lives in the shared mapping; every handle derived from
//! `open_existing` cooperates through the same atomics. On cache-coherent
//! architectures (x86-64, aarch64, ...) atomics and volatile accesses are
//! coherent across processes mapping the same file, which is precisely the
//! deployment this crate targets.

use std::fmt;
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

use memmap2::MmapMut;
use zerocopy::{FromBytes, Immutable};

use crate::error::ShmRingError;
use crate::header::{RingHeader, HEADER_SIZE, MAX_READERS};

/// Byte offset where slot data begins (immediately after the header).
const DATA_OFFSET: usize = HEADER_SIZE;

/// A fixed-capacity, single-producer / multi-consumer, lock-free ring buffer
/// backed by a shared-memory-mapped file.
///
/// See the [module documentation](self) for the memory-ordering argument and
/// the single-producer contract.
///
/// # Type parameter bounds and why they exist
///
/// * `T: Copy` — slots are overwritten in place by volatile writes. A type
///   with a `Drop` impl would leak/double-run destructors when the producer
///   laps a slot; `Copy` (which forbids `Drop`) rules that out, and makes the
///   bitwise `write_volatile`/`read_volatile` moves well-defined.
/// * `T: FromBytes` — the producer writes raw bytes into slots that may
///   previously have contained *any* bit pattern (fresh zeroed pages or a
///   lapped old message). `FromBytes` is zerocopy's machine-checked proof
///   that every bit pattern is a valid `T`, so reading a slot can never
///   produce an invalid value (no UB via invalid enum discriminants, null
///   `NonNull`s, etc.).
/// * `T: Immutable` — the mapping is read through `&self` handles by
///   consumers, yet written by the producer. `Immutable` proves `T` contains
///   no `UnsafeCell`/interior mutability, so the volatile write through a
///   pointer derived from a shared mapping cannot create a `&T` to a cell
///   that is being mutated — without it, the read side would be UB.
///
/// Additionally, `align_of::<T>()` must not exceed 64 (the header/first-slot
/// alignment guarantee); both constructors panic if it is, since it is a
/// static property of `T`, not a runtime condition.
pub struct SpmcRingBuffer<T: Copy + FromBytes + Immutable> {
    /// The mapped ring file. Owns the address range that every raw pointer
    /// below points into. The mapping's address is fixed by the OS for as
    /// long as this value lives, and does not change when the struct is
    /// moved (only the handle moves; the mapping does not).
    mmap: MmapMut,
    /// Self-referential pointer to the header at the base of `mmap`.
    ///
    /// # Self-referential layout (deliberate, and sound)
    ///
    /// `header` is `mmap.as_ptr()` cast to `*mut RingHeader`. This is not a
    /// fragile "pointer into a struct that may move" pattern: the pointer
    /// targets the *mapping*, not the struct. `MmapMut` is a handle (an
    /// address + length obtained from `mmap(2)`); moving `SpmcRingBuffer`
    /// moves the handle, and the mapped pages stay at the same virtual
    /// address until `MmapMut` is dropped, at which point the mapping is
    /// unmapped and this pointer must never be used again (it isn't — no
    /// method runs after `Drop` starts).
    header: *mut RingHeader,
    /// Number of slots; always a power of two (validated by the
    /// constructors).
    capacity: usize,
    /// Path the ring was created from or opened at.
    path: PathBuf,
    /// Pins the `T` parameter (the mapping is untyped bytes).
    _marker: PhantomData<T>,
}

// SAFETY: SpmcRingBuffer is Send because it owns its entire state: the mmap
// owns the shared pages, `header` points only into that mapping, and every
// piece of cross-thread state (`write_idx`, `read_indices`, `total_written`)
// is an atomic. `capacity`/`path` are immutable after construction. There is
// no `Rc`/raw non-atomic shared mutable state, so sending the handle to
// another thread transfers ownership soundly.
unsafe impl<T: Copy + FromBytes + Immutable> Send for SpmcRingBuffer<T> {}

// SAFETY: SpmcRingBuffer is Sync because every access to shared state from a
// `&self` handle is either (a) an atomic read/write on the header (acquire/
// release protocols documented in the module docs — no data race), (b) a
// volatile access to slot bytes whose visibility is ordered by those same
// index atomics (so no torn/stale reads beyond the documented semantics), or
// (c) an immutable field read (`capacity`, `path`). `T: Immutable` rules out
// interior mutability in payloads, making the producer's volatile writes
// through the shared mapping race-free-by-ordering rather than UB.
unsafe impl<T: Copy + FromBytes + Immutable> Sync for SpmcRingBuffer<T> {}

impl<T: Copy + FromBytes + Immutable> SpmcRingBuffer<T> {
    /// Creates a new ring file at `path` with `capacity` slots.
    ///
    /// Fails with [`ShmRingError::InvalidCapacity`] unless `capacity` is a
    /// non-zero power of two, and with [`ShmRingError::AlreadyExists`] if
    /// `path` exists. Creation is exclusive by design: re-initializing an
    /// existing ring file (the `File::create` truncation footgun) would
    /// silently corrupt every process still mapped to it. Use
    /// [`Self::open_existing`] for existing rings.
    ///
    /// Provisions all [`MAX_READERS`] reader cursors as participants; see
    /// [`Self::create_new_with_readers`] to provision fewer.
    ///
    /// # Panics
    ///
    /// Panics if `align_of::<T>() > 64` (static property of `T`).
    pub fn create_new(path: impl AsRef<Path>, capacity: usize) -> Result<Self, ShmRingError> {
        Self::create_new_with_readers(path, capacity, MAX_READERS)
    }

    /// [`Self::create_new`], but with only the first `num_readers` reader
    /// cursors participating.
    ///
    /// Backpressure is computed against the slowest *participant*; cursors
    /// beyond `num_readers` exist in the file but never gate the producer,
    /// and consumer calls with `reader_id >= num_readers` fail with
    /// [`ShmRingError::InvalidReaderId`]. Provision exactly the readers you
    /// will run: `create_new` defaults to all [`MAX_READERS`].
    ///
    /// # Panics
    ///
    /// Panics if `num_readers` is zero or exceeds [`MAX_READERS`], or if
    /// `align_of::<T>() > 64`.
    pub fn create_new_with_readers(
        path: impl AsRef<Path>,
        capacity: usize,
        num_readers: usize,
    ) -> Result<Self, ShmRingError> {
        if capacity == 0 || !capacity.is_power_of_two() {
            return Err(ShmRingError::InvalidCapacity { got: capacity });
        }
        assert!(
            (1..=MAX_READERS).contains(&num_readers),
            "num_readers must be in 1..={MAX_READERS}"
        );
        assert!(
            align_of::<T>() <= 64,
            "align_of::<T>() must be <= 64 (the header/slot alignment guarantee)"
        );
        let path = path.as_ref().to_path_buf();
        if path.exists() {
            return Err(ShmRingError::AlreadyExists);
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::AlreadyExists => ShmRingError::AlreadyExists,
                _ => ShmRingError::Io(e),
            })?;
        let len = (HEADER_SIZE + capacity * size_of::<T>()) as u64;
        file.set_len(len)?;

        // SAFETY: `file` is a valid opened file handle of length `len` >= 1;
        // memmap2 maps it MAP_SHARED for mutation. No aliasing mapping of
        // this brand-new file exists yet.
        let mut mmap = unsafe { MmapMut::map_mut(&file) }?;
        drop(file);

        // SAFETY: `mmap` covers `len` >= HEADER_SIZE == size_of::<RingHeader>()
        // bytes, is page-aligned (and 64 >= page alignment), and the header is
        // fully initialized before any other thread or process can map this
        // brand-new file. The plain `ptr::write` is therefore safe: there is no
        // concurrent observer yet; later openers synchronize via the Acquire
        // loads in `RingHeader::validate`.
        let header: *mut RingHeader = mmap.as_mut_ptr().cast();
        // SAFETY: target is within the mapping (invariant above) and
        // uninitialized-but-allocated; `ptr::write` overwrites it wholesale
        // without reading.
        unsafe { header.write(RingHeader::new((capacity - 1) as u64, num_readers)) };

        Ok(Self {
            mmap,
            header,
            capacity,
            path,
            _marker: PhantomData,
        })
    }

    /// Opens an existing ring file for use.
    ///
    /// Validates the header (`magic`, `version`, mask structure — all with
    /// `Acquire` loads) and checks the file is large enough for the header
    /// plus the `capacity * size_of::<T>()` slots the header's mask implies.
    ///
    /// # Errors
    ///
    /// * [`ShmRingError::FileTooShort`] — file smaller than the layout needs.
    /// * [`ShmRingError::HeaderCorruption`] — bad magic or impossible mask.
    /// * [`ShmRingError::VersionMismatch`] — file written by another version.
    ///
    /// # Panics
    ///
    /// Panics if `align_of::<T>() > 64`.
    pub fn open_existing(path: impl AsRef<Path>) -> Result<Self, ShmRingError> {
        assert!(
            align_of::<T>() <= 64,
            "align_of::<T>() must be <= 64 (the header/slot alignment guarantee)"
        );
        let path = path.as_ref().to_path_buf();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)?;

        // Header-only size floor before mapping: mapping a smaller file and
        // reading the header would itself be out-of-bounds.
        let file_len = file.metadata()?.len();
        if file_len < HEADER_SIZE as u64 {
            return Err(ShmRingError::FileTooShort {
                expected: HEADER_SIZE as u64,
                got: file_len,
            });
        }

        // SAFETY: `file` is a valid opened file handle (length already
        // checked >= HEADER_SIZE); memmap2 maps it MAP_SHARED read/write.
        let mmap = unsafe { MmapMut::map_mut(&file) }?;
        drop(file);

        // SAFETY: mapping covers file_len >= HEADER_SIZE bytes, page-aligned;
        // header fields are all atomics, so creating a shared reference to it
        // (aliasable, immutable, no interior non-atomic mutability) is sound.
        let header: *const RingHeader = mmap.as_ptr().cast();
        // SAFETY: pointer valid for reads per the invariant above.
        let hdr: &RingHeader = unsafe { &*header };
        hdr.validate()?;

        let mask = hdr.capacity_mask(Acquire);
        let capacity = (mask + 1) as usize;
        let expected = (HEADER_SIZE + capacity * size_of::<T>()) as u64;
        if file_len < expected {
            return Err(ShmRingError::FileTooShort {
                expected,
                got: file_len,
            });
        }

        Ok(Self {
            mmap,
            // SAFETY: same allocation as `header` above; mutability is fine —
            // writes go only through the atomics' interior mutability.
            header: header.cast_mut(),
            capacity,
            path,
            _marker: PhantomData,
        })
    }

    /// Shared view of the header.
    fn hdr(&self) -> &RingHeader {
        // SAFETY: `self.header` points at the base of `self.mmap`, which is
        // alive and mapped for `'_` of the returned reference; the header
        // region is fully initialized (constructor) and contains only
        // atomics, so a shared reference cannot be invalidated by concurrent
        // writers.
        unsafe { &*self.header }
    }

    /// Pointer to slot `idx` (unmasked `u64` index; masked here).
    fn slot_ptr(&self, idx: u64) -> *mut T {
        let slot = (idx & (self.capacity as u64 - 1)) as usize;
        let offset = DATA_OFFSET + slot * size_of::<T>();
        // SAFETY: `self.mmap.as_ptr()` is the mapping base; the mapping is
        // `HEADER_SIZE + capacity * size_of::<T>()` bytes long and
        // `offset + size_of::<T>()` == that length exactly at `slot ==
        // capacity - 1`, so the byte range is in-bounds. Alignment holds:
        // DATA_OFFSET (256) and the slot stride are multiples of
        // `align_of::<T>()` (<= 64, checked by the constructors), and the
        // base is page-aligned.
        unsafe { self.mmap.as_ptr().cast_mut().byte_add(offset).cast::<T>() }
    }

    /// Publishes one message.
    ///
    /// Returns `false` — without writing anything — when the slowest reader
    /// lags this push by the full capacity (backpressure). This is the only
    /// flow-control mechanism; there is no overwrite mode.
    ///
    /// Ordering protocol: volatile slot write, then `Release` store of
    /// `write_idx + 1` (see the module docs).
    pub fn try_push(&mut self, value: &T) -> bool {
        let hdr = self.hdr();
        // Relaxed: only this thread writes write_idx; program order lets us
        // read our own last store.
        let w = hdr.write_idx(Relaxed);
        // Acquire: synchronizes with each reader's Release cursor-bump, so a
        // cursor we observe as advanced implies that reader finished its
        // volatile reads of the slots behind the cursor. Only participating
        // readers (0..num_readers) count; unused slots never block.
        let num_readers = hdr.num_readers(Acquire);
        let min_read = hdr.slowest_read_idx(num_readers, Acquire);
        if w - min_read >= self.capacity as u64 {
            return false;
        }

        let slot = self.slot_ptr(w);
        // SAFETY: `slot` is in-bounds and aligned (see `slot_ptr`); writing
        // `*value` (a `Copy` type) copies exactly `size_of::<T>()` bytes.
        // This happens BEFORE the Release store below, publishing the bytes
        // to any thread that acquires the new write_idx.
        unsafe { std::ptr::write_volatile(slot, *value) };

        // Release: publishes the slot bytes (message passing, module docs).
        hdr.set_write_idx(w + 1, Release);
        // Relaxed diagnostic counter; no invariant depends on it.
        hdr.bump_total_written(Relaxed);
        true
    }

    /// Consumes the next message for reader `reader_id`.
    ///
    /// Each of the [`MAX_READERS`] readers independently walks the full
    /// message stream (broadcast semantics). Returns `Ok(None)` when the
    /// reader has caught up with the producer.
    ///
    /// Ordering protocol: `Acquire` load of `write_idx`, volatile slot read,
    /// `Release` store of the reader's own cursor (see the module docs).
    pub fn try_pop(&self, reader_id: usize) -> Result<Option<T>, ShmRingError> {
        self.check_reader(reader_id)?;
        let hdr = self.hdr();
        // Acquire: synchronizes with the producer's Release publish; the
        // slot bytes for every index < the loaded value are visible below.
        let w = hdr.write_idx(Acquire);
        // Acquire on our own cursor: harmless (we are its only writer) but
        // keeps the index protocol uniformly documented.
        let r = hdr.read_idx(reader_id, Acquire);
        if r == w {
            return Ok(None);
        }

        let slot = self.slot_ptr(r);
        // SAFETY: `slot` is in-bounds and aligned (see `slot_ptr`). The
        // backpressure protocol guarantees the producer cannot have
        // overwritten the slot under OUR cursor: overwriting slot `r`
        // requires ALL cursors (including ours, still at `r`) to be past it.
        // The producer's Acquire min-read load saw `r` (or less) when it
        // pushed anything up to `w`, so this volatile read races with no
        // write.
        let value: T = unsafe { std::ptr::read_volatile(slot) };

        // Release: publishes "this reader is done with index r" to the
        // producer's Acquire min-read scan (reclaim edge, module docs).
        hdr.set_read_idx(reader_id, r + 1, Release);
        Ok(Some(value))
    }

    /// Non-consuming pointer to this reader's next message, if any.
    ///
    /// # Validity window
    ///
    /// The pointer is valid to *read* (as `&*ptr`) from immediately after
    /// this call until this reader's next [`Self::try_pop`] or
    /// [`Self::peek`]. The producer may overwrite the slot only after *all*
    /// readers advance past it, so the window for a slow reader is generous
    /// — but do not cache the pointer across pops. Intended for reading
    /// several fields of one message before deciding to advance; consumers
    /// that want an owned copy should just use [`Self::try_pop`].
    pub fn peek(&self, reader_id: usize) -> Result<Option<*const T>, ShmRingError> {
        self.check_reader(reader_id)?;
        let hdr = self.hdr();
        let w = hdr.write_idx(Acquire);
        let r = hdr.read_idx(reader_id, Acquire);
        if r == w {
            return Ok(None);
        }
        // SAFETY: same in-bounds/alignment/no-clobber argument as try_pop;
        // the bytes stay valid for this reader until it advances its cursor.
        // No dereference happens here — the raw pointer is the return value.
        Ok(Some(self.slot_ptr(r) as *const T))
    }

    /// Number of reader cursors (always [`MAX_READERS`] in v1).
    pub fn reader_count(&self) -> usize {
        self.hdr().num_readers(Relaxed)
    }

    /// Messages available to `reader_id` right now (`write_idx - read_idx`).
    pub fn len(&self, reader_id: usize) -> Result<u64, ShmRingError> {
        self.check_reader(reader_id)?;
        let hdr = self.hdr();
        let w = hdr.write_idx(Acquire);
        let r = hdr.read_idx(reader_id, Acquire);
        // Counters are monotone and r <= w is an invariant of the protocol
        // (pop advances r only up to w); wrapping_sub keeps release-mode
        // builds free of an overflow panic even on absurd/corrupt files.
        Ok(w.wrapping_sub(r))
    }

    /// `true` when `reader_id` has consumed every published message.
    pub fn is_empty(&self, reader_id: usize) -> Result<bool, ShmRingError> {
        Ok(self.len(reader_id)? == 0)
    }

    /// Total successful pushes since creation (diagnostic).
    pub fn total_written(&self) -> u64 {
        self.hdr().total_written(Acquire)
    }

    /// Lag of the slowest reader behind the producer (`write_idx -
    /// min(read_indices)`); the value backpressure is computed against.
    pub fn slowest_lag(&self) -> u64 {
        let hdr = self.hdr();
        let num_readers = hdr.num_readers(Acquire);
        hdr.write_idx(Acquire)
            .wrapping_sub(hdr.slowest_read_idx(num_readers, Acquire))
    }

    /// Number of slots (power of two).
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Path of the backing file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn check_reader(&self, reader_id: usize) -> Result<(), ShmRingError> {
        let num_readers = self.hdr().num_readers(Acquire);
        if reader_id >= num_readers {
            return Err(ShmRingError::InvalidReaderId {
                id: reader_id,
                max: num_readers - 1,
            });
        }
        Ok(())
    }
}

impl<T: Copy + FromBytes + Immutable> fmt::Debug for SpmcRingBuffer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpmcRingBuffer")
            .field("path", &self.path)
            .field("capacity", &self.capacity)
            .field("write_idx", &self.hdr().write_idx(Acquire))
            .field("total_written", &self.hdr().total_written(Acquire))
            .finish()
    }
}
