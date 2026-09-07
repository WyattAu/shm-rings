//! On-disk ring header: layout, initialization, and validation.
//!
//! The header occupies exactly [`HEADER_SIZE`] (256) bytes at offset 0 of the
//! ring file — four full cache lines, so no header word ever shares a cache
//! line with slot data. All multi-byte fields are atomics because the header
//! is shared, unlocked, between the producer and up to [`MAX_READERS`]
//! consumers, which may live in the same process or in distinct processes
//! mapping the same file.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crate::error::ShmRingError;

/// Magic marker stored at offset 0: `"RINGSPMC"` as big-endian ASCII.
///
/// (`0x5249` = "RI", `0x4E47` = "NG", `0x5350` = "SP", `0x4D43` = "MC".)
pub const MAGIC: u64 = 0x5249_4E47_5350_4D43;

/// On-disk format version. Clean-break v1: no byte compatibility with any
/// prior format is attempted or supported. Bump on any layout change.
pub const VERSION: u32 = 1;

/// Fixed maximum number of independent reader cursors.
///
/// All [`MAX_READERS`] cursors exist from `create_new` and are initialized to
/// zero. Every cursor participates in backpressure: a reader that never pops
/// pins the ring (the producer stalls once the lag against that cursor
/// reaches the capacity).
pub const MAX_READERS: usize = 8;

/// Size of the serialized header in bytes: four 64-byte cache lines.
///
/// Slot data starts at this offset, guaranteeing the header and the first
/// slot never share a cache line (no false sharing between index traffic and
/// payload traffic).
pub const HEADER_SIZE: usize = 256;

/// The shared control block at the start of every ring file.
///
/// Layout (repr(C), 64-byte aligned, 256 bytes total):
///
/// | offset | field           | meaning                                     |
/// |--------|-----------------|---------------------------------------------|
/// | 0      | `magic`         | [`MAGIC`] identity marker                    |
/// | 8      | `version`       | [`VERSION`]                                  |
/// | 12     | `num_readers`   | valid reader cursors (`= MAX_READERS`)       |
/// | 16     | `_pad1`         | pad to keep `write_idx` line-local           |
/// | 24     | `write_idx`     | total messages ever published                |
/// | 32     | `read_indices`  | per-reader consumed counter (`MAX_READERS`)  |
/// | 96     | `capacity_mask` | `capacity - 1` (capacity is a power of two)  |
/// | 104    | `total_written` | monotonic diagnostic counter                 |
/// | 112    | `_pad_to_256`   | pad out to four cache lines                  |
///
/// The struct is never accessed bytewise; every open maps the file and casts
/// the base pointer here, so all reads/writes go through the atomics.
#[repr(C, align(64))]
pub struct RingHeader {
    magic: AtomicU64,
    version: AtomicU32,
    num_readers: AtomicU32,
    _pad1: u64,
    write_idx: AtomicU64,
    read_indices: [AtomicU64; MAX_READERS],
    capacity_mask: AtomicU64,
    total_written: AtomicU64,
    _pad_to_256: [u64; 18],
}

const _: () = assert!(std::mem::size_of::<RingHeader>() <= HEADER_SIZE);
// Machine-check the on-disk contract: repr(C) fixes the declaration order,
// and these pin the exact file-format offsets. Without repr(C) the compiler
// would be free to reorder fields and the file format would be garbage.
const _: () = assert!(std::mem::offset_of!(RingHeader, magic) == 0);
const _: () = assert!(std::mem::offset_of!(RingHeader, version) == 8);
const _: () = assert!(std::mem::offset_of!(RingHeader, num_readers) == 12);
const _: () = assert!(std::mem::offset_of!(RingHeader, write_idx) == 24);
const _: () = assert!(std::mem::offset_of!(RingHeader, read_indices) == 32);
const _: () = assert!(std::mem::offset_of!(RingHeader, capacity_mask) == 96);
const _: () = assert!(std::mem::offset_of!(RingHeader, total_written) == 104);

impl RingHeader {
    /// Builds the initial header for a freshly created ring.
    ///
    /// Plain (non-atomic) `Atomic*::new` construction is sufficient: at this
    /// point the file has just been created and no other thread or process
    /// can possibly have mapped it yet, so there is no concurrency to order
    /// against. The atomics' values become visible to everyone else through
    /// the `Acquire` loads performed by [`Self::validate`] on open.
    pub fn new(capacity_mask: u64, num_readers: usize) -> Self {
        assert!((1..=MAX_READERS).contains(&num_readers));
        Self {
            magic: AtomicU64::new(MAGIC),
            version: AtomicU32::new(VERSION),
            num_readers: AtomicU32::new(num_readers as u32),
            _pad1: 0,
            write_idx: AtomicU64::new(0),
            read_indices: std::array::from_fn(|_| AtomicU64::new(0)),
            capacity_mask: AtomicU64::new(capacity_mask),
            total_written: AtomicU64::new(0),
            _pad_to_256: [0; 18],
        }
    }

    /// Validates `magic`, `version`, and the structural form of
    /// `capacity_mask`.
    ///
    /// Every load is `Acquire`: this is the first observation any opener
    /// makes of the shared file, and it must synchronize-with the plain
    /// initialization writes from `create_new` (or, for a later opener, with
    /// whatever the previous users published). File-length consistency is
    /// checked by the caller, which knows `size_of::<T>()`.
    pub fn validate(&self) -> Result<(), ShmRingError> {
        let magic = self.magic.load(Ordering::Acquire);
        if magic != MAGIC {
            return Err(ShmRingError::HeaderCorruption {
                field: "magic",
                expected: MAGIC,
                got: magic,
            });
        }
        let version = self.version.load(Ordering::Acquire);
        if version != VERSION {
            return Err(ShmRingError::VersionMismatch {
                expected: VERSION,
                got: version,
            });
        }
        let mask = self.capacity_mask.load(Ordering::Acquire);
        // A valid mask is 2^k - 1 for some k >= 0; the producer-side
        // constructor additionally rejects capacity 1 by rejecting k < 1.
        // `expected: 0` documents "no single required value; structural
        // invariant (mask + 1) must be a power of two".
        if mask == 0 || !(mask + 1).is_power_of_two() {
            return Err(ShmRingError::HeaderCorruption {
                field: "capacity_mask",
                expected: 0,
                got: mask,
            });
        }
        Ok(())
    }

    /// Current value of `write_idx` (total published messages).
    pub fn write_idx(&self, ord: Ordering) -> u64 {
        self.write_idx.load(ord)
    }

    /// Publishes a new `write_idx` value.
    pub fn set_write_idx(&self, v: u64, ord: Ordering) {
        self.write_idx.store(v, ord);
    }

    /// Reads reader `i`'s consumed counter.
    pub fn read_idx(&self, i: usize, ord: Ordering) -> u64 {
        self.read_indices[i].load(ord)
    }

    /// Advances reader `i`'s consumed counter to `v`.
    pub fn set_read_idx(&self, i: usize, v: u64, ord: Ordering) {
        self.read_indices[i].store(v, ord);
    }

    /// `min` over the `num_readers` participating reader cursors: the
    /// position of the slowest participant. Cursors beyond `num_readers`
    /// exist in the file but never take part in backpressure.
    pub fn slowest_read_idx(&self, num_readers: usize, ord: Ordering) -> u64 {
        self.read_indices[..num_readers]
            .iter()
            .map(|r| r.load(ord))
            .fold(u64::MAX, u64::min)
    }

    /// The capacity mask stored at creation time.
    pub fn capacity_mask(&self, ord: Ordering) -> u64 {
        self.capacity_mask.load(ord)
    }

    /// Total successful pushes since creation (diagnostic, monotonically
    /// increasing).
    pub fn total_written(&self, ord: Ordering) -> u64 {
        self.total_written.load(ord)
    }

    /// Bumps `total_written` by one.
    pub fn bump_total_written(&self, ord: Ordering) {
        self.total_written.fetch_add(1, ord);
    }

    /// Number of valid reader cursors (always [`MAX_READERS`] in v1).
    pub fn num_readers(&self, ord: Ordering) -> usize {
        self.num_readers.load(ord) as usize
    }
}
