//! Generic file-backed POD status: [`PodStatus`] + [`create`]/[`read`]/
//! [`update`]/[`cleanup`].
//!
//! A status file is the simplest shared-memory primitive: one `#[repr(C)]`
//! plain-old-data struct, mapped into a fixed-length file, written by one
//! process and read by others. The ring ([`crate::SpmcRingBuffer`]) streams
//! messages; a status file publishes *state* — counters, mount flags, pids,
//! heartbeat timestamps. This module gives that pattern a validated,
//! reusable home with the same safety discipline as the ring.
//!
//! # The trait
//!
//! Implementors are `#[repr(C)]` structs whose **first two fields are the
//! magic and version** (the layout convention every file-backed status in
//! this ecosystem follows — `u64` magic at offset 0, `u32` version at
//! offset 8). The trait carries:
//!
//! - [`PodStatus::MAGIC`] / [`PodStatus::VERSION`] — the identity constants;
//! - [`PodStatus::set_magic`] — stamps a freshly constructed value;
//! - [`PodStatus::version_matches`] — the reader-side version gate;
//! - [`PodStatus::magic_matches`] — the reader-side magic gate (both are
//!   enforced by [`read`]/[`update`]; magic alone is *not* enough — a
//!   same-magic layout bump must fail loudly, not misinterpret bytes);
//! - [`PodStatus::magic`] / [`PodStatus::version`] — accessors used to
//!   report *what was found* in a rejection.
//!
//! # POD contract
//!
//! `T: PodStatus` must be plain old data: `#[repr(C)]`, no heap pointers,
//! no `Drop`, no interior mutability, no padding-dependent semantics, and
//! **no invalid bit patterns** (every `size_of::<T>()` byte sequence must
//! decode to a valid `T`, since `read` materializes a `T` with
//! `ptr::read` after validation only of magic/version). Append new fields
//! at the end and bump [`PodStatus::VERSION`]; old readers then fail with
//! [`ShmRingError::VersionMismatch`] instead of misreading.
//!
//! # Errors
//!
//! Reuses the ring's error vocabulary: a short file is
//! [`ShmRingError::FileTooShort`], a magic mismatch is
//! [`ShmRingError::HeaderCorruption`], a version mismatch is
//! [`ShmRingError::VersionMismatch`], and all filesystem/mmap failures map
//! to [`ShmRingError::Io`].
//!
//! # Example
//!
//! ```
//! use shm_rings::status::{self, PodStatus};
//! use std::path::Path;
//!
//! #[derive(Debug, Clone, Copy)]
//! #[repr(C)]
//! struct MountStatus {
//!     magic: u64,      // must be the first field
//!     version: u32,    // must be the second field
//!     is_mounted: u32,
//! }
//!
//! impl PodStatus for MountStatus {
//!     const MAGIC: u64 = 0x4D535455; // "MSTU"
//!     const VERSION: u32 = 1;
//!     fn magic(&self) -> u64 { self.magic }
//!     fn version(&self) -> u32 { self.version }
//!     fn set_magic(&mut self) { self.magic = Self::MAGIC; }
//! }
//!
//! # fn main() -> Result<(), shm_rings::ShmRingError> {
//! let path = std::env::temp_dir().join("shm-rings-doc-status");
//! let initial = MountStatus {
//!     magic: MountStatus::MAGIC,
//!     version: MountStatus::VERSION,
//!     is_mounted: 0,
//! };
//! status::create(&path, &initial)?;
//!
//! let mut st: MountStatus = status::read(&path)?;
//! st.is_mounted = 1;
//! status::update(&path, &st)?;
//! assert_eq!(status::read::<MountStatus>(&path)?.is_mounted, 1);
//!
//! status::cleanup(&path)?;
//! # Ok(())
//! # }
//! ```

use std::fs::{File, OpenOptions};
use std::path::Path;

use memmap2::{Mmap, MmapMut};

use crate::error::ShmRingError;

/// A plain-old-data status struct that can live in a file-backed mapping.
///
/// See the [module docs](self) for the layout and POD contract.
pub trait PodStatus: Copy {
    /// Identity constant written at offset 0; [`read`]/[`update`] reject
    /// files whose stored magic differs.
    const MAGIC: u64;

    /// Layout version written at offset 8; [`read`]/[`update`] reject files
    /// whose stored version differs. Bump on any layout change.
    const VERSION: u32;

    /// The stored magic value (for rejection diagnostics).
    fn magic(&self) -> u64;

    /// The stored version value (for rejection diagnostics).
    fn version(&self) -> u32;

    /// Stamps [`Self::MAGIC`] into the magic field. Call on freshly
    /// constructed values whose magic field starts zeroed.
    fn set_magic(&mut self);

    /// Reader gate: stored magic equals [`Self::MAGIC`].
    fn magic_matches(&self) -> bool {
        self.magic() == Self::MAGIC
    }

    /// Reader gate: stored version equals [`Self::VERSION`].
    fn version_matches(&self) -> bool {
        self.version() == Self::VERSION
    }
}

/// Creates (or overwrites) the status file at `path` with a copy of
/// `initial`.
///
/// The file is opened with create + truncate, extended to exactly
/// `size_of::<T>()` bytes, the bytes of `initial` are copied in, and the
/// mapping is flushed to disk. Any prior content is destroyed — status
/// files are single-writer state, unlike rings whose creation is
/// exclusive ([`crate::ShmRingError::AlreadyExists`]).
pub fn create<T: PodStatus>(path: &Path, initial: &T) -> Result<(), ShmRingError> {
    let size = size_of::<T>();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    file.set_len(size as u64)?;

    // SAFETY: `file` is a valid opened file handle of length `size` >= 1
    // (guaranteed by `set_len` above; the module POD contract excludes ZST
    // status types); memmap2 maps it MAP_SHARED for mutation. No other
    // mapping of this just-truncated file exists yet.
    let mut mmap = unsafe { MmapMut::map_mut(&file) }?;
    drop(file);

    // SAFETY: `initial` is a valid `T` on the caller's stack (T: Copy by
    // the trait contract, so the borrow is a bitwise copy with no drop
    // hazards). We reinterpret it as a byte slice of exactly `size_of::<T>()`
    // bytes; the pointer derives from a live reference, so it is aligned
    // and valid for that length.
    let bytes =
        unsafe { std::slice::from_raw_parts(std::ptr::from_ref::<T>(initial).cast::<u8>(), size) };
    mmap.copy_from_slice(bytes);
    mmap.flush()?;
    Ok(())
}

/// Reads a validated status value from the file at `path`.
///
/// Maps the file read-only and checks, in order:
///
/// 1. **Length** — the file must hold at least `size_of::<T>()` bytes
///    ([`ShmRingError::FileTooShort`] otherwise);
/// 2. **Magic** — [`ShmRingError::HeaderCorruption`] with `field: "magic"`;
/// 3. **Version** — [`ShmRingError::VersionMismatch`] (this is the gate a
///    magic-only check misses: same magic, changed layout).
///
/// Only after all three pass is the byte region materialized as a `T`.
pub fn read<T: PodStatus>(path: &Path) -> Result<T, ShmRingError> {
    let file = File::open(path)?;
    // SAFETY: read-only mapping of a file opened for reading. The length
    // check below guards every subsequent byte access; the mapping base is
    // page-aligned, so offset 0 satisfies `align_of::<T>()` for any T.
    let mmap = unsafe { Mmap::map(&file) }?;

    let size = size_of::<T>();
    if (mmap.len() as u64) < size as u64 {
        return Err(ShmRingError::FileTooShort {
            expected: size as u64,
            got: mmap.len() as u64,
        });
    }

    // SAFETY: `mmap.len() >= size_of::<T>()` was verified above, and the
    // base pointer is page-aligned (hence T-aligned). The module POD
    // contract guarantees no bit pattern of the region is an invalid `T`,
    // so materializing the value with `ptr::read` is sound; magic/version
    // validation happens on the materialized value immediately after.
    let value: T = unsafe { std::ptr::read(mmap.as_ptr().cast::<T>()) };

    if !value.magic_matches() {
        return Err(ShmRingError::HeaderCorruption {
            field: "magic",
            expected: T::MAGIC,
            got: value.magic(),
        });
    }
    if !value.version_matches() {
        return Err(ShmRingError::VersionMismatch {
            expected: T::VERSION,
            got: value.version(),
        });
    }
    Ok(value)
}

/// Overwrites the status file at `path` with a copy of `value`.
///
/// Opens the existing file read-write (no create, no truncate), maps it
/// mutable, validates its length and identity exactly as [`read`] does —
/// so an update can never reinterpret foreign bytes — copies `value` in,
/// and flushes.
pub fn update<T: PodStatus>(path: &Path, value: &T) -> Result<(), ShmRingError> {
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    // SAFETY: `file` is an existing, read-write opened file created by
    // [`create`] with a known size; memmap2 maps it MAP_SHARED for
    // mutation. The length/identity checks below guard all byte access.
    let mut mmap = unsafe { MmapMut::map_mut(&file) }?;
    drop(file);

    // 1. Length: never copy into (or read identities out of) a short file.
    let size = size_of::<T>();
    if (mmap.len() as u64) < size as u64 {
        return Err(ShmRingError::FileTooShort {
            expected: size as u64,
            got: mmap.len() as u64,
        });
    }

    // 2. Identity: the file must already hold a valid-looking T. Reading
    //    the head fields through a reference into the (T-aligned,
    //    length-checked) mapping keeps this path free of value
    //    materialization.
    // SAFETY: the mapping covers `size_of::<T>()` bytes at offset 0 and is
    // page-aligned, so an aligned read of the two head fields is in-bounds.
    let (stored_magic, stored_version) = unsafe {
        (
            std::ptr::read(mmap.as_ptr().cast::<u64>()),
            std::ptr::read(mmap.as_ptr().add(8).cast::<u32>()),
        )
    };
    if stored_magic != T::MAGIC {
        return Err(ShmRingError::HeaderCorruption {
            field: "magic",
            expected: T::MAGIC,
            got: stored_magic,
        });
    }
    if stored_version != T::VERSION {
        return Err(ShmRingError::VersionMismatch {
            expected: T::VERSION,
            got: stored_version,
        });
    }

    // SAFETY: `value` is a valid `T` reference; the byte slice length is
    // exactly `size_of::<T>()`, matching the verified mapping length, and
    // the pointer derives from a live reference (aligned, valid).
    let bytes =
        unsafe { std::slice::from_raw_parts(std::ptr::from_ref::<T>(value).cast::<u8>(), size) };
    mmap.copy_from_slice(bytes);
    mmap.flush()?;
    Ok(())
}

/// Removes the status file at `path`.
///
/// Idempotent: a missing file is success, matching the cleanup-at-shutdown
/// pattern where any of several cooperating processes may clean up first.
pub fn cleanup(path: &Path) -> Result<(), ShmRingError> {
    if path.exists() {
        std::fs::remove_file(path)?;
    }
    Ok(())
}
