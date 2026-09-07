//! Integration tests for [`shm_rings::status`].
//!
//! Ports the suture-daemon `shm.rs` test patterns (round-trip, magic
//! reject, update, cleanup) and extends them: version reject (the gate
//! magic-only validation misses), short-file reject, and parametrization
//! over two distinct POD types to prove the trait is generic, not
//! hard-wired to one layout.

#![cfg(unix)]

use std::path::PathBuf;

use shm_rings::ShmRingError;
use shm_rings::status::{self, PodStatus};

/// Status type A: scalar fields.
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
struct StatusA {
    magic: u64,
    version: u32,
    repo_count: u32,
    total: u64,
}

impl PodStatus for StatusA {
    const MAGIC: u64 = 0x5354_4154_4141_4141;
    const VERSION: u32 = 1;
    fn magic(&self) -> u64 {
        self.magic
    }
    fn version(&self) -> u32 {
        self.version
    }
    fn set_magic(&mut self) {
        self.magic = Self::MAGIC;
    }
}

/// Status type B: different size, fields, magic, and version.
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
struct StatusB {
    magic: u64,
    version: u32,
    flags: u32,
    blob: [u8; 32],
}

impl PodStatus for StatusB {
    const MAGIC: u64 = 0x5354_4154_4242_4242;
    const VERSION: u32 = 7;
    fn magic(&self) -> u64 {
        self.magic
    }
    fn version(&self) -> u32 {
        self.version
    }
    fn set_magic(&mut self) {
        self.magic = Self::MAGIC;
    }
}

/// Same layout as A, same VERSION, different MAGIC: isolates the magic gate.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct StatusC {
    magic: u64,
    version: u32,
    repo_count: u32,
    total: u64,
}

impl PodStatus for StatusC {
    const MAGIC: u64 = 0x5354_4154_4343_4343;
    const VERSION: u32 = 1;
    fn magic(&self) -> u64 {
        self.magic
    }
    fn version(&self) -> u32 {
        self.version
    }
    fn set_magic(&mut self) {
        self.magic = Self::MAGIC;
    }
}

/// Same MAGIC as A but different VERSION: isolates the version gate.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct StatusAV2 {
    magic: u64,
    version: u32,
    repo_count: u32,
    total: u64,
}

impl PodStatus for StatusAV2 {
    const MAGIC: u64 = StatusA::MAGIC;
    const VERSION: u32 = 2;
    fn magic(&self) -> u64 {
        self.magic
    }
    fn version(&self) -> u32 {
        self.version
    }
    fn set_magic(&mut self) {
        self.magic = Self::MAGIC;
    }
}

fn fresh_path(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "shm-rings-status-test-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

fn status_a(count: u32, total: u64) -> StatusA {
    StatusA {
        magic: StatusA::MAGIC,
        version: StatusA::VERSION,
        repo_count: count,
        total,
    }
}

fn status_b(flags: u32, blob: &[u8]) -> StatusB {
    let mut b = StatusB {
        magic: StatusB::MAGIC,
        version: StatusB::VERSION,
        flags,
        blob: [0u8; 32],
    };
    let n = blob.len().min(32);
    b.blob[..n].copy_from_slice(&blob[..n]);
    b
}

// ---- pattern 1 (suture: test_shm_round_trip) — parametrized over A and B

#[test]
fn round_trip_status_a() {
    let path = fresh_path("rt-a");
    status::create(&path, &status_a(3, 1000)).expect("create");

    let st: StatusA = status::read(&path).expect("read");
    assert_eq!(st.magic, StatusA::MAGIC);
    assert_eq!(st.version, StatusA::VERSION);
    assert_eq!(st.repo_count, 3);
    assert_eq!(st.total, 1000);

    status::cleanup(&path).expect("cleanup");
}

#[test]
fn round_trip_status_b() {
    let path = fresh_path("rt-b");
    status::create(&path, &status_b(0b1010, b"head-branch-name")).expect("create");

    let st: StatusB = status::read(&path).expect("read");
    assert_eq!(st.flags, 0b1010);
    assert_eq!(&st.blob[..16], b"head-branch-name");

    status::cleanup(&path).expect("cleanup");
}

// ---- pattern 2 (suture: test_shm_magic) — magic reject

#[test]
fn read_rejects_corrupted_magic() {
    let path = fresh_path("magic");
    status::create(&path, &status_a(1, 1)).expect("create");

    let mut raw = std::fs::read(&path).expect("read raw");
    raw[..8].copy_from_slice(&0xDEADBEEFu64.to_le_bytes());
    std::fs::write(&path, &raw).expect("write raw");

    let err = status::read::<StatusA>(&path).expect_err("must reject");
    match err {
        ShmRingError::HeaderCorruption {
            field: "magic",
            expected,
            got,
        } => {
            assert_eq!(expected, StatusA::MAGIC);
            assert_eq!(got, 0xDEADBEEF);
        }
        other => panic!("expected HeaderCorruption(magic), got {other:?}"),
    }

    status::cleanup(&path).expect("cleanup");
}

// ---- pattern 2b — version reject (the improvement over magic-only)

#[test]
fn read_rejects_version_mismatch() {
    let path = fresh_path("ver");
    status::create(&path, &status_a(1, 1)).expect("create");

    // Same magic, different layout version: must NOT be read as StatusAV2.
    let err = status::read::<StatusAV2>(&path).expect_err("must reject");
    match err {
        ShmRingError::VersionMismatch { expected, got } => {
            assert_eq!(expected, StatusAV2::VERSION);
            assert_eq!(got, StatusA::VERSION);
        }
        other => panic!("expected VersionMismatch, got {other:?}"),
    }

    status::cleanup(&path).expect("cleanup");
}

// ---- pattern 2c — short file reject

#[test]
fn read_rejects_short_file() {
    let path = fresh_path("short");
    status::create(&path, &status_a(1, 1)).expect("create");

    let mut raw = std::fs::read(&path).expect("read raw");
    raw.truncate(size_of::<StatusA>() - 8);
    std::fs::write(&path, &raw).expect("write raw");

    let err = status::read::<StatusA>(&path).expect_err("must reject");
    assert!(matches!(err, ShmRingError::FileTooShort { .. }), "got {err:?}");

    status::cleanup(&path).expect("cleanup");
}

// ---- pattern 3 (suture: test_shm_cleanup) — cleanup + idempotence

#[test]
fn cleanup_removes_and_is_idempotent() {
    let path = fresh_path("cleanup");
    status::create(&path, &status_a(1, 1)).expect("create");
    assert!(path.exists(), "file should exist after create");

    status::cleanup(&path).expect("cleanup");
    assert!(!path.exists(), "file should be gone after cleanup");

    // Second cleanup on a missing file: still Ok (idempotent).
    status::cleanup(&path).expect("second cleanup");
}

// ---- pattern 4 (suture: test_shm_update) — update round-trip

#[test]
fn update_persists_mutation() {
    let path = fresh_path("update");
    status::create(&path, &status_a(1, 20)).expect("create");

    let mut st: StatusA = status::read(&path).expect("read");
    assert_eq!(st.total, 20);

    st.total = 99;
    st.repo_count = 5;
    status::update(&path, &st).expect("update");

    let updated: StatusA = status::read(&path).expect("read after update");
    assert_eq!(updated.total, 99);
    assert_eq!(updated.repo_count, 5);

    status::cleanup(&path).expect("cleanup");
}

#[test]
fn update_rejects_foreign_file() {
    // Same size as StatusA (24 bytes), so the length gate passes and the
    // identity gates are what reject — for both magic and version.
    let path_a = fresh_path("upd-foreign-magic");
    status::create(&path_a, &status_a(1, 1)).expect("create");
    let err = status::update(&path_a, &StatusC {
        magic: StatusC::MAGIC,
        version: StatusC::VERSION,
        repo_count: 1,
        total: 1,
    })
    .expect_err("must reject");
    assert!(
        matches!(
            err,
            ShmRingError::HeaderCorruption { field: "magic", .. }
        ),
        "got {err:?}"
    );
    status::cleanup(&path_a).expect("cleanup");

    let path_av2 = fresh_path("upd-foreign-version");
    status::create(&path_av2, &status_a(1, 1)).expect("create");
    let err = status::update(&path_av2, &StatusAV2 {
        magic: StatusAV2::MAGIC,
        version: StatusAV2::VERSION,
        repo_count: 1,
        total: 1,
    })
    .expect_err("must reject");
    assert!(matches!(err, ShmRingError::VersionMismatch { .. }), "got {err:?}");
    status::cleanup(&path_av2).expect("cleanup");
}

// ---- pattern 5 — create overwrites prior content (documented semantics)

#[test]
fn create_overwrites_existing_file() {
    let path = fresh_path("overwrite");
    status::create(&path, &status_a(1, 1)).expect("create");
    status::create(&path, &status_a(9, 9)).expect("second create");

    let st: StatusA = status::read(&path).expect("read");
    assert_eq!(st.repo_count, 9);
    assert_eq!(st.total, 9);

    status::cleanup(&path).expect("cleanup");
}

// ---- pattern 6 — set_magic helper round-trips a zeroed value

#[test]
fn set_magic_stamps_fresh_value() {
    let mut st = StatusA {
        magic: 0,
        version: StatusA::VERSION,
        repo_count: 0,
        total: 0,
    };
    assert!(!st.magic_matches());
    st.set_magic();
    assert!(st.magic_matches());
    assert!(st.version_matches());
}
