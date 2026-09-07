//! Fuzz `create_new` / `open_existing` with arbitrary paths and capacities.
//!
//! Invariant: neither constructor ever panics; every failure is a typed
//! `ShmRingError`; a file that `create_new` accepts must be accepted by
//! `open_existing` unchanged; a second `create_new` on the same path must
//! fail with `AlreadyExists`.

#![no_main]

use libfuzzer_sys::fuzz_target;
use shm_rings::{SpmcRingBuffer, ShmRingError};
use tempfile::TempDir;

fuzz_target!(|data: &[u8]| {
    let dir = TempDir::new().expect("tempdir");

    // Arbitrary-ish path name derived from the input bytes (hex-encoded so
    // it stays a valid single component).
    let name: String = data
        .iter()
        .take(4)
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .concat();
    let name = if name.is_empty() { "empty".into() } else { name };
    let path = dir.path().join(name);

    // Capacity stream: one raw (possibly invalid) value and one masked
    // power-of-two candidate.
    let raw = u32::from_le_bytes([
        data.first().copied().unwrap_or(0),
        data.get(1).copied().unwrap_or(0),
        data.get(2).copied().unwrap_or(0),
        data.get(3).copied().unwrap_or(0),
    ]);
    let pow2 = 1usize << (u64::from(data.first().copied().unwrap_or(0)) % 12);

    let created = SpmcRingBuffer::<u64>::create_new(&path, pow2);
    match created {
        Ok(ring) => {
            assert_eq!(ring.capacity(), pow2);
            assert_eq!(ring.path(), path.as_path());
            // A second create on the same path must be refused.
            match SpmcRingBuffer::<u64>::create_new(&path, pow2) {
                Err(ShmRingError::AlreadyExists) => {}
                Err(e) => panic!("expected AlreadyExists, got {e}"),
                Ok(_) => panic!("duplicate create_new succeeded"),
            }
            // A ring this crate just wrote must open cleanly.
            let opened = SpmcRingBuffer::<u64>::open_existing(&path)
                .unwrap_or_else(|e| panic!("open failed on ring we just created: {e}"));
            assert_eq!(opened.capacity(), pow2);
            drop(opened);
            drop(ring);
        }
        Err(ShmRingError::AlreadyExists) | Err(ShmRingError::Io(_)) => {
            // Environment-level failures are acceptable outcomes.
        }
        Err(e) => panic!("unexpected constructor error for capacity {pow2}: {e}"),
    }

    // Raw arbitrary capacity must be typed-rejected, never a panic.
    if let Err(e) = SpmcRingBuffer::<u64>::create_new(&path, raw as usize) {
        assert!(
            matches!(
                e,
                ShmRingError::InvalidCapacity { .. } | ShmRingError::AlreadyExists | ShmRingError::Io(_)
            ),
            "raw capacity {raw} produced unexpected error {e}"
        );
    }

    // Opening a nonexistent path must be a typed error, never a panic.
    let missing = dir.path().join("does-not-exist");
    match SpmcRingBuffer::<u64>::open_existing(&missing) {
        Err(ShmRingError::Io(_)) => {}
        Err(e) => panic!("missing file produced unexpected error {e}"),
        Ok(_) => panic!("open_existing succeeded on a missing file"),
    }
});
