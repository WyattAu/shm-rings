//! End-to-end integration tests against real mmap-backed rings.

use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};

use shm_rings::{ShmRingError, SpmcRingBuffer, HEADER_SIZE, MAGIC, MAX_READERS, VERSION};

type U64Ring = SpmcRingBuffer<u64>;

fn temp_ring(name: &str, capacity: usize) -> (tempfile::TempDir, U64Ring) {
    let dir = tempfile::tempdir().expect("tempdir");
    let ring = U64Ring::create_new(dir.path().join(name), capacity).expect("create_new");
    (dir, ring)
}

#[test]
fn create_new_rejects_existing_path() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dup.ring");
    U64Ring::create_new(&path, 8).expect("first create");
    match U64Ring::create_new(&path, 8) {
        Err(ShmRingError::AlreadyExists) => {}
        other => panic!("expected AlreadyExists, got {other:?}"),
    }
}

#[test]
fn roundtrip_push_pop() {
    let (_dir, mut ring) = temp_ring("rt.ring", 16);
    for i in 0..10u64 {
        assert!(ring.try_push(&(i * i)), "push {i}");
    }
    let reader = U64Ring::open_existing(ring.path()).expect("open");
    for i in 0..10u64 {
        assert_eq!(reader.try_pop(0).unwrap(), Some(i * i));
    }
    assert!(reader.try_pop(0).unwrap().is_none(), "drained");
}

#[test]
fn capacity_boundary_backpressure_then_recover() {
    let cap = 4;
    let (_dir, mut ring) = temp_ring("cap.ring", cap);
    for i in 0..cap as u64 {
        assert!(ring.try_push(&i), "push {i} into empty ring");
    }
    // Slowest participant is at 0; ring is full -> backpressure.
    assert!(!ring.try_push(&999), "push past capacity must fail");
    assert_eq!(ring.slowest_lag(), cap as u64);

    let reader = U64Ring::open_existing(ring.path()).unwrap();

    // Participation contract: ALL reader cursors take part in backpressure.
    // Reader 0 advancing alone does not lift the slowest participant, because
    // readers 1..NUM_READERS still pin their cursors one message back.
    assert_eq!(reader.try_pop(0).unwrap(), Some(0));
    assert!(
        !ring.try_push(&999),
        "one of {MAX_READERS} participants advanced only"
    );

    // Once every participant consumes one message the slowest cursor moves
    // to 1 and the producer may push again.
    for id in 1..ring.reader_count() {
        assert_eq!(reader.try_pop(id).unwrap(), Some(0), "reader {id}");
    }
    assert_eq!(ring.slowest_lag(), (cap - 1) as u64);
    assert!(
        ring.try_push(&999),
        "push after every participant popped must succeed"
    );
    // Reader 0 continues its own stream: 1, 2, 3, then the recovered 999
    // (which landed in slot 0, index 4 — its cursor never skipped a beat).
    assert_eq!(reader.try_pop(0).unwrap(), Some(1));
    assert_eq!(reader.try_pop(0).unwrap(), Some(2));
    assert_eq!(reader.try_pop(0).unwrap(), Some(3));
    assert_eq!(reader.try_pop(0).unwrap(), Some(999));
}

#[test]
fn capacity_boundary_single_reader_ring_recovers_after_one_pop() {
    // With exactly one participating reader, the classic boundary dance is
    // literal: fill -> reject -> pop one -> accept.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("single.ring");
    let mut ring = U64Ring::create_new_with_readers(&path, 4, 1).unwrap();
    assert_eq!(ring.reader_count(), 1);

    for i in 0..4u64 {
        assert!(ring.try_push(&i));
    }
    assert!(!ring.try_push(&4));
    let reader = U64Ring::open_existing(&path).unwrap();
    assert_eq!(reader.try_pop(0).unwrap(), Some(0));
    assert!(ring.try_push(&4), "pop one -> push succeeds");
    assert!(reader.try_pop(1).is_err(), "reader 1 was not provisioned");

    for i in 1..5u64 {
        assert_eq!(reader.try_pop(0).unwrap(), Some(i));
    }
}

#[test]
fn readers_are_independent() {
    let (_dir, mut ring) = temp_ring("multi.ring", 16);
    for i in 0..10u64 {
        assert!(ring.try_push(&i));
    }

    let r0 = U64Ring::open_existing(ring.path()).unwrap();
    let r3 = U64Ring::open_existing(ring.path()).unwrap();

    // Reader 0 drains everything while reader 3 never pops.
    for i in 0..10u64 {
        assert_eq!(r0.try_pop(0).unwrap(), Some(i));
    }
    assert!(r0.is_empty(0).unwrap());
    // Reader 3 lagging 10 behind must not have blocked reader 0, and can
    // still consume the full stream afterwards.
    for i in 0..10u64 {
        assert_eq!(r3.try_pop(3).unwrap(), Some(i), "reader 3 sees full stream");
    }
}

#[test]
fn header_corruption_magic_detected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("corrupt.ring");
    drop(U64Ring::create_new(&path, 8).unwrap());

    // Unmap (drop) then damage the magic at offset 0.
    let mut f = OpenOptions::new().write(true).open(&path).unwrap();
    f.seek(SeekFrom::Start(0)).unwrap();
    f.write_all(&[0xDE; 8]).unwrap();
    f.sync_all().unwrap();
    drop(f);

    match U64Ring::open_existing(&path) {
        Err(ShmRingError::HeaderCorruption {
            field,
            expected,
            got,
        }) => {
            assert_eq!(field, "magic");
            assert_eq!(expected, MAGIC);
            assert_ne!(got, MAGIC);
        }
        other => panic!("expected HeaderCorruption, got {other:?}"),
    }
}

#[test]
fn version_mismatch_detected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("version.ring");
    drop(U64Ring::create_new(&path, 8).unwrap());

    let mut f = OpenOptions::new().write(true).open(&path).unwrap();
    f.seek(SeekFrom::Start(8)).unwrap(); // version: u32 at offset 8
    f.write_all(&(VERSION + 1).to_le_bytes()).unwrap();
    f.sync_all().unwrap();
    drop(f);

    match U64Ring::open_existing(&path) {
        Err(ShmRingError::VersionMismatch { expected, got }) => {
            assert_eq!(expected, VERSION);
            assert_eq!(got, VERSION + 1);
        }
        other => panic!("expected VersionMismatch, got {other:?}"),
    }
}

#[test]
fn file_too_short_detected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("short.ring");
    drop(U64Ring::create_new(&path, 8).unwrap());

    // Shrink below the full layout (header + 8 * 8 bytes).
    let f = OpenOptions::new().write(true).open(&path).unwrap();
    f.set_len(HEADER_SIZE as u64 + 32).unwrap();
    drop(f);

    match U64Ring::open_existing(&path) {
        Err(ShmRingError::FileTooShort { expected, got }) => {
            assert_eq!(expected, (HEADER_SIZE + 64) as u64);
            assert_eq!(got, (HEADER_SIZE + 32) as u64);
        }
        other => panic!("expected FileTooShort, got {other:?}"),
    }

    // Also shrink below the header floor itself.
    let f = OpenOptions::new().write(true).open(&path).unwrap();
    f.set_len(100).unwrap();
    drop(f);
    match U64Ring::open_existing(&path) {
        Err(ShmRingError::FileTooShort { expected, got }) => {
            assert_eq!(expected, HEADER_SIZE as u64);
            assert_eq!(got, 100);
        }
        other => panic!("expected FileTooShort (header floor), got {other:?}"),
    }
}

#[test]
fn zero_capacity_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("zero.ring");
    match U64Ring::create_new(&path, 0) {
        Err(ShmRingError::InvalidCapacity { got }) => assert_eq!(got, 0),
        other => panic!("expected InvalidCapacity(0), got {other:?}"),
    }
}

#[test]
fn non_power_of_two_capacity_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("np2.ring");
    for bad in [3usize, 6, 12, 1000, usize::MAX] {
        match U64Ring::create_new(&path, bad) {
            Err(ShmRingError::InvalidCapacity { got }) => assert_eq!(got, bad),
            other => panic!("expected InvalidCapacity({bad}), got {other:?}"),
        }
        assert!(!path.exists(), "no file may be created on rejection");
    }
}

#[test]
fn invalid_reader_id_rejected() {
    let (_dir, ring) = temp_ring("rid.ring", 4);
    let max = ring.reader_count() - 1;
    assert_eq!(max, MAX_READERS - 1);
    match ring.try_pop(max + 1) {
        Err(ShmRingError::InvalidReaderId { id, max: m }) => {
            assert_eq!(id, max + 1);
            assert_eq!(m, max);
        }
        other => panic!("expected InvalidReaderId, got {other:?}"),
    }
    match ring.len(max + 1) {
        Err(ShmRingError::InvalidReaderId { .. }) => {}
        other => panic!("expected InvalidReaderId from len, got {other:?}"),
    }
    match ring.peek(max + 1) {
        Err(ShmRingError::InvalidReaderId { .. }) => {}
        other => panic!("expected InvalidReaderId from peek, got {other:?}"),
    }
}

#[test]
fn peek_is_non_consuming() {
    let (_dir, mut ring) = temp_ring("peek.ring", 4);
    assert!(ring.try_push(&0xABCD));
    let reader = U64Ring::open_existing(ring.path()).unwrap();

    let ptr = reader.peek(0).unwrap().expect("peek on non-empty");
    // SAFETY: pointer returned by peek(0); no try_pop has advanced reader
    // 0's cursor since, so the slot is within its documented validity
    // window.
    let v = unsafe { *ptr };
    assert_eq!(v, 0xABCD);

    // Non-consuming: peek again -> same value; the cursor has not moved.
    let ptr2 = reader.peek(0).unwrap().expect("second peek");
    // SAFETY: same window as above.
    assert_eq!(unsafe { *ptr2 }, 0xABCD);

    // try_pop consumes the same message.
    assert_eq!(reader.try_pop(0).unwrap(), Some(0xABCD));
    assert!(reader.peek(0).unwrap().is_none());
}

#[test]
fn diagnostics_track_state() {
    let (_dir, mut ring) = temp_ring("diag.ring", 8);
    assert_eq!(ring.capacity(), 8);
    assert_eq!(ring.reader_count(), MAX_READERS);
    assert!(ring.is_empty(0).unwrap());
    assert_eq!(ring.total_written(), 0);
    assert_eq!(ring.slowest_lag(), 0);

    for i in 0..5u64 {
        assert!(ring.try_push(&i));
    }
    assert_eq!(ring.total_written(), 5);
    assert_eq!(ring.slowest_lag(), 5);
    assert_eq!(ring.len(2).unwrap(), 5);
    assert!(!ring.is_empty(2).unwrap());

    let reader = U64Ring::open_existing(ring.path()).unwrap();
    // Every participant must advance for the slowest cursor to move.
    for id in 0..ring.reader_count() {
        assert_eq!(reader.try_pop(id).unwrap(), Some(0), "reader {id}");
    }
    assert_eq!(ring.slowest_lag(), 4);
    assert_eq!(reader.path(), ring.path());
}

#[test]
fn handles_are_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<U64Ring>();
    assert_send_sync::<ShmRingError>();
}

#[test]
fn open_existing_is_idempotent_and_shared() {
    // Many handles to the same file cooperate through the same header.
    let (_dir, mut ring) = temp_ring("shared.ring", 8);
    let mut handles = Vec::new();
    for _ in 0..MAX_READERS {
        handles.push(U64Ring::open_existing(ring.path()).unwrap());
    }
    for i in 0..8u64 {
        assert!(ring.try_push(&i));
    }
    for (id, h) in handles.iter().enumerate() {
        for i in 0..8u64 {
            assert_eq!(h.try_pop(id).unwrap(), Some(i), "reader {id}");
        }
    }
}
