//! Integration tests for zero-copy loans ([`shm_rings::Loan`]) against
//! real mmap-backed rings: commit/abort/drop semantics, pipeline capacity,
//! pinning, and concurrent interleavings.
// Test/bench code: unwrap/expect are the idiomatic way to assert outcomes.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use shm_rings::{ShmRingError, SpmcRingBuffer};

type U64Ring = SpmcRingBuffer<u64>;

fn temp_ring(name: &str, capacity: usize) -> (tempfile::TempDir, U64Ring) {
    let dir = tempfile::tempdir().expect("tempdir");
    let ring =
        U64Ring::create_new_with_readers(dir.path().join(name), capacity, 1).expect("create_new");
    (dir, ring)
}

/// Two participating readers (tests that exercise reader independence).
fn temp_ring_2readers(name: &str, capacity: usize) -> (tempfile::TempDir, U64Ring) {
    let dir = tempfile::tempdir().expect("tempdir");
    let ring =
        U64Ring::create_new_with_readers(dir.path().join(name), capacity, 2).expect("create_new");
    (dir, ring)
}

#[test]
fn loan_commit_matches_try_pop_values() {
    let (_dir, mut ring) = temp_ring("loan_values.ring", 16);
    for i in 0..8u64 {
        assert!(ring.try_push(&(i * 3)));
    }
    let reader = U64Ring::open_existing(ring.path()).unwrap();
    for i in 0..8u64 {
        let mut loan = reader.claim(0).unwrap().expect("message available");
        assert_eq!(loan.index(), i);
        assert_eq!(*loan, i * 3, "loan must view the pushed record in place");
        loan.commit().unwrap();
    }
    assert!(reader.claim(0).unwrap().is_none(), "drained");
    // The commit path and try_pop observe identical stream state.
    assert_eq!(reader.try_pop(0).unwrap(), None);
}

#[test]
fn loan_drop_commits_by_default() {
    let (_dir, mut ring) = temp_ring("loan_drop.ring", 8);
    for i in 0..4u64 {
        assert!(ring.try_push(&i));
    }
    let reader = U64Ring::open_existing(ring.path()).unwrap();
    {
        let loan = reader.claim(0).unwrap().expect("loan");
        assert_eq!(*loan, 0);
        // Drop without explicit commit: RAII default commits (cursor 0 -> 1).
    }
    assert_eq!(ring.slowest_lag(), 3, "cursor advanced by drop-commit");
    // Next claim starts at index 1.
    let next = reader.claim(0).unwrap().expect("next message");
    assert_eq!(next.index(), 1);
}

#[test]
fn loan_abort_rewinds_and_message_is_reclaimable() {
    let (_dir, mut ring) = temp_ring("loan_abort.ring", 8);
    for i in 0..4u64 {
        assert!(ring.try_push(&i));
    }
    let reader = U64Ring::open_existing(ring.path()).unwrap();

    let loan = reader.claim(0).unwrap().expect("loan");
    assert_eq!(*loan, 0);
    loan.abort();
    // Abort did not advance the cursor: the producer stays pinned...
    assert_eq!(ring.slowest_lag(), 4);
    // ...and the same message is claimable again with identical content.
    let mut again = reader.claim(0).unwrap().expect("re-claim after abort");
    assert_eq!(again.index(), 0);
    assert_eq!(*again, 0);
    again.commit().unwrap();
    assert_eq!(ring.slowest_lag(), 3, "commit released the pin");
}

#[test]
fn loan_abort_rewinds_pipeline_later_loans_recover() {
    let (_dir, mut ring) = temp_ring("loan_rewind.ring", 8);
    for i in 0..4u64 {
        assert!(ring.try_push(&i));
    }
    let reader = U64Ring::open_existing(ring.path()).unwrap();

    let first = reader.claim(0).unwrap().expect("loan 0");
    let mut second = reader.claim(0).unwrap().expect("loan 1");
    assert_eq!((first.index(), second.index()), (0, 1));

    // Rewind to 0: message 0 is released unconsumed.
    first.abort();
    // The stale later loan cannot commit (cursor is still at 0).
    match second.commit() {
        Err(ShmRingError::LoanNotAtCursor { index, cursor }) => {
            assert_eq!((index, cursor), (1, 0));
        }
        other => panic!("expected LoanNotAtCursor, got {other:?}"),
    }
    // Re-claim re-issues index 0; the pipeline resumes in order.
    let mut reclaimed = reader.claim(0).unwrap().expect("re-claim");
    assert_eq!(reclaimed.index(), 0);
    reclaimed.commit().unwrap();
    // The stale second loan's index is next in line: it commits now.
    second.commit().unwrap();
    assert_eq!(ring.slowest_lag(), 2, "two messages consumed");
}

#[test]
fn out_of_order_commit_rejected_then_fifo_succeeds() {
    let (_dir, mut ring) = temp_ring("loan_fifo.ring", 8);
    for i in 0..3u64 {
        assert!(ring.try_push(&i));
    }
    let reader = U64Ring::open_existing(ring.path()).unwrap();
    let mut loans: Vec<_> = (0..3).map(|_| reader.claim(0).unwrap().unwrap()).collect();
    assert_eq!(
        loans.iter().map(|l| l.index()).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );

    match loans[2].commit() {
        Err(ShmRingError::LoanNotAtCursor { .. }) => {}
        other => panic!("expected LoanNotAtCursor, got {other:?}"),
    }
    // Nothing was written by the failed commit; the loan still pins.
    assert_eq!(*loans[2], 2);
    for (i, loan) in loans.iter_mut().enumerate() {
        loan.commit().unwrap_or_else(|e| panic!("commit {i}: {e}"));
    }
    assert_eq!(ring.slowest_lag(), 0);
}

#[test]
fn commit_is_idempotent_and_drop_after_commit_is_noop() {
    let (_dir, mut ring) = temp_ring("loan_idem.ring", 8);
    assert!(ring.try_push(&7));
    let reader = U64Ring::open_existing(ring.path()).unwrap();
    let mut loan = reader.claim(0).unwrap().unwrap();
    loan.commit().unwrap();
    loan.commit().unwrap(); // idempotent
    drop(loan); // must not double-advance
    assert_eq!(ring.slowest_lag(), 0, "exactly one cursor advance");
    assert!(reader.claim(0).unwrap().is_none());
}

#[test]
fn overlapping_loans_up_to_capacity() {
    let cap = 8;
    let (_dir, mut ring) = temp_ring("loan_cap.ring", cap);
    for i in 0..cap as u64 {
        assert!(ring.try_push(&i));
    }
    let reader = U64Ring::open_existing(ring.path()).unwrap();

    let loans: Vec<_> = (0..cap)
        .map(|_| reader.claim(0).unwrap().unwrap())
        .collect();
    assert_eq!(
        loans.iter().map(|l| l.index()).collect::<Vec<_>>(),
        (0..cap as u64).collect::<Vec<_>>()
    );
    // The published stream is exhausted: further claims get None (producer
    // is backpressured, so the pipeline is bounded by capacity).
    assert!(
        reader.claim(0).unwrap().is_none(),
        "no more published messages"
    );

    for (i, mut loan) in loans.into_iter().enumerate() {
        assert_eq!(*loan, i as u64, "loan {i} views its own record");
        loan.commit().unwrap();
    }
    assert_eq!(ring.slowest_lag(), 0);
}

#[test]
fn try_pop_rejected_while_loan_outstanding() {
    let (_dir, mut ring) = temp_ring_2readers("loan_guard.ring", 8);
    assert!(ring.try_push(&1));
    let reader = U64Ring::open_existing(ring.path()).unwrap();

    let mut loan = reader.claim(0).unwrap().unwrap();
    match reader.try_pop(0) {
        Err(ShmRingError::LoanOutstanding { reader_id }) => assert_eq!(reader_id, 0),
        other => panic!("expected LoanOutstanding, got {other:?}"),
    }
    // Other readers are unaffected by reader 0's gate.
    assert_eq!(reader.try_pop(1).unwrap(), Some(1));
    loan.commit().unwrap();
    assert!(
        reader.try_pop(0).unwrap().is_none(),
        "loan consumed the message"
    );
}

#[test]
fn loan_pins_producer_at_capacity() {
    let cap = 4;
    let (_dir, mut ring) = temp_ring("loan_pin.ring", cap);
    for i in 0..cap as u64 {
        assert!(ring.try_push(&i));
    }
    let reader = U64Ring::open_existing(ring.path()).unwrap();

    let mut loan = reader.claim(0).unwrap().unwrap();
    // Ring full, cursor pinned at 0 by the open loan: producer is stopped.
    assert!(!ring.try_push(&99));
    loan.commit().unwrap();
    // Cursor moved to 1: one slot is free again.
    assert!(ring.try_push(&99));
}

#[test]
fn failed_claim_releases_the_gate() {
    let (_dir, ring) = temp_ring("loan_empty.ring", 8);
    // Empty ring: claim fails...
    assert!(ring.claim(0).unwrap().is_none());
    // ...and the loan gate must be released: try_pop still works.
    let mut producer = U64Ring::open_existing(ring.path()).unwrap();
    assert!(producer.try_push(&5));
    assert_eq!(ring.try_pop(0).unwrap(), Some(5));
}

#[test]
fn loan_bytes_view_matches_record() {
    #[repr(C)]
    #[derive(Debug, Clone, Copy, zerocopy::FromBytes, zerocopy::Immutable)]
    struct Record {
        seq: u64,
        payload: [u8; 16],
        checksum: u64,
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("loan_record.ring");
    let mut ring = SpmcRingBuffer::<Record>::create_new(&path, 4).unwrap();
    let record = Record {
        seq: 0xDEAD_BEEF,
        payload: *b"fixed-layout-ok!",
        checksum: 0x1234,
    };
    assert!(ring.try_push(&record));

    let reader = SpmcRingBuffer::<Record>::open_existing(&path).unwrap();
    let mut loan = reader.claim(0).unwrap().unwrap();
    assert_eq!(loan.seq, 0xDEAD_BEEF);
    assert_eq!(loan.payload, *b"fixed-layout-ok!");
    // The byte view is the mapped record, size_of(Record) bytes long.
    assert_eq!(loan.as_bytes().len(), std::mem::size_of::<Record>());
    assert_eq!(loan.as_bytes()[..8], record.seq.to_ne_bytes());
    loan.commit().unwrap();
}

#[test]
fn loans_on_different_readers_are_independent() {
    let (_dir, mut ring) = temp_ring_2readers("loan_readers.ring", 8);
    for i in 0..4u64 {
        assert!(ring.try_push(&i));
    }
    let reader = U64Ring::open_existing(ring.path()).unwrap();

    let mut loan0 = reader.claim(0).unwrap().unwrap();
    // Reader 0's gate does not block reader 1's pops or claims.
    assert_eq!(reader.try_pop(1).unwrap(), Some(0));
    let mut loan1 = reader.claim(1).unwrap().unwrap();
    assert_eq!(loan1.index(), 1, "reader 1's pop advanced its own cursor");
    assert_eq!(loan0.index(), 0);
    loan0.commit().unwrap();
    loan1.commit().unwrap();
}

#[test]
fn claim_realigns_after_external_cursor_advance() {
    let (_dir, mut ring) = temp_ring("loan_realign.ring", 8);
    for i in 0..4u64 {
        assert!(ring.try_push(&i));
    }
    let a = U64Ring::open_existing(ring.path()).unwrap();
    let b = U64Ring::open_existing(ring.path()).unwrap();

    // Handle A claims and commits index 0.
    let mut loan = a.claim(0).unwrap().unwrap();
    loan.commit().unwrap();
    // Handle B (fresh pipeline at base 0, cursor now 1) must realign and
    // claim index 1, never a stale index behind the cursor.
    let loan_b = b.claim(0).unwrap().unwrap();
    assert_eq!(loan_b.index(), 1);
    assert_eq!(*loan_b, 1);
}

#[test]
fn loans_are_send_and_resolvable_on_another_thread() {
    let (_dir, mut ring) = temp_ring("loan_send.ring", 8);
    assert!(ring.try_push(&0xC0FFEE));
    let reader = Arc::new(U64Ring::open_existing(ring.path()).unwrap());
    let thread_reader = Arc::clone(&reader);

    let handle = std::thread::spawn(move || {
        let loan = thread_reader.claim(0).unwrap().unwrap();
        assert_eq!(*loan, 0xC0FFEE);
        let mut loan = loan;
        loan.commit().unwrap();
        loan.index()
    });
    assert_eq!(handle.join().unwrap(), 0);
    assert_eq!(ring.slowest_lag(), 0);
}

#[test]
fn concurrent_claims_commits_with_producer() {
    const MESSAGES: u64 = 5_000;
    let cap = 64;
    let (_dir, ring) = temp_ring("loan_concurrent.ring", cap);
    let ring = Arc::new(ring);
    let producer_ring = U64Ring::open_existing(ring.path()).unwrap();
    let reader = Arc::clone(&ring);

    let consumed = Arc::new(AtomicUsize::new(0));
    let consumed_producer = Arc::clone(&consumed);

    let producer = std::thread::spawn(move || {
        let mut ring = producer_ring;
        for i in 0..MESSAGES {
            while !ring.try_push(&i) {
                std::hint::spin_loop();
            }
        }
    });

    let consumer = std::thread::spawn(move || {
        let mut got = 0u64;
        while got < MESSAGES {
            if let Some(mut loan) = reader.claim(0).unwrap() {
                assert_eq!(*loan, got, "loan stream must be FIFO");
                loan.commit().unwrap();
                got += 1;
                consumed_producer.fetch_add(1, Ordering::Relaxed);
            } else {
                std::hint::spin_loop();
            }
        }
        got
    });

    producer.join().unwrap();
    assert_eq!(consumer.join().unwrap(), MESSAGES);
    assert_eq!(consumed.load(Ordering::Relaxed), MESSAGES as usize);
    assert_eq!(ring.total_written(), MESSAGES);
    assert_eq!(ring.slowest_lag(), 0);
}

#[test]
fn abort_during_active_producer_keeps_stream_consistent() {
    const MESSAGES: u64 = 2_000;
    let (_dir, ring) = temp_ring("loan_abort_stress.ring", 32);
    let reader = Arc::new(ring);
    let producer_ring = U64Ring::open_existing(reader.path()).unwrap();
    let consumer_reader = Arc::clone(&reader);

    let producer = std::thread::spawn(move || {
        let mut ring = producer_ring;
        for i in 0..MESSAGES {
            while !ring.try_push(&i) {
                std::hint::spin_loop();
            }
        }
    });

    let consumer = std::thread::spawn(move || {
        let reader = consumer_reader;
        let mut committed = 0u64;
        let mut aborted = 0u64;
        // Abort each index at most once, so every lap makes progress.
        let mut abort_armed = true;
        while committed < MESSAGES {
            if let Some(loan) = reader.claim(0).unwrap() {
                // The cursor only moves via commits, so every claim lands
                // exactly on the first uncommitted message.
                assert_eq!(loan.index(), committed, "claims are FIFO");
                if abort_armed && loan.index() % 3 == 0 {
                    loan.abort();
                    aborted += 1;
                    abort_armed = false;
                } else {
                    let mut loan = loan;
                    assert_eq!(*loan, committed, "value at the cursor");
                    loan.commit().unwrap();
                    committed += 1;
                    abort_armed = true;
                }
            } else {
                std::hint::spin_loop();
            }
        }
        (committed, aborted)
    });

    producer.join().unwrap();
    let (committed, aborted) = consumer.join().unwrap();
    assert_eq!(committed, MESSAGES);
    assert!(aborted > 0, "abort path exercised");
    assert_eq!(reader.slowest_lag(), 0, "all messages finally committed");
}
