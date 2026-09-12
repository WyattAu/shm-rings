//! Integration tests for the eventfd notification path (feature `notify`,
//! Linux): real threads, real kernel sleeps — no busy-wait on either side.
//!
//! The no-lost-wakeup stress hammers the exact race the handshake exists
//! for: a consumer that decides to park while a producer is mid-publish.
// Test/bench code: unwrap/expect are the idiomatic way to assert outcomes.
#![cfg(all(feature = "notify", target_os = "linux"))]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use shm_rings::notify::EventNotify;
use shm_rings::SpmcRingBuffer;

type U64Ring = SpmcRingBuffer<u64>;

/// Single-consumer ring: one participating reader cursor, so backpressure
/// tracks exactly the consumer under test.
fn ring1(path: impl AsRef<std::path::Path>, capacity: usize) -> U64Ring {
    U64Ring::create_new_with_readers(path, capacity, 1).unwrap()
}

#[test]
fn eventfd_counter_accumulates_and_drains() {
    let notify = EventNotify::new().unwrap();
    assert!(
        notify.try_wait().unwrap().is_none(),
        "fresh eventfd is empty"
    );
    notify.signal().unwrap();
    notify.signal().unwrap();
    assert_eq!(notify.try_wait().unwrap(), Some(2), "units accumulate");
    assert!(notify.try_wait().unwrap().is_none(), "drained");
}

#[test]
fn eventfd_wait_timeout_returns_none_when_idle() {
    let notify = EventNotify::new().unwrap();
    let start = Instant::now();
    assert!(notify
        .wait_timeout(Some(Duration::from_millis(50)))
        .unwrap()
        .is_none());
    assert!(
        start.elapsed() >= Duration::from_millis(45),
        "must actually park, not spin"
    );
}

#[test]
fn pop_blocking_wakes_on_signal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nb_wake.ring");
    let mut ring = ring1(&path, 16);
    let notify = Arc::new(EventNotify::new().unwrap());

    let reader = U64Ring::open_existing(&path).unwrap();
    let r_notify = Arc::clone(&notify);
    let handle = std::thread::spawn(move || {
        // Parks in the kernel (ring empty); must wake on the push signal.
        reader.pop_blocking(0, &r_notify).unwrap()
    });

    std::thread::sleep(Duration::from_millis(50)); // let the consumer park
    ring.push_notified(&42, &notify).unwrap();
    assert_eq!(handle.join().unwrap(), Some(42));
}

#[test]
fn pop_blocking_timeout_returns_none_when_no_message() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nb_timeout.ring");
    let ring = ring1(&path, 16);
    let notify = EventNotify::new().unwrap();
    let start = Instant::now();
    let got = ring
        .pop_blocking_timeout(0, &notify, Duration::from_millis(50))
        .unwrap();
    assert_eq!(got, None);
    assert!(start.elapsed() >= Duration::from_millis(45));
}

#[test]
fn no_lost_wakeup_stress() {
    const MESSAGES: u64 = 4_000;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nb_stress.ring");
    let mut ring = ring1(&path, 64);
    let notify = Arc::new(EventNotify::new().unwrap());

    let reader = U64Ring::open_existing(&path).unwrap();
    let r_notify = Arc::clone(&notify);
    let consumer = std::thread::spawn(move || {
        let mut seen = 0u64;
        while seen < MESSAGES {
            // pop_blocking re-checks after every wakeup; a missed wakeup
            // would hang here until the deadline and fail the test.
            let v = reader
                .pop_blocking_timeout(0, &r_notify, Duration::from_secs(20))
                .unwrap()
                .expect("no lost wakeups within the deadline");
            assert_eq!(v, seen, "FIFO under notification");
            seen += 1;
        }
        seen
    });

    for i in 0..MESSAGES {
        while !ring.push_notified(&i, &notify).unwrap() {
            // Backpressured: the consumer will drain; retry.
            std::thread::sleep(Duration::from_micros(50));
        }
        // Vary the race window: sometimes signal immediately after the
        // next push, sometimes stall between (consumer parks mid-stream).
        if i % 97 == 0 {
            std::thread::sleep(Duration::from_micros(200));
        }
    }

    assert_eq!(consumer.join().unwrap(), MESSAGES);
    // Note: leftover eventfd units are legal and expected here. The counter
    // is an accumulating semaphore: every push leaves one unit, and a
    // consumer that receives messages via direct hits (empty-check passing
    // without parking) never drains them. The property under test is that
    // no consumer park ever misses its wakeup — proven by the join above
    // completing within the per-message deadline.
}

#[test]
fn pingpong_handoff_round_trips() {
    const ROUNDS: u64 = 500;
    let dir = tempfile::tempdir().unwrap();
    let req_path = dir.path().join("nb_req.ring");
    let ack_path = dir.path().join("nb_ack.ring");
    let mut req = ring1(&req_path, 8);
    let ack = ring1(&ack_path, 8);
    let req_notify = Arc::new(EventNotify::new().unwrap());
    let ack_notify = Arc::new(EventNotify::new().unwrap());
    let resp_req = Arc::clone(&req_notify);
    let resp_ack = Arc::clone(&ack_notify);

    let responder = std::thread::spawn(move || {
        let reader = U64Ring::open_existing(&req_path).unwrap();
        let mut writer = U64Ring::open_existing(&ack_path).unwrap();
        let in_notify = resp_req;
        let out_notify = resp_ack;
        for round in 0..ROUNDS {
            let v = reader
                .pop_blocking(0, &in_notify)
                .unwrap()
                .expect("request");
            assert_eq!(v, round);
            while !writer.push_notified(&(v * 2), &out_notify).unwrap() {
                std::thread::sleep(Duration::from_micros(50));
            }
        }
    });

    for round in 0..ROUNDS {
        req.push_notified(&round, &req_notify).unwrap();
        let a = ack
            .pop_blocking_timeout(0, &ack_notify, Duration::from_secs(5))
            .unwrap()
            .expect("ack within deadline");
        assert_eq!(a, round * 2);
    }
    responder.join().unwrap();
}

#[test]
fn push_notified_reports_backpressure_without_signaling() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nb_backpressure.ring");
    let mut ring = ring1(&path, 2);
    let notify = EventNotify::new().unwrap();

    assert!(ring.push_notified(&1, &notify).unwrap());
    assert!(ring.push_notified(&2, &notify).unwrap());
    // Full ring: nothing published, nothing signaled.
    assert!(!ring.push_notified(&3, &notify).unwrap());
    assert_eq!(notify.try_wait().unwrap(), Some(2), "exactly two signals");
}
