//! Fuzz the push/pop protocol against a `VecDeque`-style oracle.
//!
//! Invariant: whatever interleaving of Push/Pop/AdvanceNothing the input
//! encodes, a reader may only ever observe the exact value the oracle says
//! is next for it, and the ring must never panic, never let the slowest lag
//! exceed capacity, and never regress `total_written`.

#![no_main]

use libfuzzer_sys::fuzz_target;
use shm_rings::SpmcRingBuffer;
use tempfile::TempDir;

const CAPACITY: usize = 4;
const READERS: usize = 3;

fuzz_target!(|data: &[u8]| {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("fuzz.ring");
    let mut ring = SpmcRingBuffer::<u64>::create_new(&path, CAPACITY).expect("create");
    let reader = SpmcRingBuffer::<u64>::open_existing(&path).expect("open");

    let mut pushed: Vec<u64> = Vec::new();
    let mut next = [0u64; READERS];
    let mut prev_total = 0u64;

    let mut it = data.iter().copied();
    while let Some(op) = it.next() {
        match op % 4 {
            0 | 1 => {
                let v = u64::from(it.next().unwrap_or(0));
                if ring.try_push(&v) {
                    pushed.push(v);
                } else {
                    assert!(
                        ring.slowest_lag() <= CAPACITY as u64,
                        "lag {} exceeded capacity at backpressure",
                        ring.slowest_lag()
                    );
                }
            }
            2 => {
                let id = usize::from(it.next().unwrap_or(0)) % READERS;
                let got = reader
                    .try_pop(id)
                    .unwrap_or_else(|e| panic!("valid reader id {id} errored: {e}"));
                if (next[id] as usize) < pushed.len() {
                    assert_eq!(
                        got,
                        Some(pushed[next[id] as usize]),
                        "reader {id} FIFO violated"
                    );
                    next[id] += 1;
                } else {
                    assert!(got.is_none(), "reader {id} saw an unconsumed-but-overwritten or never-pushed value");
                }
            }
            _ => {} // AdvanceNothing
        }
        let total = ring.total_written();
        assert!(total >= prev_total, "total_written regressed");
        prev_total = total;
        assert!(
            ring.slowest_lag() <= CAPACITY as u64,
            "lag {} exceeded capacity",
            ring.slowest_lag()
        );
    }
});
