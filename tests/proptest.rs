//! Model-based property test: a `VecDeque` oracle drives an arbitrary
//! operation sequence and checks the ring's invariants after every step.

use proptest::prelude::*;
use shm_rings::SpmcRingBuffer;

/// Fixed small capacity keeps backpressure events frequent.
const CAPACITY: usize = 4;
/// Readers exercised by the model (0..READERS).
const READERS: usize = 2;

#[derive(Debug, Clone)]
enum Op {
    /// Push a byte value.
    Push(u8),
    /// Pop as reader 0..READERS.
    Pop(usize),
    /// Do nothing (varies sequence shape without changing state).
    AdvanceNothing,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (any::<u8>()).prop_map(Op::Push),
        3 => (0..READERS).prop_map(Op::Pop),
        1 => Just(Op::AdvanceNothing),
    ]
}

#[test]
fn model_based_ops_match_vecdeque_oracle() {
    let config = ProptestConfig::with_cases(500);
    proptest!(config, |(ops in proptest::collection::vec(op_strategy(), 0..64))| {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prop.ring");
        let mut ring = SpmcRingBuffer::<u64>::create_new(&path, CAPACITY).unwrap();
        let reader = SpmcRingBuffer::<u64>::open_existing(&path).unwrap();

        // Oracle state.
        let mut pushed: Vec<u64> = Vec::new();
        let mut next: [u64; READERS] = [0; READERS]; // per-reader consumed count
        let mut prev_total: u64 = 0;

        for op in &ops {
            match op {
                Op::Push(b) => {
                    let v = u64::from(*b);
                    if ring.try_push(&v) {
                        pushed.push(v);
                    } else {
                        // Backpressure must fire exactly at the boundary.
                        prop_assert_eq!(ring.slowest_lag(), CAPACITY as u64);
                    }
                }
                Op::Pop(id) => {
                    let got = ring.try_pop(*id).unwrap();
                    if (next[*id] as usize) < pushed.len() {
                        // Core safety property: this reader's cursor still
                        // points at a live slot, so it must observe exactly
                        // the oracle's next value — never a stale, torn, or
                        // lapped one.
                        prop_assert_eq!(got, Some(pushed[next[*id] as usize]));
                        next[*id] += 1;
                    } else {
                        prop_assert_eq!(
                            got,
                            None,
                            "reader {} saw a value the oracle never pushed",
                            id
                        );
                    }
                }
                Op::AdvanceNothing => {}
            }

            // Invariant: total_written is monotone.
            let total = ring.total_written();
            prop_assert!(total >= prev_total, "total_written regressed");
            prev_total = total;

            // Invariant: the producer is never allowed to outrun the
            // slowest reader by more than the capacity.
            prop_assert!(
                ring.slowest_lag() <= CAPACITY as u64,
                "slowest_lag {} exceeded capacity {CAPACITY}",
                ring.slowest_lag()
            );
        }

        // Final per-reader FIFO check over ALL MAX_READERS cursors: readers
        // 0..READERS were driven by ops, the rest sat at 0 and must observe
        // the entire stream. Drain every reader fully.
        let starts: Vec<u64> = (0..shm_rings::MAX_READERS)
            .map(|i| next.get(i).copied().unwrap_or(0))
            .collect();
        for (id, start) in starts.into_iter().enumerate() {
            let mut consumed = start;
            while let Some(v) = reader.try_pop(id).unwrap() {
                prop_assert!(
                    (consumed as usize) < pushed.len(),
                    "reader {} consumed more than was pushed",
                    id
                );
                prop_assert_eq!(
                    v,
                    pushed[consumed as usize],
                    "reader {} FIFO violated at tail",
                    id
                );
                consumed += 1;
            }
        }
    });
}

#[test]
fn generated_sequences_have_useful_shape() {
    // Sanity: the strategy actually produces all op kinds within the
    // documented size bounds.
    proptest!(|(ops in proptest::collection::vec(op_strategy(), 16..64))| {
        let pushes = ops.iter().filter(|o| matches!(o, Op::Push(_))).count();
        let pops = ops.iter().filter(|o| matches!(o, Op::Pop(_))).count();
        prop_assert!(pushes + pops > 0);
        prop_assert!(ops.len() < 64);
    });
}
