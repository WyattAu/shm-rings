// iai-callgrind benchmarks run once under Valgrind on fixed inputs; the
// harness measures instruction counts, so there is no "expected failure"
// recovery path — a panic aborts the run visibly, which is what we want.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

//! Deterministic regression gate for the ring's hot paths.
//!
//! Unlike criterion (wall-clock, noisy, human-readable trend —
//! `benches/ring_bench.rs` / `benches/loan_bench.rs`), iai-callgrind
//! counts CPU instructions under Valgrind and is reproducible for a given
//! binary — fit for a CI gate. Criterion stays the source of the
//! wall-clock trend (the ~18.5 ns/push README number); this file pins the
//! instruction count of the push path so a regression fails CI even when
//! a noisy runner hides it in the wall clock. `tests/zero_alloc_ring_ops.rs`
//! pins the heap behavior (steady-state ring ops allocate nothing).
//!
//! Paths pinned:
//!
//! - `push_single` — one successful `try_push` on an empty ring
//! - `push_batch_4096` — the cold-fill shape of
//!   `ring_bench::push/fill_4096_no_readers`, amortized per-push
//! - `pop_single` — one `try_pop` of a pending message
//! - `loan_claim_commit` — one `claim` → read → `commit` round trip

use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use shm_rings::SpmcRingBuffer;
use tempfile::TempDir;

type U64Ring = SpmcRingBuffer<u64>;

const CAP: usize = 1 << 13;

fn temp() -> TempDir {
    TempDir::new().unwrap()
}

fn setup_empty_ring() -> (TempDir, U64Ring) {
    let dir = temp();
    let ring =
        U64Ring::create_new_with_readers(dir.path().join("iai.ring"), CAP, 1).expect("create ring");
    (dir, ring)
}

fn setup_one_message() -> (TempDir, U64Ring) {
    let (dir, mut ring) = setup_empty_ring();
    assert!(ring.try_push(&42));
    (dir, ring)
}

// One successful try_push on an empty ring: the full publish protocol
// (relaxed load, min-scan, volatile slot write, release store, relaxed
// fetch_add). This is the path behind the ~18.5 ns/push README number.
#[library_benchmark]
#[bench::single(setup = setup_empty_ring)]
fn push_single(env: (TempDir, U64Ring)) -> bool {
    let (_dir, mut ring) = env;
    ring.try_push(&42)
}

// Batched cold fill, 4096 pushes with no readers (same shape as
// `ring_bench::push/fill_4096_no_readers`); iai reports per-batch totals
// and the per-push mean is the gateable quantity.
#[library_benchmark]
#[bench::batch_4096(setup = setup_empty_ring)]
fn push_batch_4096(env: (TempDir, U64Ring)) -> u64 {
    let (_dir, mut ring) = env;
    let mut pushed = 0u64;
    for i in 0..4096u64 {
        if ring.try_push(&i) {
            pushed += 1;
        }
    }
    pushed
}

// One try_pop of a pending message: acquire publish-edge load, volatile
// slot read, release cursor bump.
#[library_benchmark]
#[bench::single(setup = setup_one_message)]
fn pop_single(env: (TempDir, U64Ring)) -> Option<u64> {
    let (_dir, ring) = env;
    ring.try_pop(0).expect("pop")
}

// Loan round trip: claim (acquire load + loan gate) → read in place →
// commit (release cursor bump). The zero-copy consume path.
#[library_benchmark]
#[bench::claim_commit(setup = setup_one_message)]
fn loan_claim_commit(env: (TempDir, U64Ring)) -> u64 {
    let (_dir, ring) = env;
    let mut loan = ring.claim(0).expect("claim").expect("loan");
    let value = *loan;
    loan.commit().expect("commit");
    value
}

library_benchmark_group!(
    name = iai_ring_hot_path;
    benchmarks = push_single, push_batch_4096, pop_single, loan_claim_commit
);

main!(library_benchmark_groups = iai_ring_hot_path);
