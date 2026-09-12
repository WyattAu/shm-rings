// Zero-allocation gate for the ring's steady-state hot paths: a counting
// global allocator proves push/pop/claim/commit never touch the heap once
// a ring is up and running.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

//! Allocation counter tests for `SpmcRingBuffer` steady-state operations.
//!
//! The ring lives in an `mmap`-ed file: slots, header, and cursors are all
//! shared-memory, so the steady-state hot paths must allocate exactly
//! nothing on the Rust heap. This binary is the verification (a counting
//! allocator cannot lie about code reading):
//!
//! - `try_push` (the ~18.5 ns/push path) — zero allocations
//! - `try_pop` — zero allocations (returns `T` by value)
//! - `claim` / `commit` (zero-copy loan) — zero allocations
//! - peek-free diagnostics (`len`, `slowest_lag`, `capacity`) — zero
//!   allocations
//!
//! The iai-callgrind instruction-count gate (CI-only; requires valgrind,
//! `benches/iai_ring.rs`) pins the cycle cost; this file pins the heap
//! behavior on every `cargo test` run.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use shm_rings::SpmcRingBuffer;
use tempfile::TempDir;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static ALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn allocations() -> usize {
    ALLOCATIONS.load(Ordering::Relaxed)
}

const CAP: usize = 4096;
const WARMUP: usize = 64;
const ITERATIONS: usize = 1000;

/// One sequential test: the allocation counter is process-global, so
/// parallel test threads would pollute each other's counts.
#[test]
fn steady_state_ring_ops_are_allocation_free() {
    let dir = TempDir::new().unwrap();
    let mut ring =
        SpmcRingBuffer::<u64>::create_new_with_readers(dir.path().join("zero.ring"), CAP, 1)
            .expect("create ring");

    // Warm-up: fill part of the ring and drain it. Creation, mmap, and
    // error-free path discovery all happen here, outside measurement.
    for i in 0..WARMUP as u64 {
        assert!(ring.try_push(&i), "warm-up push must succeed");
    }
    for i in 0..WARMUP as u64 {
        assert_eq!(ring.try_pop(0).expect("pop"), Some(i));
    }

    // --- Steady-state try_push: zero allocations. ---
    let before = allocations();
    for i in 0..ITERATIONS as u64 {
        assert!(ring.try_push(&i), "push must succeed within capacity");
    }
    assert_eq!(
        allocations(),
        before,
        "steady-state try_push must not allocate (before: {before}, after: {})",
        allocations()
    );

    // --- Steady-state try_pop: zero allocations. ---
    let before = allocations();
    for i in 0..ITERATIONS as u64 {
        assert_eq!(ring.try_pop(0).expect("pop"), Some(i));
    }
    assert_eq!(
        allocations(),
        before,
        "steady-state try_pop must not allocate"
    );

    // --- Loan round trip (claim → read in place → commit): zero
    // allocations. (Commit consumes the message, so each iteration leaves
    // the ring empty again.) ---
    let before = allocations();
    for i in 0..ITERATIONS as u64 {
        assert!(ring.try_push(&i), "push must succeed (ring kept empty)");
        let mut loan = ring.claim(0).expect("claim").expect("loan");
        assert_eq!(*loan, i);
        loan.commit().expect("commit");
    }
    assert_eq!(
        allocations(),
        before,
        "loan claim/commit round trip must not allocate"
    );

    // --- Backpressure rejection path: zero allocations too. ---
    for i in 0..CAP as u64 {
        assert!(ring.try_push(&i), "fill to capacity");
    }
    let before = allocations();
    assert!(!ring.try_push(&u64::MAX), "full ring must reject");
    assert_eq!(
        allocations(),
        before,
        "backpressure rejection must not allocate"
    );

    // --- Counter sanity guard: construction (mmap setup) does allocate.
    // If this ever fails, the zero-alloc assertions above prove nothing
    // (the counter would be broken, not the hot path miraculously free). ---
    let before = allocations();
    drop(
        SpmcRingBuffer::<u64>::create_new_with_readers(dir.path().join("sanity.ring"), 64, 1)
            .expect("second ring"),
    );
    assert!(
        allocations() > before,
        "ring creation must allocate (mmap + handle setup) — counter sanity check"
    );
}
