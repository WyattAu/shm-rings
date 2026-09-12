//! Criterion benchmarks for the zero-copy loan path vs the copy path.
//!
//! Expectation documented in REQUIREMENTS: on large fixed-layout records
//! the loan (claim → read in place → commit) should beat the copy
//! (`try_pop`) by roughly 2×, since it skips the `size_of::<T>()` byte
//! move. The push path is identical in both variants, so each pair also
//! isolates the consume-path delta.
//!
//! Run: `cargo bench --bench loan_bench`
// Test/bench code: unwrap/expect are the idiomatic way to assert outcomes.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use shm_rings::SpmcRingBuffer;
use tempfile::TempDir;

/// Messages moved per benchmarked iteration.
const MESSAGES: u64 = 10_000;
/// 1 KiB fixed-layout record: big enough that the copy dominates.
type BigRecord = [u64; 128];

/// Copy-path round trip: `try_push` + `try_pop` per message (u64).
fn bench_copy_u64(c: &mut Criterion) {
    let mut group = c.benchmark_group("consume");
    group.throughput(Throughput::Elements(MESSAGES));
    group.bench_function("copy_u64", |b| {
        let dir = TempDir::new().unwrap();
        let mut ring =
            SpmcRingBuffer::<u64>::create_new_with_readers(dir.path().join("copy64.ring"), 4096, 1)
                .unwrap();
        b.iter(|| {
            for i in 0..MESSAGES {
                black_box(ring.try_push(&i));
                black_box(ring.try_pop(0).unwrap());
            }
        })
    });
    group.finish();
}

/// Loan-path round trip: `try_push` + `claim` + read in place + `commit`.
fn bench_loan_u64(c: &mut Criterion) {
    let mut group = c.benchmark_group("consume");
    group.throughput(Throughput::Elements(MESSAGES));
    group.bench_function("loan_u64", |b| {
        let dir = TempDir::new().unwrap();
        let mut ring =
            SpmcRingBuffer::<u64>::create_new_with_readers(dir.path().join("loan64.ring"), 4096, 1)
                .unwrap();
        b.iter(|| {
            for i in 0..MESSAGES {
                black_box(ring.try_push(&i));
                let mut loan = ring.claim(0).unwrap().unwrap();
                black_box(*loan);
                loan.commit().unwrap();
            }
        })
    });
    group.finish();
}

/// Copy-path round trip with 1 KiB records.
fn bench_copy_1kib(c: &mut Criterion) {
    let mut group = c.benchmark_group("consume_1kib");
    group.throughput(Throughput::Elements(MESSAGES));
    group.bench_function("copy_1kib_record", |b| {
        let dir = TempDir::new().unwrap();
        let mut ring = SpmcRingBuffer::<BigRecord>::create_new_with_readers(
            dir.path().join("copy1k.ring"),
            1024,
            1,
        )
        .unwrap();
        let record: BigRecord = std::array::from_fn(|i| i as u64);
        b.iter(|| {
            for i in 0..MESSAGES {
                black_box(ring.try_push(&record));
                let out = ring.try_pop(0).unwrap();
                black_box(out.map(|r| r[0] + i));
            }
        })
    });
    group.finish();
}

/// Loan-path round trip with 1 KiB records: read a few fields in place.
fn bench_loan_1kib(c: &mut Criterion) {
    let mut group = c.benchmark_group("consume_1kib");
    group.throughput(Throughput::Elements(MESSAGES));
    group.bench_function("loan_1kib_record", |b| {
        let dir = TempDir::new().unwrap();
        let mut ring = SpmcRingBuffer::<BigRecord>::create_new_with_readers(
            dir.path().join("loan1k.ring"),
            1024,
            1,
        )
        .unwrap();
        let record: BigRecord = std::array::from_fn(|i| i as u64);
        b.iter(|| {
            for i in 0..MESSAGES {
                black_box(ring.try_push(&record));
                let mut loan = ring.claim(0).unwrap().unwrap();
                // Typical consumer: read the fields it needs, in place.
                black_box(loan[0] + loan[127] + i);
                loan.commit().unwrap();
            }
        })
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_copy_u64,
    bench_loan_u64,
    bench_copy_1kib,
    bench_loan_1kib
);
criterion_main!(benches);
