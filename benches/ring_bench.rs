//! Criterion benchmarks for the ring's hot paths.
//!
//! Documented target: **< 100 ns per successful push** on a modern x86-64
//! core. `try_push` is: one Relaxed load, an N-wide Acquire min-scan, one
//! volatile store, one Release store, one Relaxed fetch_add — all
//! L1-resident when header and producer stay on one core.
//!
//! Run: `cargo bench`

use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use shm_rings::SpmcRingBuffer;
use tempfile::TempDir;

type U64Ring = SpmcRingBuffer<u64>;

/// Ring size for the streaming benches.
const CAP: usize = 1 << 16;
/// Slot capacity for the cold-fill bench (small: a fresh file per sample).
const FILL_CAP: usize = 4096;
/// Messages moved per benchmarked iteration in the streaming benches.
const MESSAGES: u64 = 10_000;

fn fresh_ring(dir: &TempDir, name: &str, readers: usize) -> U64Ring {
    U64Ring::create_new_with_readers(dir.path().join(name), CAP, readers).expect("create ring")
}

/// Push throughput with **no readers reading**: each sample fills a freshly
/// created, empty ring with exactly `FILL_CAP` successful pushes (the only
/// work possible without readers — backpressure halts the producer
/// afterwards). Setup (file creation + mmap) is excluded from timing.
fn bench_push_no_readers(c: &mut Criterion) {
    let mut group = c.benchmark_group("push");
    group.throughput(Throughput::Elements(FILL_CAP as u64));
    group.bench_function("fill_4096_no_readers", |b| {
        b.iter_batched(
            || {
                let dir = TempDir::new().expect("tempdir");
                U64Ring::create_new(dir.path().join("fill.ring"), FILL_CAP).expect("create")
            },
            |mut ring| {
                for i in 0..FILL_CAP as u64 {
                    black_box(ring.try_push(&i));
                }
            },
            BatchSize::PerIteration,
        )
    });
    group.finish();
}

/// Push+pop round-trip latency: one thread alternates a push and the
/// corresponding pop, so every message pays the full publish protocol
/// (volatile store + Release) *and* the full consume protocol (Acquire +
/// volatile load + Release cursor bump). The ring returns to its initial
/// state every iteration, so work scales linearly with criterion's iters.
fn bench_push_pop_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("push_pop");
    group.throughput(Throughput::Elements(MESSAGES));
    group.bench_function("pingpong_1_reader", |b| {
        let dir = TempDir::new().expect("tempdir");
        let mut ring = fresh_ring(&dir, "ping.ring", 1);
        b.iter(|| {
            for i in 0..MESSAGES {
                black_box(ring.try_push(&i));
                black_box(ring.try_pop(0).unwrap());
            }
        })
    });
    group.finish();
}

/// 4-reader fan-out: four consumer threads each drain the full stream while
/// the producer streams `iters` messages (exactly 4 participants are
/// provisioned, so backpressure tracks the real consumers). Timed region is
/// the producer's push loop only; consumer spawn/join is outside the clock.
fn bench_fanout_4_readers(c: &mut Criterion) {
    let mut group = c.benchmark_group("push_pop");
    group.throughput(Throughput::Elements(1));
    group.bench_function("fanout_4_readers", |b| {
        b.iter_custom(|iters| {
            let dir = TempDir::new().expect("tempdir");
            let ring = Arc::new(fresh_ring(&dir, "fan.ring", 4));
            let consumers: Vec<_> = (0..4usize)
                .map(|id| {
                    let ring = Arc::clone(&ring);
                    std::thread::spawn(move || {
                        let mut seen = 0u64;
                        while seen < iters {
                            if let Ok(Some(_)) = ring.try_pop(id) {
                                seen += 1;
                            }
                        }
                    })
                })
                .collect();
            let mut ring = U64Ring::open_existing(ring.path()).expect("producer handle");
            let start = Instant::now();
            for i in 0..iters {
                while !ring.try_push(&i) {
                    std::hint::spin_loop();
                }
            }
            let elapsed = start.elapsed();
            for consumer in consumers {
                consumer.join().expect("consumer thread");
            }
            elapsed
        })
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_push_no_readers,
    bench_push_pop_latency,
    bench_fanout_4_readers
);
criterion_main!(benches);
