//! Criterion benchmarks: notified consume vs spin consume on a 2-thread
//! ping-pong, with wall-clock latency (criterion) and CPU utilization
//! (utime+stime deltas from /proc/self/stat, Linux).
//!
//! Handoff topology per sample (strict alternation, one message in flight):
//!
//! ```text
//! main thread ──req ring──▶ responder thread
//!      ▲──ack ring─────────────┘   (each side signals its eventfd)
//! ```
//!
//! The spin variant drains `try_pop` in a `spin_loop`; the notified variant
//! parks in the eventfd via `pop_blocking`. Latency is the full round trip
//! (req + ack); CPU utilization is (process CPU seconds delta / wall
//! seconds delta) across the whole sample — the number that shows *why*
//! blocking beats spinning for idle-heavy traffic.
//!
//! Run: `cargo bench --bench notify_bench --features notify`
// Test/bench code: unwrap/expect are the idiomatic way to assert outcomes.
#![allow(clippy::unwrap_used, clippy::expect_used)]
#![cfg(target_os = "linux")]

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, Criterion};
use shm_rings::notify::EventNotify;
use shm_rings::SpmcRingBuffer;
use tempfile::TempDir;

type U64Ring = SpmcRingBuffer<u64>;

/// Round trips per sample.
const ROUNDS: u64 = 20_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Spin,
    Notified,
}

/// utime+stime in clock ticks, read from /proc/self/stat (Linux).
fn cpu_ticks() -> u64 {
    let stat = std::fs::read_to_string("/proc/self/stat").expect("/proc/self/stat");
    let close = stat.rfind(')').expect("comm field");
    let rest = stat[close + 2..].split_whitespace().collect::<Vec<_>>();
    // rest[0] is `state` (field 3); utime is field 14 → rest[11];
    // stime is field 15 → rest[12].
    let utime: u64 = rest[11].parse().expect("utime");
    let stime: u64 = rest[12].parse().expect("stime");
    utime + stime
}

/// _SC_CLK_TCK on Linux (100 on every supported kernel configuration).
const CLK_TCK: f64 = 100.0;

/// One full ping-pong measurement: builds rings, runs `mode` for `ROUNDS`
/// strict alternations, returns (wall elapsed, cpu ticks consumed).
fn measure(mode: Mode) -> (Duration, u64) {
    let dir = TempDir::new().unwrap();
    let req_path = dir.path().join("req.ring");
    let ack_path = dir.path().join("ack.ring");
    let mut req = U64Ring::create_new_with_readers(&req_path, 64, 1).unwrap();
    let ack = U64Ring::create_new_with_readers(&ack_path, 64, 1).unwrap();
    let req_notify = Arc::new(EventNotify::new().unwrap());
    let ack_notify = Arc::new(EventNotify::new().unwrap());
    let resp_req = Arc::clone(&req_notify);
    let resp_ack = Arc::clone(&ack_notify);

    let responder = std::thread::spawn(move || {
        let reader = U64Ring::open_existing(&req_path).unwrap();
        let mut writer = U64Ring::open_existing(&ack_path).unwrap();
        let req_notify = resp_req;
        let ack_notify = resp_ack;
        for round in 0..ROUNDS {
            match mode {
                Mode::Spin => {
                    while reader.try_pop(0).unwrap() != Some(round) {
                        std::hint::spin_loop();
                    }
                }
                Mode::Notified => {
                    black_box(reader.pop_blocking(0, &req_notify).unwrap());
                }
            }
            while !writer.try_push(&round) {
                std::hint::spin_loop();
            }
            if mode == Mode::Notified {
                ack_notify.signal().unwrap();
            }
        }
    });

    let cpu0 = cpu_ticks();
    let start = Instant::now();
    for round in 0..ROUNDS {
        while !req.try_push(&round) {
            std::hint::spin_loop();
        }
        if mode == Mode::Notified {
            req_notify.signal().unwrap();
        }
        match mode {
            Mode::Spin => {
                while ack.try_pop(0).unwrap() != Some(round) {
                    std::hint::spin_loop();
                }
            }
            Mode::Notified => {
                black_box(
                    ack.pop_blocking_timeout(0, &ack_notify, Duration::from_secs(10))
                        .unwrap()
                        .expect("ack within deadline"),
                );
            }
        }
    }
    let elapsed = start.elapsed();
    let cpu = cpu_ticks() - cpu0;
    responder.join().unwrap();
    (elapsed, cpu)
}

fn bench_pingpong(c: &mut Criterion) {
    let mut group = c.benchmark_group("pingpong_2threads");
    for (name, mode) in [("spin", Mode::Spin), ("notified", Mode::Notified)] {
        group.bench_function(name, |b| {
            b.iter_custom(|iters| {
                let (wall, ticks) = measure(mode);
                let cpu_secs = ticks as f64 / CLK_TCK;
                eprintln!(
                    "[{name}] {ROUNDS} round trips: wall {:>10?}  ({:>6.1} ns/rt)  cpu {:.1}%  ({:.0} ns cpu/rt)",
                    wall,
                    wall.as_nanos() as f64 / ROUNDS as f64,
                    100.0 * cpu_secs / wall.as_secs_f64(),
                    cpu_secs * 1e9 / ROUNDS as f64,
                );
                wall.mul_f64(iters as f64 / ROUNDS as f64)
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_pingpong);
criterion_main!(benches);
