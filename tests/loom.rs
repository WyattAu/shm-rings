//! Loom model tests (run with `cargo test --features loom`).
//!
//! The models live in [`shm_rings::loom_ring`]: an in-memory double that
//! replaces the mmap+volatile plumbing with loom's tracked cells while
//! keeping the identical acquire/release ordering discipline, so loom can
//! exhaustively explore interleavings of the protocol itself.

#![cfg(feature = "loom")]

#[test]
fn loom_fanout_two_readers_no_overwrite_before_slowest_read() {
    shm_rings::loom_ring::model_fanout_two_readers();
}

#[test]
fn loom_backpressure_boundary_never_crossed_without_reader_advance() {
    shm_rings::loom_ring::model_backpressure_boundary();
}

#[test]
fn loom_loan_pins_cursor_against_producer_and_rejects_out_of_order_commit() {
    shm_rings::loom_ring::model_loan_pins_cursor_against_backpressure();
}

#[test]
fn loom_notify_handshake_woken_consumer_always_sees_the_message() {
    shm_rings::loom_ring::model_notify_no_lost_wakeup();
}
