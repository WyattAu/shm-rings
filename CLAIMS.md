# shm-rings: claims inventory
- **backed**; an existing bench/test asserts the claim; linked below.
- **proven**; unproven at survey time; a bench/test was added by this pass.
- **reworded**; claim adjusted to what the artifacts actually prove.
| Power-of-two masked indices; no modulo on the hot path | structural (`src/ring.rs` mask arithmetic) + `benches/iai_ring.rs::push_single` instruction count | backed |
| "The `try_push` path is byte-identical to 0.1.1" (0.2.0 release note) | **reworded**; historical release-comparison; not re-verifiable today. Going forward the iai instruction gate (`benches/iai_ring.rs`) pins the push path's shape so any change is visible in CI. |
- `benches/iai_ring.rs`; iai-callgrind instruction-count gate for
- `tests/zero_alloc_ring_ops.rs`; counting-allocator proof that