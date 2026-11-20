# PATTERN_ADOPTION: shm-rings

Relationship to the WyattAu pattern canon. Statuses follow the closed
vocabulary: adopted, partial, planned, refused, n/a. Evidence paths are
relative to this repo.

| Pattern | Status | Evidence |
|---|---|---|
| T20 | adopted | src/ring.rs:149 and src/ring.rs:176 (fixed-capacity single-producer ring); try_push takes the writer through a unique borrow at src/ring.rs:443, so the borrow checker enforces the single-writer invariant instead of a lock |
| T16 | adopted | Backpressure-only capacity: the producer stalls when the slowest reader lags by a full capacity (src/ring.rs:36) and try_push reports refusal as a bool (src/ring.rs:443); there is no overwrite mode, so slow consumers cannot force unbounded buffering or data loss |
