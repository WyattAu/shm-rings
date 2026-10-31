`Copy` messages; up to 8 independent consumers walk the stream concurrently -
- power-of-two masked indices; no modulo on the hot path
  `commit` / `abort` / drop; no byte leaves shared memory until you decide
  `push_notified`, consumers `pop_blocking`; kernel parking, no lost wakeups
| `Immutable` | consumers read through `&self` while the producer writes; no `UnsafeCell` |
    // 2. Read the mapped bytes in place; nothing was copied.
dropping a loan commits it if it is at the cursor; a drop can never corrupt
**fixed-layout record**: `#[repr(C)]` POD; no pointers, no `Drop`, no
- producer: `push_notified(&v, &notify)`; publish (`Release`) **then**
- consumer: `pop_blocking(reader_id, &notify)`; empty-check (`Acquire`)
`fork` or `SCM_RIGHTS`; the descriptor is the one handle you pass
  every byte written before it; including the volatile slot write; visible
  that matters is a single store→load pair on a single atomic; the exact
**v0.2; loans** re-use the observe protocol unchanged and defer the reclaim
**v0.2; notification** chains a third message-passing edge after publish:
the counter and consumed by (b); there is no window in which it can be
need no coordination: `try_pop(&self, reader_id)`; share one handle across
participants; `create_new_with_readers(path, capacity, n)` provisions `n` -
existence check that yields a typed `AlreadyExists`). The alternative -
`File::create` + re-init; is the classic shared-memory truncation footgun:
([iai-callgrind](https://github.com/iai-callgrind/iai-callgrind)); it
  extra atomic bookkeeping; use `try_pop` for small records, `claim` for
  `write` + one `poll`/`read` per handoff but uses roughly 3–6× less CPU -
fetch_add; all L1-resident single-core.
| miri    | design is miri-oriented (no transmutes, no ptr-to-int, atomics-only shared state); the syscall-backed file-mmap path is outside miri's model ("Miri does not support file-backed memory mappings"); safety is carried by the crate-level `# Safety` audit + the zero-unsafe loom double |