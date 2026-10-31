  for the hot paths; `push_single`, `push_batch_4096` (the shape behind
  steady-state `try_push` / `try_pop` / `claim` / `commit`; and the
  backpressure rejection path; perform zero heap allocations, as the
  reserves the next message as a `Loan<'_, T>`; a `Deref`-able view into
  `SpmcRingBuffer` struct, not the file; a mapping shared between handles
- Initial public release; lock-free SPMC shared-memory ring.