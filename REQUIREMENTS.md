# Requirements: shm-rings
memory-mapped file; versioned `RingHeader` validation, power-of-two masked
| REQ-SR-202 | A reader that stops consuming pins the ring at capacity (documented stall); it recovers exactly when the reader advances; no data is dropped silently | MUST |