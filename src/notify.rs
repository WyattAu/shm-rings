//! Cross-thread / cross-process wakeup: Linux `eventfd` (feature `notify`).
//!
//! The ring is deliberately polling-only: `try_pop` returns `None` on an
//! empty ring and the caller decides what to do. This module adds the
//! efficient *block* — an [`EventNotify`] wrapping a Linux
//! [`eventfd(2)`](https://man7.org/linux/man-pages/man2/eventfd.2.html)
//! counter — plus two handshake helpers on the ring:
//!
//! * producer: [`SpmcRingBuffer::push_notified`] — publish (`Release`) then
//!   signal the eventfd;
//! * consumer: [`SpmcRingBuffer::pop_blocking`] — empty-check (`Acquire`)
//!   then block on the eventfd, re-checking after each wakeup.
//!
//! # Wakeup topology
//!
//! ```text
//! producer ──try_push──▶ [ ring file / mmap ] ──try_pop──▶ consumer
//!     │                                                    ▲
//!     └── signal(): write(eventfd) ────────── wait(): read(eventfd)
//! ```
//!
//! One `EventNotify` serves any number of consumers: eventfd is an
//! accumulating counter, so N signals wake up to N pending `wait`s (each
//! `read` consumes one unit). Ring it with one producer and one semaphore
//! eventfd per consumer group for strict per-consumer wakeups.
//!
//! # No lost wakeups
//!
//! The handshake is the classic check-then-wait pattern with the ring's own
//! ordering discipline (see the ring module docs, "notification handshake"):
//! the producer signals *after* the `Release` publish; the consumer checks
//! emptiness (`Acquire`) *before* blocking. Between check and block, a
//! landing signal is accumulated by the eventfd counter and consumed by the
//! subsequent `read` — it cannot be missed, because `read` never compares a
//! value, unlike a futex wait.
//!
//! # Sharing across processes
//!
//! An eventfd is an anonymous inode: threads share it naturally through
//! `&EventNotify`/`Arc`, and separate processes must inherit or pass the
//! descriptor (fork, or `SCM_RIGHTS` over a Unix socket). The ring file
//! itself stays the only path-addressed artifact; the descriptor is the one
//! handle users distribute out-of-band. This is the same model iceoryx2
//! uses for its waitsets (fd-based, not path-based).
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use shm_rings::{SpmcRingBuffer, notify::EventNotify};
//!
//! # fn main() -> Result<(), shm_rings::ShmRingError> {
//! let notify = Arc::new(EventNotify::new().map_err(shm_rings::ShmRingError::Io)?);
//! let mut ring = SpmcRingBuffer::<u64>::create_new("/dev/shm/demo.ring", 1024)?;
//!
//! let consumer_ring = SpmcRingBuffer::<u64>::open_existing(ring.path())?;
//! let consumer_notify = Arc::clone(&notify);
//! let handle = std::thread::spawn(move || {
//!     // Blocks on the eventfd while the ring is empty; never busy-waits.
//!     consumer_ring.pop_blocking(0, &consumer_notify).unwrap()
//! });
//!
//! ring.push_notified(&42, &notify)?;
//! assert_eq!(handle.join().unwrap(), Some(42));
//! # Ok(())
//! # }
//! ```

use std::fmt;
use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::time::Duration;

use zerocopy::{FromBytes, Immutable};

use crate::error::ShmRingError;
use crate::ring::SpmcRingBuffer;

/// An `eventfd(2)` counter used to wake blocked consumers.
///
/// Create one per producer→consumer-group edge; signal it after publishing
/// ([`SpmcRingBuffer::push_notified`]), wait on it when the ring looks empty
/// ([`SpmcRingBuffer::pop_blocking`]).
pub struct EventNotify {
    fd: OwnedFd,
}

// SAFETY: the eventfd descriptor is shared deliberately; `signal` and `wait`
// go through the kernel (read/write/poll), which is atomic with respect to
// itself, so concurrent signal/wait from any number of threads is sound and
// intended. No Rust-level interior mutability exists.
unsafe impl Send for EventNotify {}
// SAFETY: see `Send`; kernel-side synchronization covers all shared access.
unsafe impl Sync for EventNotify {}

impl EventNotify {
    /// Creates a fresh eventfd (`EFD_CLOEXEC | EFD_NONBLOCK`, counter 0).
    ///
    /// Non-blocking is the descriptor's standing mode: [`Self::wait`] blocks
    /// in `poll(2)` (which also wakes on fd closure) and performs a
    /// non-blocking read once readable, so a signal that lands between poll
    /// and read is consumed instead of hanging.
    pub fn new() -> io::Result<Self> {
        // eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK); flags are libc constants
        // (EFD_CLOEXEC = 0o2000000, EFD_NONBLOCK = 0o4000 on Linux).
        // SAFETY: `eventfd` is a plain fd-creating syscall; it takes no
        // pointers and has no aliasing preconditions. A negative return is
        // handled below via `last_os_error`.
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            // SAFETY: `fd` is a freshly created, uniquely owned descriptor
            // (or we returned the error above).
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
        })
    }

    /// Signals one wakeup unit (counter += 1).
    ///
    /// `EAGAIN` (counter momentarily full at `u64::MAX - 1`) is mapped to
    /// `Ok(())`: a full counter means overwhelmingly many wakeups are
    /// already pending, which is the semantic a signal wants.
    pub fn signal(&self) -> io::Result<()> {
        let one: u64 = 1;
        // SAFETY: `write` on our own valid descriptor; the buffer is a
        // live, initialized, 8-byte local (eventfd requires exactly 8)
        // and is not concurrently mutated for the call's duration.
        let written = unsafe {
            libc::write(
                self.fd.as_raw_fd(),
                std::ptr::from_ref(&one).cast(),
                std::mem::size_of::<u64>(),
            )
        };
        if written < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                return Ok(());
            }
            return Err(err);
        }
        Ok(())
    }

    /// Blocks until at least one wakeup unit is pending, then consumes all
    /// pending units. Returns the number of units consumed (≥ 1).
    pub fn wait(&self) -> io::Result<u64> {
        Ok(self.wait_timeout(None)?.unwrap_or(1))
    }

    /// [`Self::wait`] with a deadline: `Ok(None)` on timeout, `Ok(Some(n))`
    /// when `n` wakeup units arrived in time.
    pub fn wait_timeout(&self, timeout: Option<Duration>) -> io::Result<Option<u64>> {
        loop {
            // 1. Fast path: drain any units already accumulated. This is
            //    what makes the handshake lossless — a signal that landed
            //    between the consumer's empty-check and this call is
            //    consumed here.
            if let Some(n) = self.try_wait()? {
                return Ok(Some(n));
            }
            // 2. Nothing pending: block in poll until readable (or timeout).
            let mut pfd = libc::pollfd {
                fd: self.fd.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            let timeout_ms = match timeout {
                None => -1,
                Some(d) => {
                    let ms = d.as_millis();
                    i32::try_from(ms).unwrap_or(i32::MAX)
                }
            };
            // SAFETY: `poll` on our own valid descriptor; the single-element
            // `pollfd` array is a live local for the call's duration.
            let ready = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
            if ready < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(err);
            }
            if ready == 0 {
                return Ok(None); // timeout with nothing pending
            }
            // 3. Readable: loop back through the drain; the read cannot
            //    block (non-blocking descriptor) and the unit cannot vanish
            //    (only readers consume).
        }
    }

    /// Non-blocking drain: `Some(units)` if wakeups were pending, else
    /// `None`.
    pub fn try_wait(&self) -> io::Result<Option<u64>> {
        let mut count: u64 = 0;
        // SAFETY: `read` on our own valid descriptor (opened non-blocking);
        // the buffer is a live, initialized, 8-byte local (eventfd requires
        // exactly 8) writable for the call's duration.
        let n = unsafe {
            libc::read(
                self.fd.as_raw_fd(),
                std::ptr::from_mut(&mut count).cast(),
                std::mem::size_of::<u64>(),
            )
        };
        if n < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                return Ok(None);
            }
            return Err(err);
        }
        debug_assert_eq!(n, 8, "eventfd reads are exactly 8 bytes");
        Ok(Some(count))
    }

    /// Borrowed view of the underlying descriptor (for poll/epoll integration).
    pub fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

impl AsRawFd for EventNotify {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

impl fmt::Debug for EventNotify {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventNotify")
            .field("fd", &self.fd.as_raw_fd())
            .finish()
    }
}

impl<T: Copy + FromBytes + Immutable> SpmcRingBuffer<T> {
    /// Publishes `value` and, on success, signals one wakeup unit.
    ///
    /// This is the producer half of the handshake: the `Release` publish of
    /// [`Self::try_push`] is ordered *before* the eventfd write, so a
    /// consumer that wakes can never fail to see the message it was woken
    /// for. Returns `Ok(false)` under backpressure (nothing published,
    /// nothing signaled).
    pub fn push_notified(&mut self, value: &T, notify: &EventNotify) -> Result<bool, ShmRingError> {
        if self.try_push(value) {
            notify.signal().map_err(ShmRingError::Io)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// [`Self::try_pop`], blocking on `notify` while the ring is empty.
    ///
    /// Check-then-wait loop: empty-check (`Acquire`), block in the kernel,
    /// re-check. Returns the first message available for `reader_id`; wakes
    /// are cheap when several messages accumulated (the loop drains without
    /// re-blocking). Never spin-waits: every empty pass parks in
    /// `poll(2)`.
    pub fn pop_blocking(
        &self,
        reader_id: usize,
        notify: &EventNotify,
    ) -> Result<Option<T>, ShmRingError> {
        loop {
            if let Some(v) = self.try_pop(reader_id)? {
                return Ok(Some(v));
            }
            notify.wait().map_err(ShmRingError::Io)?;
            // Loop: re-run the empty check — wakeup units may have
            // accumulated past the message we are about to take.
        }
    }

    /// [`Self::pop_blocking`] with a deadline: `Ok(None)` if `reader_id` had
    /// no message within `timeout`.
    pub fn pop_blocking_timeout(
        &self,
        reader_id: usize,
        notify: &EventNotify,
        timeout: Duration,
    ) -> Result<Option<T>, ShmRingError> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if let Some(v) = self.try_pop(reader_id)? {
                return Ok(Some(v));
            }
            let now = std::time::Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            match notify
                .wait_timeout(Some(deadline - now))
                .map_err(ShmRingError::Io)?
            {
                Some(_) => {}
                None => return Ok(None), // timed out while parked
            }
        }
    }
}
