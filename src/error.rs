//! Error type for [`crate`] operations.

use std::io;
use thiserror::Error;

/// All failure modes of creating, opening, or operating on a shared-memory
/// ring buffer.
#[derive(Debug, Error)]
pub enum ShmRingError {
    /// The requested capacity is not usable. Capacities must be a non-zero
    /// power of two so that index masking (`idx & (capacity - 1)`) replaces
    /// modulo arithmetic on the hot path.
    #[error("invalid capacity {got}: must be a non-zero power of two")]
    InvalidCapacity {
        /// The rejected capacity value.
        got: usize,
    },

    /// [`crate::SpmcRingBuffer::create_new`] was called with a path that
    /// already exists. Creation is deliberately `O_EXCL`-style: silently
    /// truncating or re-initializing an existing ring would corrupt any
    /// process still mapped to it.
    #[error("ring file already exists: refusing to overwrite")]
    AlreadyExists,

    /// The file on disk is smaller than the ring layout requires. Either the
    /// file is not a ring file at all, or it was truncated after creation.
    #[error("file too short: expected at least {expected} bytes, got {got}")]
    FileTooShort {
        /// Minimum number of bytes the ring layout requires
        /// (256-byte header + `capacity * size_of::<T>()`).
        expected: u64,
        /// Actual size of the file on disk.
        got: u64,
    },

    /// A validated header field does not hold its required value. The ring
    /// file exists and is long enough, but its content was not written by
    /// this crate (or was damaged).
    #[error("header corruption in `{field}`: expected {expected:#x}, got {got:#x}")]
    HeaderCorruption {
        /// Name of the offending header field (e.g. `"magic"`,
        /// `"capacity_mask"`).
        field: &'static str,
        /// The required value. `0` means "no single required value; the field
        /// must satisfy a structural invariant" (used for the mask, which
        /// must be `2^k - 1` for some `k`).
        expected: u64,
        /// The value found in the file.
        got: u64,
    },

    /// The ring file was written by an incompatible format version. Bump
    /// [`crate::VERSION`] on any layout change; old files then fail here
    /// instead of being misinterpreted.
    #[error("format version mismatch: file has {got}, this build expects {expected}")]
    VersionMismatch {
        /// Version this build of the crate supports.
        expected: u32,
        /// Version stored in the file header.
        got: u32,
    },

    /// An underlying file operation failed.
    #[error(transparent)]
    Io(#[from] io::Error),

    /// A consumer call was given a `reader_id` outside the valid range.
    #[error("invalid reader id {id}: max valid id is {max}")]
    InvalidReaderId {
        /// The rejected reader id.
        id: usize,
        /// Highest valid reader id (`MAX_READERS - 1`).
        max: usize,
    },

    /// [`crate::SpmcRingBuffer::try_pop`] was called for a reader that has
    /// at least one outstanding [`crate::loan::Loan`].
    ///
    /// Popping past an open loan would advance the reader's cursor over the
    /// loaned slot and un-pin it, voiding the loan's zero-copy guarantee.
    /// Resolve (commit or abort) the outstanding loan first.
    #[error("reader {reader_id} has an outstanding loan; commit or abort it before popping")]
    LoanOutstanding {
        /// The reader id with the open loan.
        reader_id: usize,
    },

    /// [`crate::loan::Loan::commit`] was called on a loan whose index is not
    /// the reader's current cursor. Commits are strictly FIFO: every earlier
    /// loan for this reader must be committed (or the pipeline rewound via
    /// [`crate::loan::Loan::abort`]) before this one may advance the cursor.
    ///
    /// This is a sequencing error, not a safety error: nothing was written,
    /// and the loan (still alive) continues to pin its slot.
    #[error(
        "loan at index {index} cannot commit: reader cursor is at {cursor} \
         (commit earlier loans first, or abort to rewind the claim pipeline)"
    )]
    LoanNotAtCursor {
        /// The stream index this loan views.
        index: u64,
        /// The reader's current (committed) cursor position.
        cursor: u64,
    },
}
