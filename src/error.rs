use bincode::{Error as BincodeError, ErrorKind as BincodeErrorKind};
use peg::{error::ParseError, str::LineCol};
use sanakirja::Error as SanakirjaError;
use std::convert::Infallible;
use std::sync::TryLockError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    /// IO errors, from the `std::io` module.
    #[error(transparent)]
    IO(#[from] std::io::Error),

    /// Database storage corruption error.
    #[error("Storage corruption")]
    Corruption,

    /// Lock poisoning error.
    #[error("Lock poisoning")]
    Poison,

    /// Internal coordination error.
    #[error("Internal")]
    Internal,

    /// Attempted to write to a read
    /// only transaction.
    #[error("Read only write attempt")]
    ReadOnlyWriteAttempt,

    /// CYPHER syntax error.
    #[error("Invalid syntax")]
    Syntax {
        line: usize,
        column: usize,
        offset: usize,
        expected: String,
    },
    /// The given identifier exists
    /// but does not refer to a node.
    #[error("Identifier {0} does not refer to a node")]
    IdentifierIsNotNode(String),
    /// The given identifier exists
    /// but does not refer to an edge.
    #[error("Identifier {0} does not refer to an edge")]
    IdentifierIsNotEdge(String),
    /// The given identifier already exists.
    #[error("Identifier {0} already exists")]
    IdentifierExists(String),
    /// The given identifier does not exists.
    #[error("Identifier {0} does not exists")]
    UnknownIdentifier(String),

    /// Attempted type conversion failed.
    #[error("Type mismatch")]
    TypeMismatch,

    /// Attempted to load a missing node.
    #[error("Missing node")]
    MissingNode,
    /// Attempted to load a missing edge.
    #[error("Missing edge")]
    MissingEdge,
    /// Attempted to delete a node which is
    /// still connected with the rest of the
    /// graph.
    #[error("Attempt to delete connected node")]
    DeleteConnectedAttempt,

    #[error("TODO")]
    #[deprecated]
    Todo,
}

impl From<SanakirjaError> for Error {
    fn from(error: SanakirjaError) -> Self {
        match error {
            SanakirjaError::IO(err) => Self::IO(err),
            SanakirjaError::Poison => Self::Poison,
            SanakirjaError::VersionMismatch | SanakirjaError::CRC(_) => Self::Corruption,
        }
    }
}

impl From<BincodeError> for Error {
    fn from(error: BincodeError) -> Self {
        match *error {
            BincodeErrorKind::Io(err) => Self::IO(err),
            _ => Self::Corruption,
        }
    }
}

impl From<Infallible> for Error {
    fn from(_: Infallible) -> Self {
        unreachable!()
    }
}

impl From<ParseError<LineCol>> for Error {
    fn from(error: ParseError<LineCol>) -> Self {
        Self::Syntax {
            line: error.location.line,
            column: error.location.column,
            offset: error.location.offset,
            expected: format!("{}", error.expected),
        }
    }
}

impl<T> From<TryLockError<T>> for Error {
    fn from(error: TryLockError<T>) -> Self {
        match error {
            TryLockError::Poisoned(_) => Self::Poison,
            TryLockError::WouldBlock => Self::Internal,
        }
    }
}
