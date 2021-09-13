use bincode::Error as BincodeError;
use peg::{error::ParseError, str::LineCol};
use sanakirja::Error as SanakirjaError;

#[derive(Debug, PartialEq)]
pub enum Error {
    Todo,
    FindMe,
}

impl From<SanakirjaError> for Error {
    fn from(error: SanakirjaError) -> Self {
        eprintln!("TODO: {:?}", error);
        Self::Todo
    }
}

impl From<BincodeError> for Error {
    fn from(error: BincodeError) -> Self {
        eprintln!("TODO: {:?}", error);
        Self::Todo
    }
}

impl From<ParseError<LineCol>> for Error {
    fn from(error: ParseError<LineCol>) -> Self {
        eprintln!("TODO: {:?}", error);
        Self::Todo
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ERROR TODO: {:?}", self)
    }
}

impl std::error::Error for Error {}
