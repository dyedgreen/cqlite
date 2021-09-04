use sanakirja::btree;
use serde::{Deserialize, Serialize};
use std::path::Path;
use store::{MutStoreTxn, Store, StoreTxn};

mod error;
mod parser;
mod planner;
mod store;

pub use error::Error;

impl<E: std::error::Error> From<E> for Error {
    fn from(error: E) -> Self {
        eprintln!("TODO: {:?}", error);
        Self::Todo
    }
}

pub struct Graph {
    store: Store,
}

impl Graph {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, sanakirja::Error> {
        let store = Store::open(path)?;
        Ok(Self { store })
    }
}
