#![allow(unused)] // TODO: Delete !

use sanakirja::btree;
use serde::{Deserialize, Serialize};
use std::path::Path;
use store::{MutStoreTxn, Store, StoreTxn};

pub(crate) mod error;
pub(crate) mod parser;
pub(crate) mod planner;
pub(crate) mod runtime;
pub(crate) mod store;

pub use error::Error;

pub struct Graph {
    store: Store,
}

impl Graph {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, sanakirja::Error> {
        let store = Store::open(path)?;
        Ok(Self { store })
    }
}
