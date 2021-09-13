use crate::Error;
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, collections::HashMap, convert::TryInto};

// Some general notes
//
// Currently Nodes are read / written to and from disk whole-sale
//
// I think having the key value pairs, and edge associations on the nodes makes sense,
// but I think there is room to be more hands on with how these are encoded on disk.
//
// Specifically, I would like to be able to load and write or serialize and de-serialize
// independently:
//
// - ID
// - KIND
// - origins + targets
// - key value pairs
//
// What about something like the following format on disk? (All integer types encoded
// as little endian):
//
// |- HEADER (20 bytes) ---------------------------------------------| ...
// | id (u64) | kind_len (u32) | origin_len (u32) | target_len (u32) | ...
//
// ... |- KIND -|- ORIGINS -|- TARGETS -|- DATA ---------------|
// ... | [u8]   | [u64]     | [u64]     | [u8] (some encoding) |
//
// Currently all nodes are owned, but this can and should change in the future.
// The storage interface should provide granular methods like:
//
// - LoadNode / LoadEdge        (for now, loads a fully owned node or edge)
// - ...                        (future: provide ways to access borrowed/ partial
//                               data, owned by the transaction)
//
// - CreateNode / CreateEdge    (takes reference to data that should be written)
// - DeleteNode / DeleteEdge    (takes node/ edge ID)
// - UpdateNode / UpdateEdge    (takes reference to new key-value pair)
// - Flush                      (ensures writes are propagated to underlying store)

/// TODO: A single property
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Property {
    Id(u64),
    Integer(i64),
    Real(f64),
    Boolean(bool),
    Text(String),
    Blob(Vec<u8>),
    Null,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PropertyRef<'p> {
    Integer(i64),
    Real(f64),
    Boolean(bool),
    Text(&'p str),
    Blob(&'p [u8]),
    Null,
}

/// TODO: A single node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub(crate) id: u64,
    pub(crate) label: String,
    pub(crate) properties: HashMap<String, Property>,

    // TODO: Should these go back into a separate b-tree index(?)
    // if yes, that would potentially allow for much higher numbers of
    // connections ...
    pub(crate) origins: Vec<u64>,
    pub(crate) targets: Vec<u64>,
}

/// TODO: A single edge
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Edge {
    pub(crate) id: u64,
    pub(crate) label: String,
    pub(crate) properties: HashMap<String, Property>,
    pub(crate) origin: u64,
    pub(crate) target: u64,
}

impl Node {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn label(&self) -> &str {
        self.label.as_str()
    }

    pub fn property(&self, key: &str) -> &Property {
        self.properties.get(key).unwrap_or(&Property::Null)
    }
}

impl Edge {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn label(&self) -> &str {
        self.label.as_str()
    }

    pub fn property(&self, key: &str) -> &Property {
        self.properties.get(key).unwrap_or(&Property::Null)
    }
}

impl Property {
    pub(crate) fn loosely_equals(&self, other: &Property) -> bool {
        fn eq(lhs: &Property, rhs: &Property) -> bool {
            use Property::*;
            match (lhs, rhs) {
                (Integer(i), Real(r)) => *i as f64 == *r,
                _ => false,
            }
        }
        self == other || eq(self, other) || eq(other, self)
    }

    pub(crate) fn loosely_compare(&self, other: &Property) -> Option<Ordering> {
        use Property::*;
        match (self, other) {
            (Integer(lhs), Integer(rhs)) => lhs.partial_cmp(rhs),
            (Real(lhs), Real(rhs)) => lhs.partial_cmp(rhs),
            (Real(lhs), Integer(rhs)) => lhs.partial_cmp(&(*rhs as f64)),
            (Integer(lhs), Real(rhs)) => (*lhs as f64).partial_cmp(rhs),
            _ => None,
        }
    }

    pub(crate) fn is_truthy(&self) -> bool {
        match self {
            Self::Id(_) => true,
            Self::Integer(i) => *i != 0,
            Self::Real(r) => *r != 0.0,
            Self::Boolean(b) => *b,
            Self::Text(_) => true,
            Self::Blob(_) => true,
            Self::Null => false,
        }
    }

    pub(crate) fn cast_to_id(&self) -> Result<u64, Error> {
        use Property::*;
        match self {
            &Id(val) => Ok(val),
            &Integer(val) => Ok(val.try_into().map_err(|_| Error::Todo)?),
            _ => Err(Error::Todo),
        }
    }
}
