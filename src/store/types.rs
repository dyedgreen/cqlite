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
// ... |- KIND -|- DATA ---------------|
// ... | [u8]   | [u8] (some encoding) |
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

/// TODO: A reference to a single property
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PropertyRef<'prop> {
    Id(u64),
    Integer(i64),
    Real(f64),
    Boolean(bool),
    Text(&'prop str),
    Blob(&'prop [u8]),
    Null,
}

/// TODO: A single node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub(crate) id: u64,
    pub(crate) label: String,
    pub(crate) properties: HashMap<String, Property>,
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
    pub fn as_ref(&self) -> PropertyRef {
        match self {
            Property::Id(id) => PropertyRef::Id(*id),
            Property::Integer(num) => PropertyRef::Integer(*num),
            Property::Real(num) => PropertyRef::Real(*num),
            Property::Boolean(val) => PropertyRef::Boolean(*val),
            Property::Text(text) => PropertyRef::Text(text),
            Property::Blob(bytes) => PropertyRef::Blob(bytes),
            Property::Null => PropertyRef::Null,
        }
    }
}

impl<'prop> PropertyRef<'prop> {
    pub fn to_owned(&self) -> Property {
        match self {
            PropertyRef::Id(id) => Property::Id(*id),
            PropertyRef::Integer(num) => Property::Integer(*num),
            PropertyRef::Real(num) => Property::Real(*num),
            PropertyRef::Boolean(val) => Property::Boolean(*val),
            PropertyRef::Text(text) => Property::Text(text.to_string()),
            PropertyRef::Blob(bytes) => Property::Blob(bytes.to_vec()),
            PropertyRef::Null => Property::Null,
        }
    }

    pub(crate) fn loosely_equals(&self, other: &PropertyRef) -> bool {
        fn eq(lhs: &PropertyRef, rhs: &PropertyRef) -> bool {
            use PropertyRef::*;
            match (lhs, rhs) {
                (Integer(i), Real(r)) => *i as f64 == *r,
                _ => false,
            }
        }
        self == other || eq(self, other) || eq(other, self)
    }

    pub(crate) fn loosely_compare(&self, other: &PropertyRef) -> Option<Ordering> {
        use PropertyRef::*;
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
        use PropertyRef::*;
        match self {
            &Id(val) => Ok(val),
            &Integer(val) => Ok(val.try_into().map_err(|_| Error::Todo)?),
            _ => Err(Error::Todo),
        }
    }
}
