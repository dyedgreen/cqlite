use crate::Error;
use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::{cmp::Ordering, collections::HashMap};

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PropRef<'a> {
    Id(u64),
    Integer(i64),
    Real(f64),
    Boolean(bool),
    Text(&'a str),
    Blob(&'a [u8]),
    Null,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PropOwned {
    Id(u64),
    Integer(i64),
    Real(f64),
    Boolean(bool),
    Text(String),
    Blob(Vec<u8>),
    Null,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub(crate) id: u64,
    pub(crate) label: String,
    pub(crate) properties: HashMap<String, PropOwned>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Edge {
    pub(crate) id: u64,
    pub(crate) label: String,
    pub(crate) properties: HashMap<String, PropOwned>,
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

    pub fn property(&self, key: &str) -> &PropOwned {
        self.properties.get(key).unwrap_or(&PropOwned::Null)
    }
}

impl Edge {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn label(&self) -> &str {
        self.label.as_str()
    }

    pub fn property(&self, key: &str) -> &PropOwned {
        self.properties.get(key).unwrap_or(&PropOwned::Null)
    }
}

impl PropOwned {
    pub(crate) fn to_ref(&self) -> PropRef {
        match self {
            Self::Id(id) => PropRef::Id(*id),
            Self::Integer(num) => PropRef::Integer(*num),
            Self::Real(num) => PropRef::Real(*num),
            Self::Boolean(val) => PropRef::Boolean(*val),
            Self::Text(text) => PropRef::Text(text.as_str()),
            Self::Blob(bytes) => PropRef::Blob(bytes.as_slice()),
            Self::Null => PropRef::Null,
        }
    }
}

impl<'a> PropRef<'a> {
    pub(crate) fn to_owned(&self) -> PropOwned {
        match self {
            Self::Id(id) => PropOwned::Id(*id),
            Self::Integer(num) => PropOwned::Integer(*num),
            Self::Real(num) => PropOwned::Real(*num),
            Self::Boolean(val) => PropOwned::Boolean(*val),
            Self::Text(text) => PropOwned::Text(text.to_string()),
            Self::Blob(bytes) => PropOwned::Blob(bytes.to_vec()),
            Self::Null => PropOwned::Null,
        }
    }

    pub(crate) fn loosely_equals(&self, other: &Self) -> bool {
        fn eq(lhs: &PropRef, rhs: &PropRef) -> bool {
            match (lhs, rhs) {
                (PropRef::Integer(i), PropRef::Real(r)) => *i as f64 == *r,
                _ => false,
            }
        }
        self == other || eq(self, other) || eq(other, self)
    }

    pub(crate) fn loosely_compare(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Integer(lhs), Self::Integer(rhs)) => lhs.partial_cmp(rhs),
            (Self::Real(lhs), Self::Real(rhs)) => lhs.partial_cmp(rhs),
            (Self::Real(lhs), Self::Integer(rhs)) => lhs.partial_cmp(&(*rhs as f64)),
            (Self::Integer(lhs), Self::Real(rhs)) => (*lhs as f64).partial_cmp(rhs),
            (Self::Text(lhs), Self::Text(rhs)) => Some(lhs.cmp(rhs)),
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
        match *self {
            Self::Id(val) => Ok(val),
            Self::Integer(val) => Ok(val.try_into().map_err(|_| Error::TypeMismatch)?),
            _ => Err(Error::TypeMismatch),
        }
    }
}
