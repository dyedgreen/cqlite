use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
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
    pub(crate) kind: String,

    pub(crate) data: HashMap<String, Value>,
    pub(crate) origins: Vec<u64>,
    pub(crate) targets: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Edge {
    pub(crate) id: u64,
    pub(crate) kind: String,
    pub(crate) origin: u64,
    pub(crate) target: u64,

    pub(crate) data: HashMap<String, Value>,
}

impl Node {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn label(&self) -> &str {
        &self.kind
    }

    pub fn entry(&self, key: &str) -> Option<&Value> {
        self.data.get(key)
    }
}

impl Edge {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn label(&self) -> &str {
        &self.kind
    }

    pub fn entry(&self, key: &str) -> Option<&Value> {
        self.data.get(key)
    }
}
