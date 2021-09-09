use serde::{Deserialize, Serialize};

/*
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum Value<'a> {
    Integer(i64),
    Real(f64),
    Boolean(bool),
    #[serde(borrow = "'a")]
    Text(&'a str),
    #[serde(borrow = "'a")]
    Blob(&'a [u8]),
}
*/

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Node<'a> {
    pub(crate) id: u64,
    pub(crate) kind: &'a str,
    pub(crate) origins: Vec<u64>,
    pub(crate) targets: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OwnedNode {
    pub(crate) id: u64,
    pub(crate) kind: String,
    pub(crate) origins: Vec<u64>,
    pub(crate) targets: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Edge<'a> {
    pub(crate) id: u64,
    pub(crate) kind: &'a str,
    pub(crate) origin: u64,
    pub(crate) target: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OwnedEdge {
    pub(crate) id: u64,
    pub(crate) kind: String,
    pub(crate) origin: u64,
    pub(crate) target: u64,
}

impl<'a> Node<'a> {
    pub(crate) fn owned(&self) -> OwnedNode {
        OwnedNode {
            id: self.id,
            kind: self.kind.to_string(),
            origins: self.origins.to_vec(),
            targets: self.targets.to_vec(),
        }
    }
}

impl<'a> Edge<'a> {
    pub(crate) fn owned(&self) -> OwnedEdge {
        OwnedEdge {
            id: self.id,
            kind: self.kind.to_string(),
            origin: self.origin,
            target: self.target,
        }
    }
}
