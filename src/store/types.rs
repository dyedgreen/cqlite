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
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Edge<'a> {
    pub(crate) id: u64,
    pub(crate) kind: &'a str,
    pub(crate) origin: u64,
    pub(crate) target: u64,
}
