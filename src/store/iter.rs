use super::{DynTxn, Node, StoreTxn};
use crate::Error;
use sanakirja::{btree, Env, UnsizedStorable};
use serde::Deserialize;
use std::marker::PhantomData;

type BytesCursor<K, V> = btree::Cursor<K, V, btree::page_unsized::Page<K, V>>;

type BytesIter<'txn, K, V> =
    btree::Iter<'txn, DynTxn<&'txn Env>, K, V, btree::page_unsized::Page<K, V>>;

pub(crate) type IndexIter<'txn> =
    btree::Iter<'txn, DynTxn<&'txn Env>, u64, u64, btree::page::Page<u64, u64>>;

pub(crate) enum EdgeIter<'txn> {
    Directed(u64, IndexIter<'txn>),
    // TODO(dyedgreen): Fix this; its a bit iffy ...
    Undirected(u64, Option<IndexIter<'txn>>, IndexIter<'txn>),
}

pub(crate) struct DeserializeIter<'txn, K, I>
where
    K: UnsizedStorable,
    I: Deserialize<'txn>,
{
    inner: BytesIter<'txn, K, [u8]>,
    _item: PhantomData<&'txn I>,
}

pub(crate) enum NodeIter<'txn> {
    All(DeserializeIter<'txn, u64, Node>),
    WithLabel(String, &'txn StoreTxn<'txn>, BytesCursor<[u8], u64>),
}

impl<'txn> EdgeIter<'txn> {
    pub fn origins(txn: &'txn StoreTxn<'txn>, node: u64) -> Result<Self, Error> {
        let iter = btree::iter(&txn.txn, &txn.origins, Some((&node, None)))?;
        Ok(Self::Directed(node, iter))
    }

    pub fn targets(txn: &'txn StoreTxn<'txn>, node: u64) -> Result<Self, Error> {
        let iter = btree::iter(&txn.txn, &txn.targets, Some((&node, None)))?;
        Ok(Self::Directed(node, iter))
    }

    pub fn both(txn: &'txn StoreTxn<'txn>, node: u64) -> Result<Self, Error> {
        let iter_orig = btree::iter(&txn.txn, &txn.origins, Some((&node, None)))?;
        let iter_targ = btree::iter(&txn.txn, &txn.targets, Some((&node, None)))?;
        Ok(Self::Undirected(node, Some(iter_orig), iter_targ))
    }
}

impl<'txn> Iterator for EdgeIter<'txn> {
    type Item = Result<u64, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let filter = |id: u64| {
            move |entry: Result<(&u64, &u64), Error>| {
                entry
                    .map(|(&k, &v)| if k == id { Some(v) } else { None })
                    .transpose()
            }
        };
        match self {
            Self::Directed(id, iter) | Self::Undirected(id, None, iter) => {
                iter.next().and_then(filter(*id))
            }
            // It might also be worth, to combine the indices with a key like
            // (u64, u8) / (node, type) / orig_type = 0, target_type = 1, which is
            // ordered; so that pages can be loaded continuously ...
            Self::Undirected(id, iter_opt, iter_target) => iter_opt
                .as_mut()
                .unwrap() // None case matched above, but need ref to opt to assign
                .next()
                .and_then(filter(*id))
                .or_else(|| {
                    *iter_opt = None;
                    iter_target.next().and_then(filter(*id))
                }),
        }
    }
}

impl<'txn, K, I> DeserializeIter<'txn, K, I>
where
    K: UnsizedStorable,
    I: Deserialize<'txn>,
{
    pub(crate) fn new(
        txn: &'txn StoreTxn<'txn>,
        db: &btree::UDb<K, [u8]>,
        origin: Option<K>,
    ) -> Result<Self, Error> {
        let inner = btree::iter(&txn.txn, db, origin.as_ref().map(|key| (key, None)))?;
        Ok(Self {
            inner,
            _item: PhantomData,
        })
    }
}

impl<'txn, K, I> Iterator for DeserializeIter<'txn, K, I>
where
    K: 'txn + UnsizedStorable,
    I: Deserialize<'txn>,
{
    type Item = Result<(&'txn K, I), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|res| {
            res.map_err(|err| err).and_then(|(key, bytes)| {
                let item = bincode::deserialize(bytes)?;
                Ok((key, item))
            })
        })
    }
}

impl<'txn> NodeIter<'txn> {
    pub(crate) fn all(txn: &'txn StoreTxn<'txn>) -> Result<Self, Error> {
        Ok(Self::All(DeserializeIter::new(txn, &txn.nodes, None)?))
    }

    pub(crate) fn with_label(txn: &'txn StoreTxn<'txn>, label: String) -> Result<Self, Error> {
        let mut cursor = BytesCursor::new(&txn.txn, &txn.labels)?;
        cursor.set(&txn.txn, label.as_bytes(), None)?;
        Ok(Self::WithLabel(label, txn, cursor))
    }
}

impl<'txn> Iterator for NodeIter<'txn> {
    type Item = Result<Node, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::All(iter) => iter
                .next()
                .map(|maybe_result| maybe_result.map(|(_, node)| node)),
            Self::WithLabel(label, txn, cursor) => match cursor.next(&txn.txn).transpose() {
                Some(result) => result
                    .map(|(key, node_id)| {
                        if key == label.as_bytes() {
                            Some(node_id)
                        } else {
                            None
                        }
                    })
                    .transpose()
                    .and_then(|result| result.and_then(|&id| txn.load_node(id)).transpose()),
                None => None,
            },
        }
    }
}
