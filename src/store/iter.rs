use super::{DynTxn, Node, StoreTxn};
use crate::Error;
use sanakirja::{btree, Env, UnsizedStorable};
use serde::Deserialize;
use std::marker::PhantomData;

pub(crate) type IndexIter<'t> =
    btree::Iter<'t, DynTxn<&'t Env>, u64, u64, btree::page::Page<u64, u64>>;

pub(crate) enum EdgeIter<'t> {
    Directed(u64, IndexIter<'t>),
    Undirected(u64, Option<IndexIter<'t>>, IndexIter<'t>),
}

pub(crate) struct DeserializeIter<'t, K, I>
where
    K: UnsizedStorable,
    I: Deserialize<'t>,
{
    inner: btree::Iter<'t, DynTxn<&'t Env>, K, [u8], btree::page_unsized::Page<K, [u8]>>,
    _item: PhantomData<&'t I>,
}

pub(crate) type NodeIter<'t> = DeserializeIter<'t, u64, Node>;

impl<'t, K, I> DeserializeIter<'t, K, I>
where
    K: UnsizedStorable,
    I: Deserialize<'t>,
{
    pub(crate) fn new(
        txn: &'t DynTxn<&'t Env>,
        db: &btree::UDb<K, [u8]>,
        origin: Option<K>,
    ) -> Result<Self, Error> {
        let inner = btree::iter(txn, db, origin.as_ref().map(|key| (key, None)))?;
        Ok(Self {
            inner,
            _item: PhantomData,
        })
    }
}

impl<'t, K, I> Iterator for DeserializeIter<'t, K, I>
where
    K: 't + UnsizedStorable,
    I: Deserialize<'t>,
{
    type Item = Result<(&'t K, I), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|res| {
            res.map_err(|_| Error::Todo).and_then(|(key, bytes)| {
                let item = bincode::deserialize(bytes)?;
                Ok((key, item))
            })
        })
    }
}

impl<'t> EdgeIter<'t> {
    pub fn origins(txn: &'t StoreTxn<'t>, node: u64) -> Result<Self, Error> {
        let iter = btree::iter(&txn.txn, &txn.origins, Some((&node, None)))?;
        Ok(Self::Directed(node, iter))
    }

    pub fn targets(txn: &'t StoreTxn<'t>, node: u64) -> Result<Self, Error> {
        let iter = btree::iter(&txn.txn, &txn.targets, Some((&node, None)))?;
        Ok(Self::Directed(node, iter))
    }

    pub fn both(txn: &'t StoreTxn<'t>, node: u64) -> Result<Self, Error> {
        let iter_orig = btree::iter(&txn.txn, &txn.origins, Some((&node, None)))?;
        let iter_targ = btree::iter(&txn.txn, &txn.targets, Some((&node, None)))?;
        Ok(Self::Undirected(node, Some(iter_orig), iter_targ))
    }
}

impl<'t> Iterator for EdgeIter<'t> {
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
