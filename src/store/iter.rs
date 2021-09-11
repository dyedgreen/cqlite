use crate::Error;
use sanakirja::{btree, LoadPage};
use serde::Deserialize;
use std::marker::PhantomData;

use super::types::Node;

pub(crate) struct EntityIter<'t, T, I>
where
    T: LoadPage,
    I: Deserialize<'t>,
{
    inner: btree::Iter<'t, T, u64, [u8], btree::page_unsized::Page<u64, [u8]>>,
    _item: PhantomData<&'t I>,
}

impl<'t, T, I> EntityIter<'t, T, I>
where
    T: LoadPage,
    T::Error: std::error::Error,
    I: Deserialize<'t>,
{
    pub(crate) fn new(
        txn: &'t T,
        db: &btree::UDb<u64, [u8]>,
        origin: Option<u64>,
    ) -> Result<Self, Error> {
        let inner = btree::iter(txn, db, origin.as_ref().map(|id| (id, None)))?;
        Ok(Self {
            inner,
            _item: PhantomData,
        })
    }
}

impl<'t, T, I> Iterator for EntityIter<'t, T, I>
where
    T: LoadPage,
    I: Deserialize<'t>,
{
    type Item = Result<I, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|res| {
            res.map_err(|_| Error::Todo).and_then(|(_, bytes)| {
                let item = bincode::deserialize(bytes)?;
                Ok(item)
            })
        })
    }
}

pub(crate) struct EdgeIter(Vec<u64>);

impl EdgeIter {
    pub fn origins(node: &Node) -> Self {
        Self(node.origins.iter().map(|e| *e).collect())
    }

    pub fn targets(node: &Node) -> Self {
        Self(node.targets.iter().map(|e| *e).collect())
    }

    pub fn both(node: &Node) -> Self {
        Self(
            node.targets
                .iter()
                .chain(node.origins.iter())
                .map(|e| *e)
                .collect(),
        )
    }
}

impl Iterator for EdgeIter {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        self.0.pop()
    }
}
