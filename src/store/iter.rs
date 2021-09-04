use crate::Error;
use sanakirja::{btree, LoadPage};
use serde::Deserialize;
use std::marker::PhantomData;

pub struct ValueIter<'t, T, I>
where
    T: LoadPage,
    I: Deserialize<'t>,
{
    inner: btree::Iter<'t, T, u64, [u8], btree::page_unsized::Page<u64, [u8]>>,
    _item: PhantomData<&'t I>,
}

pub struct IndexIter<'t, T>
where
    T: LoadPage,
{
    inner: btree::Iter<'t, T, u64, u64, btree::page::Page<u64, u64>>,
    key: u64,
}

impl<'t, T, I> ValueIter<'t, T, I>
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

impl<'t, T, I> Iterator for ValueIter<'t, T, I>
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

impl<'t, T> IndexIter<'t, T>
where
    T: LoadPage,
    T::Error: std::error::Error,
{
    pub(crate) fn new(txn: &'t T, db: &btree::Db<u64, u64>, key: u64) -> Result<Self, Error> {
        let inner = btree::iter(txn, db, Some((&key, None)))?;
        Ok(Self { inner, key })
    }
}

impl<'t, T> Iterator for IndexIter<'t, T>
where
    T: LoadPage,
{
    type Item = Result<u64, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|res| {
                res.map_err(|_| Error::Todo)
                    .map(
                        |(&key, &value)| {
                            if key == self.key {
                                Some(value)
                            } else {
                                None
                            }
                        },
                    )
                    .transpose()
            })
            .flatten()
    }
}
