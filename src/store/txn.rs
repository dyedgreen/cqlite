use crate::Error;
use sanakirja::{AllocPage, Commit, Env, LoadPage, MutTxn, RootDb, RootPage, Txn};
use std::borrow::Borrow;

pub(crate) enum DynTxn<E: Borrow<Env>> {
    Txn(Txn<E>),
    MutTxn(MutTxn<E, ()>),
}

impl<E: Borrow<Env>> DynTxn<E> {
    pub fn set_root(&mut self, n: usize, value: u64) -> Result<(), Error> {
        match self {
            DynTxn::Txn(_) => Err(Error::Todo),
            DynTxn::MutTxn(txn) => Ok(txn.set_root(n, value)),
        }
    }

    pub fn commit(self) -> Result<(), Error> {
        match self {
            DynTxn::Txn(_) => Err(Error::Todo),
            DynTxn::MutTxn(txn) => Ok(txn.commit()?),
        }
    }

    pub fn is_mut(&self) -> bool {
        match self {
            DynTxn::Txn(_) => false,
            DynTxn::MutTxn(_) => true,
        }
    }
}

impl<E: Borrow<Env>> LoadPage for DynTxn<E> {
    type Error = Error;

    fn load_page(&self, off: u64) -> Result<sanakirja::CowPage, Self::Error> {
        match self {
            DynTxn::Txn(txn) => Ok(txn.load_page(off)?),
            DynTxn::MutTxn(txn) => Ok(txn.load_page(off)?),
        }
    }

    fn rc(&self, off: u64) -> Result<u64, Self::Error> {
        match self {
            DynTxn::Txn(txn) => Ok(txn.rc(off)?),
            DynTxn::MutTxn(txn) => Ok(txn.rc(off)?),
        }
    }
}

impl<E: Borrow<Env>> RootDb for DynTxn<E> {
    fn root_db<K, V, P>(&self, n: usize) -> Option<sanakirja::btree::Db_<K, V, P>>
    where
        K: sanakirja::Storable + ?Sized,
        V: sanakirja::Storable + ?Sized,
        P: sanakirja::btree::BTreePage<K, V>,
    {
        match self {
            DynTxn::Txn(txn) => txn.root_db(n),
            DynTxn::MutTxn(txn) => txn.root_db(n),
        }
    }
}

impl<E: Borrow<Env>> RootPage for DynTxn<E> {
    /// # Safety
    /// We are using the underlying implementation
    /// of the transactions, which are safe themselves.
    unsafe fn root_page(&self) -> &[u8; 4064] {
        match self {
            DynTxn::Txn(txn) => txn.root_page(),
            DynTxn::MutTxn(txn) => txn.root_page(),
        }
    }
}

impl<E: Borrow<Env>> AllocPage for DynTxn<E> {
    fn alloc_page(&mut self) -> Result<sanakirja::MutPage, Self::Error> {
        match self {
            DynTxn::Txn(_) => Err(Error::Todo),
            DynTxn::MutTxn(txn) => Ok(txn.alloc_page()?),
        }
    }

    fn incr_rc(&mut self, off: u64) -> Result<usize, Self::Error> {
        match self {
            DynTxn::Txn(_) => Err(Error::Todo),
            DynTxn::MutTxn(txn) => Ok(txn.incr_rc(off)?),
        }
    }

    fn decr_rc(&mut self, off: u64) -> Result<usize, Self::Error> {
        match self {
            DynTxn::Txn(_) => Err(Error::Todo),
            DynTxn::MutTxn(txn) => Ok(txn.decr_rc(off)?),
        }
    }

    fn decr_rc_owned(&mut self, off: u64) -> Result<usize, Self::Error> {
        match self {
            DynTxn::Txn(_) => Err(Error::Todo),
            DynTxn::MutTxn(txn) => Ok(txn.decr_rc_owned(off)?),
        }
    }
}
