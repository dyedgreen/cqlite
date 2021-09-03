use sanakirja::btree::{Db, UDb};
use sanakirja::{
    btree, AllocPage, Commit, Env, LoadPage, MutTxn, RootDb, Storable, Txn, UnsizedStorable,
};
use serde::{Deserialize, Serialize};
use std::path::Path;

// alloc the pages to write to
// them every time ... You can free them
// when you replace them ...

const ID_SQUENCE: usize = 0;
const DB_NODES: usize = 1;
const DB_EDGES: usize = 2;
const DB_EDGE_ORIGINS: usize = 3;
const DB_EDGE_TARGETS: usize = 4;

pub(crate) struct Store {
    pub env: Env,
}

pub(crate) struct StoreTxn<'a> {
    pub txn: Txn<&'a Env>,
    // node and edges storage
    pub nodes: Option<UDb<u64, [u8]>>, // FIXME: having these be optional is really annoying ...
    pub edges: Option<UDb<u64, [u8]>>,
    // maps from nodes to edges
    pub origins: Option<Db<u64, u64>>,
    pub targets: Option<Db<u64, u64>>,
}

pub(crate) struct MutStoreTxn<'a> {
    pub txn: MutTxn<&'a Env, ()>,
    // node and edges storage
    pub nodes: UDb<u64, [u8]>,
    pub edges: UDb<u64, [u8]>,
    // maps from nodes to edges
    pub origins: Db<u64, u64>,
    pub targets: Db<u64, u64>,
}

impl Store {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, sanakirja::Error> {
        // TODO: How small can the thing be initially,
        // how many version do we want to allow?
        let env = Env::new(path, 4096 * 4, 2)?;
        Ok(Self { env })
    }

    pub fn mut_txn(&self) -> Result<MutStoreTxn, sanakirja::Error> {
        let mut txn = Env::mut_txn_begin(&self.env)?;
        let nodes = Self::get_buffer_db(&mut txn, DB_NODES)?;
        let edges = Self::get_buffer_db(&mut txn, DB_EDGES)?;
        let origins = Self::get_db(&mut txn, DB_EDGE_ORIGINS)?;
        let targets = Self::get_db(&mut txn, DB_EDGE_TARGETS)?;
        Ok(MutStoreTxn {
            txn,
            nodes,
            edges,
            origins,
            targets,
        })
    }

    pub fn txn(&self) -> Result<StoreTxn, sanakirja::Error> {
        let txn = Env::txn_begin(&self.env)?;
        let nodes = txn.root_db(DB_NODES);
        let edges = txn.root_db(DB_EDGES);
        let origins = txn.root_db(DB_EDGE_ORIGINS);
        let targets = txn.root_db(DB_EDGE_TARGETS);
        Ok(StoreTxn {
            txn,
            nodes,
            edges,
            origins,
            targets,
        })
    }

    fn get_db<K, V>(
        txn: &mut MutTxn<&Env, ()>,
        n: usize,
    ) -> Result<btree::Db<K, V>, sanakirja::Error>
    where
        K: Storable,
        V: Storable,
    {
        if let Some(db) = txn.root_db::<K, V, _>(n) {
            Ok(db)
        } else {
            let db = btree::create_db(txn)?;
            Ok(db)
        }
    }

    fn get_buffer_db<K>(
        txn: &mut MutTxn<&Env, ()>,
        n: usize,
    ) -> Result<btree::UDb<K, [u8]>, sanakirja::Error>
    where
        K: UnsizedStorable,
    {
        if let Some(db) = txn.root_db::<K, [u8], _>(n) {
            Ok(db)
        } else {
            let db = btree::create_db_(txn)?;
            Ok(db)
        }
    }
}

impl<'a> MutStoreTxn<'a> {
    pub fn get_id_seq(&mut self) -> u64 {
        let id = self.txn.root(ID_SQUENCE).unwrap_or(0);
        self.txn.set_root(ID_SQUENCE, id + 1);
        id
    }

    pub fn commit(mut self) -> Result<(), sanakirja::Error> {
        self.txn.set_root(DB_NODES, self.nodes.db);
        self.txn.set_root(DB_EDGES, self.edges.db);
        self.txn.set_root(DB_EDGE_ORIGINS, self.origins.db);
        self.txn.set_root(DB_EDGE_TARGETS, self.targets.db);
        self.txn.commit()
    }
}
