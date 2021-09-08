use crate::Error;
use sanakirja::btree::{Db, UDb};
use sanakirja::{btree, Commit, Env, MutTxn, RootDb, Storable, Txn, UnsizedStorable};
use std::path::Path;

mod iter;
mod types;

pub use iter::{IndexIter, ValueIter};
pub use types::{Edge, Node};

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

pub struct StoreTxn<'a> {
    pub txn: Txn<&'a Env>,
    // node and edges storage
    pub nodes: UDb<u64, [u8]>, // FIXME: having these be optional is really annoying ...
    pub edges: UDb<u64, [u8]>,
    // maps from nodes to edges
    pub origins: Db<u64, u64>,
    pub targets: Db<u64, u64>,
}

pub struct MutStoreTxn<'a> {
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
        let store = Self { env };
        store.mut_txn()?.commit()?;
        Ok(store)
    }

    pub fn open_anon() -> Result<Self, sanakirja::Error> {
        // TODO: is the size good?
        let env = Env::new_anon(4096 * 4, 2)?;
        let store = Self { env };
        store.mut_txn()?.commit()?;
        Ok(store)
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

    pub fn txn(&self) -> Result<StoreTxn, Error> {
        let txn = Env::txn_begin(&self.env)?;
        let nodes = txn.root_db(DB_NODES).ok_or(Error::Todo)?;
        let edges = txn.root_db(DB_EDGES).ok_or(Error::Todo)?;
        let origins = txn.root_db(DB_EDGE_ORIGINS).ok_or(Error::Todo)?;
        let targets = txn.root_db(DB_EDGE_TARGETS).ok_or(Error::Todo)?;
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

impl<'e> StoreTxn<'e> {
    pub fn get_node(&self, id: u64) -> Result<Option<Node>, Error> {
        let entry = btree::get(&self.txn, &self.nodes, &id, None)?;
        if let Some((_, bytes)) = entry {
            let node = bincode::deserialize(bytes.as_ref())?;
            Ok(Some(node))
        } else {
            Ok(None)
        }
    }

    pub fn get_edge(&self, id: u64) -> Result<Option<Edge>, Error> {
        let entry = btree::get(&self.txn, &self.edges, &id, None)?;
        if let Some((_, bytes)) = entry {
            let node = bincode::deserialize(bytes.as_ref())?;
            Ok(Some(node))
        } else {
            Ok(None)
        }
    }
}

impl<'e> MutStoreTxn<'e> {
    pub fn id_seq(&mut self) -> u64 {
        let id = self.txn.root(ID_SQUENCE).unwrap_or(0);
        self.txn.set_root(ID_SQUENCE, id + 1);
        id
    }

    pub fn create_node<'t>(&'t mut self, kind: &str) -> Result<Node<'t>, Error> {
        let node = Node {
            id: self.id_seq(),
            kind,
        };
        // TODO: This can avoid allocating the vector by implementing
        // UnsizedStorabe directly ...
        let node_bytes = bincode::serialize(&node)?;
        btree::put(
            &mut self.txn,
            &mut self.nodes,
            &node.id,
            node_bytes.as_ref(),
        )?;
        let entry = btree::get(&self.txn, &self.nodes, &node.id, None)?.ok_or(Error::Todo)?;
        Ok(bincode::deserialize(entry.1).map_err(|_| Error::Todo)?)
    }

    pub fn unchecked_create_edge<'t>(
        &'t mut self,
        kind: &str,
        origin: u64,
        target: u64,
    ) -> Result<Node<'t>, Error> {
        let edge = Edge {
            id: self.id_seq(),
            kind,
            origin,
            target,
        };
        // TODO: This can avoid allocating the vector by implementing
        // UnsizedStorabe directly ...
        let edge_bytes = bincode::serialize(&edge).map_err(|_| Error::Todo)?;
        btree::put(
            &mut self.txn,
            &mut self.edges,
            &edge.id,
            edge_bytes.as_ref(),
        )?;
        btree::put(&mut self.txn, &mut self.origins, &origin, &edge.id)?;
        btree::put(&mut self.txn, &mut self.targets, &target, &edge.id)?;
        let entry = btree::get(&self.txn, &self.edges, &edge.id, None)?.ok_or(Error::Todo)?;
        Ok(bincode::deserialize(entry.1).map_err(|_| Error::Todo)?)
    }

    pub fn create_edge<'t>(
        &'t mut self,
        kind: &str,
        origin: u64,
        target: u64,
    ) -> Result<Node<'t>, Error> {
        let origin_exists = btree::get(&self.txn, &self.nodes, &origin, None)?.is_some();
        let target_exists = btree::get(&self.txn, &self.nodes, &target, None)?.is_some();
        if origin_exists && target_exists {
            self.unchecked_create_edge(kind, origin, target)
        } else {
            Err(Error::Todo)
        }
    }

    pub fn commit(mut self) -> Result<(), sanakirja::Error> {
        self.txn.set_root(DB_NODES, self.nodes.db);
        self.txn.set_root(DB_EDGES, self.edges.db);
        self.txn.set_root(DB_EDGE_ORIGINS, self.origins.db);
        self.txn.set_root(DB_EDGE_TARGETS, self.targets.db);
        self.txn.commit()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_nodes_and_edges() {
        let store = Store::open("test.gqlite").unwrap();
        let mut txn = store.mut_txn().unwrap();
        let node1 = txn.create_node("PERSON").unwrap().id;
        let node2 = txn.create_node("PERSON").unwrap().id;
        let edge = txn.create_edge("KNOWS", node1, node2).unwrap().id;
        txn.commit().unwrap();

        let txn = store.txn().unwrap();
        let node1 = txn.get_node(node1).unwrap().unwrap();
        let node2 = txn.get_node(node2).unwrap().unwrap();
        let edge = txn.get_edge(edge).unwrap().unwrap();

        assert_eq!(node1.kind, "PERSON");
        assert_eq!(node2.kind, "PERSON");
        assert_eq!(edge.kind, "KNOWS");
    }
}
