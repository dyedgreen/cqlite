use crate::Error;
use sanakirja::btree::{Db, UDb};
use sanakirja::{btree, Env, MutTxn, RootDb, Storable, UnsizedStorable};
use std::collections::HashMap;
use std::path::Path;
use txn::DynTxn;

mod iter;
mod txn;
mod types;

pub(crate) use iter::{EdgeIter, NodeIter};
pub use types::{Edge, Node, Property};

// alloc the pages to write to
// them every time ... You can free them
// when you replace them ...

const ID_SQUENCE: usize = 0;
const DB_NODES: usize = 1;
const DB_EDGES: usize = 2;
const DB_ORIGINS: usize = 3;
const DB_TARGETS: usize = 4;

pub(crate) struct Store {
    pub env: Env,
}

pub(crate) struct StoreTxn<'a> {
    pub txn: DynTxn<&'a Env>,

    pub nodes: UDb<u64, [u8]>,
    pub edges: UDb<u64, [u8]>,

    pub origins: Db<u64, u64>,
    pub targets: Db<u64, u64>,
}

impl Store {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        // TODO: How small can the thing be initially,
        // how many version do we want to allow?
        let env = Env::new(path, 4096 * 4, 2)?;
        let store = Self { env };
        store.mut_txn()?.commit()?;
        Ok(store)
    }

    pub fn open_anon() -> Result<Self, Error> {
        // TODO: is the size good?
        let env = Env::new_anon(4096 * 4, 2)?;
        let store = Self { env };
        store.mut_txn()?.commit()?;
        Ok(store)
    }

    pub fn mut_txn(&self) -> Result<StoreTxn, Error> {
        let mut txn = Env::mut_txn_begin(&self.env)?;
        let nodes = Self::get_buffer_db(&mut txn, DB_NODES)?;
        let edges = Self::get_buffer_db(&mut txn, DB_EDGES)?;
        let origins = Self::get_db(&mut txn, DB_ORIGINS)?;
        let targets = Self::get_db(&mut txn, DB_TARGETS)?;
        Ok(StoreTxn {
            txn: DynTxn::MutTxn(txn),
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
        let origins = txn.root_db(DB_ORIGINS).ok_or(Error::Todo)?;
        let targets = txn.root_db(DB_TARGETS).ok_or(Error::Todo)?;
        Ok(StoreTxn {
            txn: DynTxn::Txn(txn),
            nodes,
            edges,
            origins,
            targets,
        })
    }

    fn get_db<K, V>(txn: &mut MutTxn<&Env, ()>, n: usize) -> Result<btree::Db<K, V>, Error>
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

    fn get_buffer_db<K>(txn: &mut MutTxn<&Env, ()>, n: usize) -> Result<btree::UDb<K, [u8]>, Error>
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
    pub fn id_seq(&mut self) -> Result<u64, Error> {
        let id = self.txn.root(ID_SQUENCE).unwrap_or(0);
        self.txn.set_root(ID_SQUENCE, id + 1)?;
        Ok(id)
    }

    pub fn load_node(&self, id: u64) -> Result<Option<Node>, Error> {
        let entry = btree::get(&self.txn, &self.nodes, &id, None)?;
        if let Some((&entry_id, bytes)) = entry {
            if entry_id == id {
                let node = bincode::deserialize(bytes.as_ref())?;
                Ok(Some(node))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub fn load_edge(&self, id: u64) -> Result<Option<Edge>, Error> {
        let entry = btree::get(&self.txn, &self.edges, &id, None)?;
        if let Some((&entry_id, bytes)) = entry {
            if entry_id == id {
                let node = bincode::deserialize(bytes.as_ref())?;
                Ok(Some(node))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub fn create_node(
        &mut self,
        label: &str,
        properties: Option<HashMap<String, Property>>,
    ) -> Result<Node, Error> {
        let node = Node {
            id: self.id_seq()?,
            label: label.to_string(),
            properties: properties.unwrap_or(HashMap::new()),
        };
        let bytes = bincode::serialize(&node)?;
        btree::put(&mut self.txn, &mut self.nodes, &node.id, bytes.as_ref())?;
        Ok(node)
    }

    pub fn update_node(&mut self, node: u64, key: &str, value: Property) -> Result<(), Error> {
        let mut node = self.load_node(node)?.ok_or(Error::Todo)?;
        node.properties.insert(key.to_string(), value);
        let bytes = bincode::serialize(&node)?;
        btree::del(&mut self.txn, &mut self.nodes, &node.id, None)?;
        btree::put(&mut self.txn, &mut self.nodes, &node.id, bytes.as_ref())?;
        Ok(())
    }

    pub fn delete_node(&mut self, node: u64) -> Result<(), Error> {
        btree::del(&mut self.txn, &mut self.nodes, &node, None)?;
        Ok(())
    }

    pub fn unchecked_create_edge(
        &mut self,
        label: &str,
        origin: u64,
        target: u64,
        properties: Option<HashMap<String, Property>>,
    ) -> Result<Edge, Error> {
        let edge = Edge {
            id: self.id_seq()?,
            label: label.to_string(),
            properties: properties.unwrap_or(HashMap::new()),
            origin,
            target,
        };
        let bytes = bincode::serialize(&edge).map_err(|_| Error::Todo)?;
        btree::put(&mut self.txn, &mut self.edges, &edge.id, bytes.as_ref())?;
        btree::put(&mut self.txn, &mut self.origins, &edge.origin, &edge.id)?;
        btree::put(&mut self.txn, &mut self.targets, &edge.target, &edge.id)?;
        Ok(edge)
    }

    pub fn create_edge(
        &mut self,
        label: &str,
        origin: u64,
        target: u64,
        properties: Option<HashMap<String, Property>>,
    ) -> Result<Edge, Error> {
        let origin_exists = self.load_node(origin)?.is_some();
        let target_exists = self.load_node(target)?.is_some();
        if !origin_exists || !target_exists {
            Err(Error::Todo)
        } else {
            self.unchecked_create_edge(label, origin, target, properties)
        }
    }

    pub fn update_edge(&mut self, edge: u64, key: &str, value: Property) -> Result<(), Error> {
        let mut edge = self.load_edge(edge)?.ok_or(Error::Todo)?;
        edge.properties.insert(key.to_string(), value);
        let bytes = bincode::serialize(&edge)?;
        btree::del(&mut self.txn, &mut self.edges, &edge.id, None)?;
        btree::put(&mut self.txn, &mut self.edges, &edge.id, bytes.as_ref())?;
        Ok(())
    }
    pub fn delete_edge(&mut self, edge: u64) -> Result<(), Error> {
        btree::del(&mut self.txn, &mut self.edges, &edge, None)?;
        Ok(())
    }

    pub fn commit(mut self) -> Result<(), Error> {
        self.txn.set_root(DB_NODES, self.nodes.db)?;
        self.txn.set_root(DB_EDGES, self.edges.db)?;
        self.txn.set_root(DB_ORIGINS, self.origins.db)?;
        self.txn.set_root(DB_TARGETS, self.targets.db)?;
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
        let node1 = txn.create_node("PERSON", None).unwrap().id;
        let node2 = txn.create_node("PERSON", None).unwrap().id;
        let edge = txn.create_edge("KNOWS", node1, node2, None).unwrap().id;
        txn.commit().unwrap();

        let txn = store.txn().unwrap();
        let node1 = txn.load_node(node1).unwrap().unwrap();
        let node2 = txn.load_node(node2).unwrap().unwrap();
        let edge = txn.load_edge(edge).unwrap().unwrap();

        assert_eq!(node1.label(), "PERSON");
        assert_eq!(node2.label(), "PERSON");
        assert_eq!(edge.label(), "KNOWS");
    }

    #[test]
    fn update_nodes_and_edges() {
        let store = Store::open_anon().unwrap();
        let mut txn = store.mut_txn().unwrap();
        let node = txn.create_node("PERSON", None).unwrap();
        let edge = txn
            .create_edge("KNOWS", node.id(), node.id(), None)
            .unwrap();
        txn.commit().unwrap();

        let mut txn = store.mut_txn().unwrap();
        txn.update_node(node.id(), "test", Property::Integer(42))
            .unwrap();
        txn.update_edge(edge.id(), "test", Property::Real(42.0))
            .unwrap();
        txn.commit().unwrap();

        let txn = store.txn().unwrap();
        let node = txn.load_node(node.id()).unwrap().unwrap();
        let edge = txn.load_edge(edge.id()).unwrap().unwrap();

        assert_eq!(node.property("test"), &Property::Integer(42));
        assert_eq!(edge.property("test"), &Property::Real(42.0));
    }

    #[test]
    fn delete_nodes_and_edges() {
        let store = Store::open_anon().unwrap();
        let mut txn = store.mut_txn().unwrap();
        let node = txn.create_node("PERSON", None).unwrap();
        let edge = txn
            .create_edge("KNOWS", node.id(), node.id(), None)
            .unwrap();
        txn.commit().unwrap();

        let mut txn = store.mut_txn().unwrap();
        assert!(txn.load_node(node.id()).unwrap().is_some());
        assert!(txn.load_edge(edge.id()).unwrap().is_some());

        txn.delete_node(node.id()).unwrap();
        txn.delete_edge(edge.id()).unwrap();
        txn.commit().unwrap();

        let txn = store.txn().unwrap();
        assert!(txn.load_node(node.id()).unwrap().is_none());
        assert!(txn.load_edge(edge.id()).unwrap().is_none());
    }
}
