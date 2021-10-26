use crate::Error;
use sanakirja::btree::{Db, UDb};
use sanakirja::{btree, Env, MutTxn, RootDb, Storable, UnsizedStorable};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use txn::DynTxn;

mod iter;
mod txn;
mod types;

#[cfg(test)]
mod tests;

pub(crate) use iter::{EdgeIter, NodeIter};
pub use types::{Edge, Node, PropOwned, PropRef};

const ID_SQUENCE: usize = 0;
const DB_NODES: usize = 1;
const DB_EDGES: usize = 2;
const DB_ORIGINS: usize = 3;
const DB_TARGETS: usize = 4;
const DB_LABELS: usize = 5;

pub(crate) struct Store {
    pub env: Env,
}

pub(crate) struct StoreTxn<'env> {
    txn: DynTxn<&'env Env>,
    id_seq: AtomicU64,
    updates: RwLock<Vec<Update>>,

    pub nodes: UDb<u64, [u8]>,
    pub edges: UDb<u64, [u8]>,

    pub origins: Db<u64, u64>,
    pub targets: Db<u64, u64>,

    pub labels: UDb<[u8], u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Update {
    CreateNode(Node),
    CreateEdge(Edge),
    SetNodeProperty(u64, String, PropOwned),
    SetEdgeProperty(u64, String, PropOwned),
    DeleteNode(u64),
    DeleteEdge(u64),
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

    pub fn txn(&self) -> Result<StoreTxn, Error> {
        let txn = Env::txn_begin(&self.env)?;
        let id_seq = AtomicU64::new(txn.root(ID_SQUENCE));
        let nodes = txn.root_db(DB_NODES).ok_or(Error::Corruption)?;
        let edges = txn.root_db(DB_EDGES).ok_or(Error::Corruption)?;
        let origins = txn.root_db(DB_ORIGINS).ok_or(Error::Corruption)?;
        let targets = txn.root_db(DB_TARGETS).ok_or(Error::Corruption)?;
        let labels = txn.root_db(DB_LABELS).ok_or(Error::Corruption)?;
        Ok(StoreTxn {
            txn: DynTxn::Txn(txn),
            id_seq,
            updates: RwLock::new(Vec::new()),
            nodes,
            edges,
            origins,
            targets,
            labels,
        })
    }

    pub fn mut_txn(&self) -> Result<StoreTxn, Error> {
        let mut txn = Env::mut_txn_begin(&self.env)?;
        let id_seq = AtomicU64::new(txn.root(ID_SQUENCE).unwrap_or(0));
        let nodes = Self::get_buffer_db(&mut txn, DB_NODES)?;
        let edges = Self::get_buffer_db(&mut txn, DB_EDGES)?;
        let origins = Self::get_db(&mut txn, DB_ORIGINS)?;
        let targets = Self::get_db(&mut txn, DB_TARGETS)?;
        let labels = Self::get_buffer_db(&mut txn, DB_LABELS)?;
        Ok(StoreTxn {
            txn: DynTxn::MutTxn(txn),
            id_seq,
            updates: RwLock::new(Vec::new()),
            nodes,
            edges,
            origins,
            targets,
            labels,
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

    fn get_buffer_db<K, V>(txn: &mut MutTxn<&Env, ()>, n: usize) -> Result<btree::UDb<K, V>, Error>
    where
        K: UnsizedStorable + ?Sized,
        V: UnsizedStorable + ?Sized,
    {
        if let Some(db) = txn.root_db::<K, V, _>(n) {
            Ok(db)
        } else {
            let db = btree::create_db_(txn)?;
            Ok(db)
        }
    }
}

impl<'e> StoreTxn<'e> {
    pub fn id_seq(&self) -> u64 {
        self.id_seq.fetch_add(1, Ordering::SeqCst)
    }

    pub fn load_node(&self, id: u64) -> Result<Option<Node>, Error> {
        let entry = btree::get(&self.txn, &self.nodes, &id, None)?;
        if let Some((&entry_id, bytes)) = entry {
            if entry_id == id {
                let node = bincode::deserialize(bytes)?;
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
                let node = bincode::deserialize(bytes)?;
                Ok(Some(node))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub fn unchecked_create_node(&mut self, node: Node) -> Result<Node, Error> {
        let bytes = bincode::serialize(&node)?;
        btree::put(&mut self.txn, &mut self.nodes, &node.id, bytes.as_ref())?;
        btree::put(
            &mut self.txn,
            &mut self.labels,
            node.label().as_bytes(),
            &node.id,
        )?;
        Ok(node)
    }

    pub fn update_node(&mut self, node: u64, key: &str, value: PropOwned) -> Result<(), Error> {
        let mut node = self.load_node(node)?.ok_or(Error::MissingNode)?;
        if value == PropOwned::Null {
            node.properties.remove(key);
        } else {
            node.properties.insert(key.to_string(), value);
        }
        let bytes = bincode::serialize(&node)?;
        btree::del(&mut self.txn, &mut self.nodes, &node.id, None)?;
        btree::put(&mut self.txn, &mut self.nodes, &node.id, bytes.as_ref())?;
        Ok(())
    }

    pub fn delete_node(&mut self, node: u64) -> Result<(), Error> {
        let has_origin = btree::get(&self.txn, &self.origins, &node, None)?
            .map(|(k, _)| *k == node)
            .unwrap_or(false);
        let has_target = btree::get(&self.txn, &self.targets, &node, None)?
            .map(|(k, _)| *k == node)
            .unwrap_or(false);
        if has_origin || has_target {
            Err(Error::DeleteConnected)
        } else {
            self.load_node(node)?
                .map(|node| {
                    btree::del(
                        &mut self.txn,
                        &mut self.labels,
                        node.label.as_bytes(),
                        Some(&node.id),
                    )?;
                    btree::del(&mut self.txn, &mut self.nodes, &node.id, None)
                })
                .transpose()?;
            Ok(())
        }
    }

    pub fn unchecked_create_edge(&mut self, edge: Edge) -> Result<Edge, Error> {
        let bytes = bincode::serialize(&edge)?;
        btree::put(&mut self.txn, &mut self.edges, &edge.id, bytes.as_ref())?;
        btree::put(&mut self.txn, &mut self.origins, &edge.origin, &edge.id)?;
        btree::put(&mut self.txn, &mut self.targets, &edge.target, &edge.id)?;
        Ok(edge)
    }

    pub fn update_edge(&mut self, edge: u64, key: &str, value: PropOwned) -> Result<(), Error> {
        let mut edge = self.load_edge(edge)?.ok_or(Error::MissingEdge)?;
        if value == PropOwned::Null {
            edge.properties.remove(key);
        } else {
            edge.properties.insert(key.to_string(), value);
        }
        let bytes = bincode::serialize(&edge)?;
        btree::del(&mut self.txn, &mut self.edges, &edge.id, None)?;
        btree::put(&mut self.txn, &mut self.edges, &edge.id, bytes.as_ref())?;
        Ok(())
    }

    pub fn delete_edge(&mut self, edge: u64) -> Result<(), Error> {
        if let Some(edge) = self.load_edge(edge)? {
            btree::del(
                &mut self.txn,
                &mut self.origins,
                &edge.origin,
                Some(&edge.id),
            )?;
            btree::del(
                &mut self.txn,
                &mut self.targets,
                &edge.target,
                Some(&edge.id),
            )?;
            btree::del(&mut self.txn, &mut self.edges, &edge.id, None)?;
        }
        Ok(())
    }

    pub fn queue_update(&self, update: Update) -> Result<(), Error> {
        if self.txn.is_mut() {
            self.updates.try_write()?.push(update);
            Ok(())
        } else {
            Err(Error::ReadOnlyWrite)
        }
    }

    pub fn get_updated_property(
        &self,
        node_or_edge_id: u64,
        property: &str,
    ) -> Result<Option<PropOwned>, Error> {
        Ok(self
            .updates
            .try_read()?
            .iter()
            .rev()
            .find_map(|update| match update {
                Update::CreateNode(node) => {
                    if node.id() == node_or_edge_id {
                        Some(node.property(property).clone())
                    } else {
                        None
                    }
                }
                Update::CreateEdge(edge) => {
                    if edge.id() == node_or_edge_id {
                        Some(edge.property(property).clone())
                    } else {
                        None
                    }
                }
                Update::SetNodeProperty(node, key, value) => {
                    if *node == node_or_edge_id && key == property {
                        Some(value.clone())
                    } else {
                        None
                    }
                }
                Update::SetEdgeProperty(edge, key, value) => {
                    if *edge == node_or_edge_id && key == property {
                        Some(value.clone())
                    } else {
                        None
                    }
                }
                Update::DeleteNode(_) => None,
                Update::DeleteEdge(_) => None,
            }))
    }

    pub fn flush(&mut self) -> Result<(), Error> {
        let updates = std::mem::take(&mut *self.updates.try_write()?);
        for update in updates {
            match update {
                Update::CreateNode(node) => self.unchecked_create_node(node).map(|_| ())?,
                Update::CreateEdge(edge) => self.unchecked_create_edge(edge).map(|_| ())?,
                Update::SetNodeProperty(node, key, value) => self.update_node(node, &key, value)?,
                Update::SetEdgeProperty(edge, key, value) => self.update_edge(edge, &key, value)?,
                Update::DeleteNode(node) => self.delete_node(node)?,
                Update::DeleteEdge(edge) => self.delete_edge(edge)?,
            }
        }
        Ok(())
    }

    pub fn commit(mut self) -> Result<(), Error> {
        self.flush()?;
        self.txn.set_root(ID_SQUENCE, self.id_seq.into_inner())?;
        self.txn.set_root(DB_NODES, self.nodes.db)?;
        self.txn.set_root(DB_EDGES, self.edges.db)?;
        self.txn.set_root(DB_ORIGINS, self.origins.db)?;
        self.txn.set_root(DB_TARGETS, self.targets.db)?;
        self.txn.set_root(DB_LABELS, self.labels.db)?;
        self.txn.commit()
    }
}
