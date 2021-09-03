use sanakirja::btree;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, path::Path};
use store::{MutStoreTxn, Store, StoreTxn};

mod parser;
mod store;

#[derive(Debug, PartialEq)]
pub enum Error {
    Todo,
}

impl<E: std::error::Error> From<E> for Error {
    fn from(error: E) -> Self {
        eprintln!("TODO: {:?}", error);
        Self::Todo
    }
}

pub struct Graph {
    store: Store,
}

impl Graph {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, sanakirja::Error> {
        let store = Store::open(path)?;
        Ok(Self { store })
    }

    // fn query<'a>(txn: &'a StoreTxn, query: &str) -> Result<Vec<Item<'a>>, Error> {
    //     use parser::ast;
    //     let query = parser::parse(query)?;
    //     println!("{:?}", query);

    //     // lets do one match clause to start with ...
    //     let results = vec![];
    //     let ast::MatchClause { start, edges } = &query.match_clauses[0];

    //     #[derive(Debug)]
    //     struct Env<'a, 'b> {
    //         current: Node<'b>,
    //         nodes: HashMap<&'a str, Node<'b>>,
    //         edges: HashMap<&'a str, Edge<'b>>,
    //     }

    //     fn filter(
    //         txn: &StoreTxn,
    //         env: &mut Env,
    //         edges: &[(ast::Edge, ast::Node)],
    //     ) -> Result<(), Error> {
    //         // if edges.is_empty() {
    //         //     // done! we found a match ...
    //         //     println!("found match: {:?}", env);
    //         //     // TODO... return selected fields ...
    //         //     Ok(())
    //         // } else {
    //         //     let (edge_expr, other_expr) = edges[0];
    //         //     match edge_expr.direction {
    //         //         ast::Direction::Left => {
    //         //             // current is target
    //         //             let edges = btree::iter(
    //         //                 &txn.txn,
    //         //                 &txn.targets.ok_or(Error::Todo)?,
    //         //                 Some((&env.current.id, None)),
    //         //             )?
    //         //             .filter_map(|entry| entry.ok())
    //         //             .take_while(|&(&id, _)| id == env.current.id);
    //         //             for (_, &edge_id) in edges {
    //         //                 let edge = Graph::get_edge(&txn, edge_id)?.ok_or(Error::Todo)?;
    //         //                 if edge_expr.label.kind.map(|k| k == edge.kind).unwrap_or(true) {
    //         //                     // edge matches queried expression
    //         //                     if let Some(name) = edge_expr.label.name {
    //         //                         env.edges.insert(name, edge);
    //         //                     }
    //         //                 }
    //         //             }
    //         //         }
    //         //         ast::Direction::Right => {
    //         //             // current is origin
    //         //             btree::iter(
    //         //                 &txn.txn,
    //         //                 &txn.origins.ok_or(Error::Todo)?,
    //         //                 Some((&env.current.id, None)),
    //         //             )?
    //         //             .filter_map(|entry| entry.ok())
    //         //             .take_while(|&(&id, _)| id == env.current.id)
    //         //         }
    //         //         ast::Direction::Either => {
    //         //             // current may be either
    //         //         }
    //         //     }
    //         //     Ok(())
    //         // }
    //     }

    //     // if let Some(nodes) = &txn.nodes {
    //     //     let vars: HashMap<&str, &Node> = HashMap::new();
    //     //     for entry in btree::iter(&txn.txn, nodes, None)? {
    //     //         let (id, bytes) = entry?;
    //     //         println!("id: {}", id);
    //     //     }
    //     // }
    //     Ok(results)
    // }

    fn create_node<'a>(txn: &'a mut MutStoreTxn, kind: &str) -> Result<Node<'a>, Error> {
        let node = Node {
            id: txn.id_seq(),
            kind,
        };
        // TODO: This can avoid allocating the vector by implementing
        // UnsizedStorabe directly ...
        let node_bytes = bincode::serialize(&node)?;
        btree::put(&mut txn.txn, &mut txn.nodes, &node.id, node_bytes.as_ref())?;
        let entry = btree::get(&txn.txn, &txn.nodes, &node.id, None)?.ok_or(Error::Todo)?;
        Ok(bincode::deserialize(entry.1).map_err(|_| Error::Todo)?)
    }

    fn unchecked_create_edge<'a>(
        txn: &'a mut MutStoreTxn,
        kind: &str,
        origin: u64,
        target: u64,
    ) -> Result<Node<'a>, Error> {
        let edge = Edge {
            id: txn.id_seq(),
            kind,
            origin,
            target,
        };
        // TODO: This can avoid allocating the vector by implementing
        // UnsizedStorabe directly ...
        let edge_bytes = bincode::serialize(&edge).map_err(|_| Error::Todo)?;
        btree::put(&mut txn.txn, &mut txn.edges, &edge.id, edge_bytes.as_ref())?;
        btree::put(&mut txn.txn, &mut txn.origins, &origin, &edge.id)?;
        btree::put(&mut txn.txn, &mut txn.targets, &target, &edge.id)?;
        let entry = btree::get(&txn.txn, &txn.edges, &edge.id, None)?.ok_or(Error::Todo)?;
        Ok(bincode::deserialize(entry.1).map_err(|_| Error::Todo)?)
    }

    fn create_edge<'a>(
        txn: &'a mut MutStoreTxn,
        kind: &str,
        origin: u64,
        target: u64,
    ) -> Result<Node<'a>, Error> {
        let origin_exists = btree::get(&txn.txn, &txn.nodes, &origin, None)?.is_some();
        let target_exists = btree::get(&txn.txn, &txn.nodes, &target, None)?.is_some();
        if origin_exists && target_exists {
            Self::unchecked_create_edge(txn, kind, origin, target)
        } else {
            Err(Error::Todo)
        }
    }

    fn get_node<'a>(txn: &'a StoreTxn, id: u64) -> Result<Option<Node<'a>>, Error> {
        if let Some(nodes) = &txn.nodes {
            let entry = btree::get(&txn.txn, nodes, &id, None)?;
            if let Some((_, bytes)) = entry {
                let node = bincode::deserialize(bytes.as_ref())?;
                Ok(Some(node))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    fn get_edge<'a>(txn: &'a StoreTxn, id: u64) -> Result<Option<Edge<'a>>, Error> {
        if let Some(edges) = &txn.edges {
            let entry = btree::get(&txn.txn, edges, &id, None)?;
            if let Some((_, bytes)) = entry {
                let node = bincode::deserialize(bytes.as_ref())?;
                Ok(Some(node))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}

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
    id: u64,
    kind: &'a str,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Edge<'a> {
    id: u64,
    kind: &'a str,
    origin: u64,
    target: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Item<'a> {
    Node(Node<'a>),
    Edge(Edge<'a>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_nodes_and_edges() {
        let db = Graph::open("test.gqlite").unwrap();
        let mut txn = db.store.mut_txn().unwrap();
        let node1 = Graph::create_node(&mut txn, "PERSON").unwrap().id;
        let node2 = Graph::create_node(&mut txn, "PERSON").unwrap().id;
        let edge = Graph::create_edge(&mut txn, "KNOWS", node1, node2)
            .unwrap()
            .id;
        txn.commit().unwrap();

        println!("{}, {}, {}", node1, node2, edge);

        let txn = db.store.txn().unwrap();
        let node1 = Graph::get_node(&txn, node1).unwrap().unwrap();
        let node2 = Graph::get_node(&txn, node2).unwrap().unwrap();
        let edge = Graph::get_edge(&txn, edge).unwrap().unwrap();

        assert_eq!(node1.kind, "PERSON");
        assert_eq!(node2.kind, "PERSON");
        assert_eq!(edge.kind, "KNOWS");
    }
}
