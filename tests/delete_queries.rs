use cqlite::{Error, Graph};

#[macro_use]
mod common;

#[test]
fn delete_node() {
    let graph = Graph::open_anon().unwrap();

    let mut txn = graph.mut_txn().unwrap();
    graph
        .prepare("CREATE (:TEST)")
        .unwrap()
        .execute(&mut txn, ())
        .unwrap();
    txn.commit().unwrap();

    let nodes = graph
        .prepare("MATCH (n) RETURN ID(n)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .collect::<Result<Vec<u64>, _>>()
        .unwrap();
    assert_eq!(nodes, [0]);

    let mut txn = graph.mut_txn().unwrap();
    graph
        .prepare("MATCH (n) DELETE n")
        .unwrap()
        .execute(&mut txn, ())
        .unwrap();
    txn.commit().unwrap();

    let nodes = graph
        .prepare("MATCH (n) RETURN ID(n)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .collect::<Result<Vec<u64>, _>>()
        .unwrap();
    assert_eq!(nodes, []);
}

#[test]
fn double_delete_node() {
    let graph = Graph::open_anon().unwrap();

    let mut txn = graph.mut_txn().unwrap();
    graph
        .prepare("CREATE (:TEST)")
        .unwrap()
        .execute(&mut txn, ())
        .unwrap();
    txn.commit().unwrap();

    let nodes = graph
        .prepare("MATCH (n) RETURN ID(n)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .collect::<Result<Vec<u64>, _>>()
        .unwrap();
    assert_eq!(nodes, [0]);

    let mut txn = graph.mut_txn().unwrap();
    graph
        .prepare("MATCH (n) DELETE n DELETE n")
        .unwrap()
        .execute(&mut txn, ())
        .unwrap();
    txn.commit().unwrap();

    let nodes = graph
        .prepare("MATCH (n) RETURN ID(n)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .collect::<Result<Vec<u64>, _>>()
        .unwrap();
    assert_eq!(nodes, []);
}

#[test]
fn delete_edge() {
    let graph = Graph::open_anon().unwrap();

    let mut txn = graph.mut_txn().unwrap();
    graph
        .prepare("CREATE (a:A) CREATE (b:B) CREATE (a) -[:EDGE]-> (b)")
        .unwrap()
        .execute(&mut txn, ())
        .unwrap();
    txn.commit().unwrap();

    let nodes = graph
        .prepare("MATCH (a) -> (b) RETURN LABEL(a), LABEL(b)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| {
            Ok((m.get(0)?, m.get(1)?))
        })
        .unwrap()
        .collect::<Result<Vec<(String, String)>, _>>()
        .unwrap();
    assert_eq!(nodes, [("A".into(), "B".into())]);

    let mut txn = graph.mut_txn().unwrap();
    graph
        .prepare("MATCH () -[e]-> () DELETE e")
        .unwrap()
        .execute(&mut txn, ())
        .unwrap();
    txn.commit().unwrap();

    let nodes = graph
        .prepare("MATCH (a) -> (b) RETURN LABEL(a), LABEL(b)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| {
            Ok((m.get(0)?, m.get(1)?))
        })
        .unwrap()
        .collect::<Result<Vec<(String, String)>, _>>()
        .unwrap();
    assert_eq!(nodes, []);
}

#[test]
fn connected_delete_fails() {
    let graph = Graph::open_anon().unwrap();

    let mut txn = graph.mut_txn().unwrap();
    graph
        .prepare("CREATE (a:TEST) CREATE (b:TEST) CREATE (a) -[:CONNECTED]-> (b)")
        .unwrap()
        .execute(&mut txn, ())
        .unwrap();
    txn.commit().unwrap();

    let mut nodes = graph
        .prepare("MATCH (n) RETURN ID(n)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .collect::<Result<Vec<u64>, _>>()
        .unwrap();
    nodes.sort_unstable();
    assert_eq!(nodes, [0, 1]);

    let mut txn = graph.mut_txn().unwrap();
    let error = graph
        .prepare("MATCH (n) WHERE ID(n) = 0 DELETE n")
        .unwrap()
        .execute(&mut txn, ());
    assert_err!(error, Error::DeleteConnected);

    let mut nodes = graph
        .prepare("MATCH (n) RETURN ID(n)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .collect::<Result<Vec<u64>, _>>()
        .unwrap();
    nodes.sort_unstable();
    assert_eq!(nodes, [0, 1]);
}
