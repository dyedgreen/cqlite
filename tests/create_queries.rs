use cqlite::Graph;

#[test]
fn create_label_only() {
    let graph = Graph::open_anon().unwrap();
    let mut txn = graph.mut_txn().unwrap();
    let labels = graph
        .prepare("CREATE (n:TEST) RETURN LABEL(n)")
        .unwrap()
        .query_map(&mut txn, (), |m| m.get(0))
        .unwrap()
        .collect::<Result<Vec<String>, _>>()
        .unwrap();
    assert_eq!(labels, vec!["TEST"]);
    txn.commit().unwrap();

    let labels = graph
        .prepare("MATCH (n) RETURN LABEL(n)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .collect::<Result<Vec<String>, _>>()
        .unwrap();
    assert_eq!(labels, vec!["TEST"]);
}

#[test]
fn create_with_properties() {
    let graph = Graph::open_anon().unwrap();
    let mut txn = graph.mut_txn().unwrap();
    let labels = graph
        .prepare("CREATE (n:TEST { foo: 42, bar: 'baz' }) RETURN n.foo, n.bar")
        .unwrap()
        .query_map(&mut txn, (), |m| Ok((m.get(0)?, m.get(1)?)))
        .unwrap()
        .collect::<Result<Vec<(i64, String)>, _>>()
        .unwrap();
    assert_eq!(labels, vec![(42, "baz".into())]);
    txn.commit().unwrap();

    let labels = graph
        .prepare("MATCH (n) RETURN n.foo, n.bar")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| {
            Ok((m.get(0)?, m.get(1)?))
        })
        .unwrap()
        .collect::<Result<Vec<(i64, String)>, _>>()
        .unwrap();
    assert_eq!(labels, vec![(42, "baz".into())]);
}

#[test]
fn create_with_properties_from_parameters() {
    let graph = Graph::open_anon().unwrap();
    let mut txn = graph.mut_txn().unwrap();
    let labels = graph
        .prepare("CREATE (n:TEST { foo: $foo, bar: $bar }) RETURN n.foo, n.bar")
        .unwrap()
        .query_map(&mut txn, (("foo", 42), ("bar", "baz")), |m| {
            Ok((m.get(0)?, m.get(1)?))
        })
        .unwrap()
        .collect::<Result<Vec<(i64, String)>, _>>()
        .unwrap();
    assert_eq!(labels, vec![(42, "baz".into())]);
    txn.commit().unwrap();

    let labels = graph
        .prepare("MATCH (n) RETURN n.foo, n.bar")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| {
            Ok((m.get(0)?, m.get(1)?))
        })
        .unwrap()
        .collect::<Result<Vec<(i64, String)>, _>>()
        .unwrap();
    assert_eq!(labels, vec![(42, "baz".into())]);
}

#[test]
fn create_edges_with_label() {
    let graph = Graph::open_anon().unwrap();

    let mut txn = graph.mut_txn().unwrap();
    graph
        .prepare("CREATE (a:NODE_A) CREATE (b:NODE_B) CREATE (a) -[:EDGE]-> (b)")
        .unwrap()
        .execute(&mut txn, ())
        .unwrap();
    txn.commit().unwrap();

    let labels = graph
        .prepare("MATCH (a) -[e]-> (b) RETURN LABEL(a), LABEL(b), LABEL(e)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| {
            Ok((m.get(0)?, m.get(1)?, m.get(2)?))
        })
        .unwrap()
        .collect::<Result<Vec<(String, String, String)>, _>>()
        .unwrap();
    assert_eq!(labels, [("NODE_A".into(), "NODE_B".into(), "EDGE".into())]);
}
