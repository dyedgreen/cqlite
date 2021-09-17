use gqlite::{Graph, Property};

#[test]
fn query_a_to_b() {
    let graph = Graph::open_anon().unwrap();
    graph.delete_me_build_test_graph().unwrap();

    let stmt = graph
        .prepare("MATCH (a) -[e]-> (b) RETURN ID(a), ID(b), ID(e)")
        .unwrap();
    let mut txn = graph.txn().unwrap();
    let mut matches = stmt.query(&mut txn, None).unwrap();

    let result = matches.step().unwrap().unwrap();
    assert_eq!(Property::Id(0), result.get(0).unwrap());
    assert_eq!(Property::Id(1), result.get(1).unwrap());
    assert_eq!(Property::Id(2), result.get(2).unwrap());

    let result = matches.step().unwrap().unwrap();
    assert_eq!(Property::Id(1), result.get(0).unwrap());
    assert_eq!(Property::Id(0), result.get(1).unwrap());
    assert_eq!(Property::Id(3), result.get(2).unwrap());

    assert!(matches.step().unwrap().is_none());
}

#[test]
fn query_a_to_b_with_label() {
    let graph = Graph::open_anon().unwrap();
    graph.delete_me_build_test_graph().unwrap();

    let stmt = graph
        .prepare("MATCH (a:PERSON) -[e:HEARD_OF]-> (b:PERSON) RETURN ID(a), ID(b), ID(e)")
        .unwrap();
    let mut txn = graph.txn().unwrap();
    let mut matches = stmt.query(&mut txn, None).unwrap();

    let result = matches.step().unwrap().unwrap();
    assert_eq!(Property::Id(1), result.get(0).unwrap());
    assert_eq!(Property::Id(0), result.get(1).unwrap());
    assert_eq!(Property::Id(3), result.get(2).unwrap());

    assert!(matches.step().unwrap().is_none());
}
