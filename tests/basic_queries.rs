use gqlite::Graph;

#[test]
fn query_a_to_b() {
    let graph = Graph::open_anon().unwrap();
    graph.delete_me_build_test_graph().unwrap();

    let stmt = graph
        .prepare("MATCH (a) -[e]-> (b) RETURN a, b, e")
        .unwrap();
    let txn = graph.txn().unwrap();
    let mut matches = stmt.query(&txn).unwrap();

    let result = matches.step().unwrap().unwrap();
    assert_eq!(0, result.node(0).unwrap().id());
    assert_eq!("PERSON", result.node(0).unwrap().label());
    assert_eq!(1, result.node(1).unwrap().id());
    assert_eq!("PERSON", result.node(1).unwrap().label());
    assert_eq!(2, result.edge(2).unwrap().id());
    assert_eq!("KNOWS", result.edge(2).unwrap().label());

    let result = matches.step().unwrap().unwrap();
    assert_eq!(1, result.node(0).unwrap().id());
    assert_eq!("PERSON", result.node(0).unwrap().label());
    assert_eq!(0, result.node(1).unwrap().id());
    assert_eq!("PERSON", result.node(1).unwrap().label());
    assert_eq!(3, result.edge(2).unwrap().id());
    assert_eq!("HEARD_OF", result.edge(2).unwrap().label());

    assert!(matches.step().unwrap().is_none());
}

#[test]
fn query_a_to_b_with_label() {
    let graph = Graph::open_anon().unwrap();
    graph.delete_me_build_test_graph().unwrap();

    let stmt = graph
        .prepare("MATCH (a:PERSON) -[e:HEARD_OF]-> (b:PERSON) RETURN a, b, e")
        .unwrap();
    let txn = graph.txn().unwrap();
    let mut matches = stmt.query(&txn).unwrap();

    let result = matches.step().unwrap().unwrap();
    assert_eq!(1, result.node(0).unwrap().id());
    assert_eq!("PERSON", result.node(0).unwrap().label());
    assert_eq!(0, result.node(1).unwrap().id());
    assert_eq!("PERSON", result.node(1).unwrap().label());
    assert_eq!(3, result.edge(2).unwrap().id());
    assert_eq!("HEARD_OF", result.edge(2).unwrap().label());

    assert!(matches.step().unwrap().is_none());
}
