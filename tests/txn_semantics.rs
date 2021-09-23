use gqlite::Graph;

#[test]
fn concurrent_reader_and_writer() {
    let graph = Graph::open_anon().unwrap();

    let mut txn = graph.mut_txn().unwrap();
    graph
        .prepare("CREATE (:TEST { value: 'First' })")
        .unwrap()
        .execute(&mut txn, ())
        .unwrap();
    txn.commit().unwrap();

    let mut read = graph.txn().unwrap();
    let mut write = graph.mut_txn().unwrap();

    graph
        .prepare("MATCH (n) SET n.value = 'First Overwritten'")
        .unwrap()
        .execute(&mut write, ())
        .unwrap();
    graph
        .prepare("CREATE (:TEST { value: 'Second' })")
        .unwrap()
        .execute(&mut write, ())
        .unwrap();
    write.commit().unwrap();

    let values = graph
        .prepare("MATCH (n) RETURN n.value")
        .unwrap()
        .query_map(&mut read, (), |m| m.get(0))
        .unwrap()
        .collect::<Result<Vec<String>, _>>()
        .unwrap();
    assert_eq!(values, ["First"]);

    let mut values = graph
        .prepare("MATCH (n) RETURN n.value")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .collect::<Result<Vec<String>, _>>()
        .unwrap();
    values.sort();
    assert_eq!(values, ["First Overwritten", "Second"]);
}
