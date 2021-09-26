use cqlite::Graph;

#[test]
fn set_once() {
    let graph = Graph::open_anon().unwrap();
    let mut txn = graph.mut_txn().unwrap();
    let names = graph
        .prepare("CREATE (n:PERSON { name: 'First' }) RETURN n.name")
        .unwrap()
        .query_map(&mut txn, (), |m| m.get(0))
        .unwrap()
        .collect::<Result<Vec<String>, _>>()
        .unwrap();
    assert_eq!(names, vec!["First"]);
    txn.commit().unwrap();

    let mut txn = graph.mut_txn().unwrap();
    let names = graph
        .prepare("MATCH (n) SET n.name = 'Second' RETURN n.name")
        .unwrap()
        .query_map(&mut txn, (), |m| m.get(0))
        .unwrap()
        .collect::<Result<Vec<String>, _>>()
        .unwrap();
    assert_eq!(names, vec!["Second"]);
    txn.commit().unwrap();

    let names = graph
        .prepare("MATCH (n) RETURN n.name")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .collect::<Result<Vec<String>, _>>()
        .unwrap();
    assert_eq!(names, vec!["Second"]);
}

#[test]
fn set_after_create() {
    let graph = Graph::open_anon().unwrap();
    let mut txn = graph.mut_txn().unwrap();
    let names = graph
        .prepare("CREATE (n:PERSON { name: 'First' }) SET n.name = 'Second' RETURN n.name")
        .unwrap()
        .query_map(&mut txn, (), |m| m.get(0))
        .unwrap()
        .collect::<Result<Vec<String>, _>>()
        .unwrap();
    assert_eq!(names, vec!["Second"]);
    txn.commit().unwrap();

    let names = graph
        .prepare("MATCH (n) RETURN n.name")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .collect::<Result<Vec<String>, _>>()
        .unwrap();
    assert_eq!(names, vec!["Second"]);
}

#[test]
fn set_multiple_times() {
    let graph = Graph::open_anon().unwrap();
    let mut txn = graph.mut_txn().unwrap();
    let names = graph
        .prepare(
            "
            CREATE (n:PERSON { name: 'First' })
            SET n.name = 'Second'
            SET n.name = 'Third'
            SET n.name = 'Fourth'
            RETURN n.name
            ",
        )
        .unwrap()
        .query_map(&mut txn, (), |m| m.get(0))
        .unwrap()
        .collect::<Result<Vec<String>, _>>()
        .unwrap();
    assert_eq!(names, vec!["Fourth"]);
    txn.commit().unwrap();

    let names = graph
        .prepare("MATCH (n) RETURN n.name")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .collect::<Result<Vec<String>, _>>()
        .unwrap();
    assert_eq!(names, vec!["Fourth"]);
}

#[test]
fn delete_property() {
    let graph = Graph::open_anon().unwrap();
    let mut txn = graph.mut_txn().unwrap();
    let names = graph
        .prepare("CREATE (n:PERSON { name: 'First' }) RETURN n.name")
        .unwrap()
        .query_map(&mut txn, (), |m| m.get(0))
        .unwrap()
        .collect::<Result<Vec<Option<String>>, _>>()
        .unwrap();
    assert_eq!(names, vec![Some("First".into())]);
    txn.commit().unwrap();

    let mut txn = graph.mut_txn().unwrap();
    let names = graph
        .prepare("MATCH (n) SET n.name = NULL RETURN n.name")
        .unwrap()
        .query_map(&mut txn, (), |m| m.get(0))
        .unwrap()
        .collect::<Result<Vec<Option<String>>, _>>()
        .unwrap();
    assert_eq!(names, vec![None]);
    txn.commit().unwrap();

    let names = graph
        .prepare("MATCH (n) RETURN n.name")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .collect::<Result<Vec<Option<String>>, _>>()
        .unwrap();
    assert_eq!(names, vec![None]);
}
