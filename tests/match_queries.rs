use gqlite::Graph;

fn create_test_graph() -> Graph {
    let graph = Graph::open_anon().unwrap();
    let mut txn = graph.mut_txn().unwrap();
    graph
        .prepare(
            "
            CREATE (peter:PERSON { name: 'Peter Parker', height: 176.5, age: 21 })
            CREATE (clark:PERSON { name: 'Clark Kent', height: 190.1, age: 42 })

            CREATE (student:STUDENT)
            CREATE (journalist:JOURNALIST)

            CREATE (peter) -[:IS_A]-> (student)
            CREATE (clark) -[:IS_A]-> (journalist)

            CREATE (peter) -[:KNOWS]-> (clark)
            ",
        )
        .unwrap()
        .execute(&mut txn, ())
        .unwrap();
    txn.commit().unwrap();
    graph
}

#[test]
fn match_all_nodes() {
    let graph = create_test_graph();

    let stmt = graph.prepare("MATCH (a) RETURN ID(a), a.name").unwrap();
    let mut txn = graph.txn().unwrap();
    let mut query = stmt.query(&mut txn, ()).unwrap();

    let m = query.step().unwrap().unwrap();
    assert_eq!(0u64, m.get(0).unwrap());
    assert_eq!("Peter Parker", m.get::<String, _>(1).unwrap());

    let m = query.step().unwrap().unwrap();
    assert_eq!(1u64, m.get(0).unwrap());
    assert_eq!("Clark Kent", m.get::<String, _>(1).unwrap());

    let m = query.step().unwrap().unwrap();
    assert_eq!(2u64, m.get(0).unwrap());

    let m = query.step().unwrap().unwrap();
    assert_eq!(3u64, m.get(0).unwrap());

    assert!(query.step().unwrap().is_none());
}

#[test]
fn match_all_edges() {
    let graph = create_test_graph();

    let stmt = graph
        .prepare("MATCH (a) -[e]-> (b) RETURN ID(a), ID(e), ID(b)")
        .unwrap();
    let mut txn = graph.txn().unwrap();
    let mut query = stmt.query(&mut txn, ()).unwrap();

    let m = query.step().unwrap().unwrap();
    assert_eq!(0u64, m.get(0).unwrap());
    assert_eq!(4u64, m.get(1).unwrap());
    assert_eq!(2u64, m.get(2).unwrap());

    let m = query.step().unwrap().unwrap();
    assert_eq!(0u64, m.get(0).unwrap());
    assert_eq!(6u64, m.get(1).unwrap());
    assert_eq!(1u64, m.get(2).unwrap());

    let m = query.step().unwrap().unwrap();
    assert_eq!(1u64, m.get(0).unwrap());
    assert_eq!(5u64, m.get(1).unwrap());
    assert_eq!(3u64, m.get(2).unwrap());

    assert!(query.step().unwrap().is_none());
}
