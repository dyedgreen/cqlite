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

    let mut txn = graph.txn().unwrap();
    let mut nodes = graph
        .prepare("MATCH (a) RETURN ID(a), a.name")
        .unwrap()
        .query_map(&mut txn, (), |m| Ok((m.get(0)?, m.get(1)?)))
        .unwrap()
        .collect::<Result<Vec<(u64, Option<String>)>, _>>()
        .unwrap();
    nodes.sort();

    assert_eq!(
        nodes,
        vec![
            (0, Some("Peter Parker".into())),
            (1, Some("Clark Kent".into())),
            (2, None),
            (3, None)
        ],
    );
}

#[test]
fn match_all_edges() {
    let graph = create_test_graph();

    let mut txn = graph.txn().unwrap();
    let mut nodes = graph
        .prepare("MATCH (a) -[e]-> (b) RETURN ID(a), ID(b), ID(e)")
        .unwrap()
        .query_map(&mut txn, (), |m| Ok((m.get(0)?, m.get(1)?, m.get(2)?)))
        .unwrap()
        .collect::<Result<Vec<(u64, u64, u64)>, _>>()
        .unwrap();
    nodes.sort();

    assert_eq!(nodes, vec![(0, 1, 6), (0, 2, 4), (1, 3, 5),],);
}
