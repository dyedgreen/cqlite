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

            CREATE (peter) -[:KNOWS { since: '24.08.2019' }]-> (clark)
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
fn match_multiple_nodes() {
    let graph = create_test_graph();

    let mut nodes = graph
        .prepare("MATCH (a) MATCH (b) RETURN ID(a), ID(b)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| {
            Ok((m.get(0)?, m.get(1)?))
        })
        .unwrap()
        .collect::<Result<Vec<(u64, u64)>, _>>()
        .unwrap();
    nodes.sort_unstable();

    assert_eq!(
        nodes,
        vec![
            (0, 0),
            (0, 1),
            (0, 2),
            (0, 3),
            (1, 0),
            (1, 1),
            (1, 2),
            (1, 3),
            (2, 0),
            (2, 1),
            (2, 2),
            (2, 3),
            (3, 0),
            (3, 1),
            (3, 2),
            (3, 3),
        ],
    );

    let nodes = graph
        .prepare("MATCH () MATCH () MATCH () RETURN NULL")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |_| Ok(()))
        .unwrap()
        .collect::<Result<Vec<()>, _>>()
        .unwrap();
    assert_eq!(nodes.len(), 4 * 4 * 4);
}

#[test]
fn match_single_directed_edge() {
    let graph = create_test_graph();

    let mut paths = graph
        .prepare("MATCH (a) -[e]-> (b) RETURN ID(a), ID(b), ID(e)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| {
            Ok((m.get(0)?, m.get(1)?, m.get(2)?))
        })
        .unwrap()
        .collect::<Result<Vec<(u64, u64, u64)>, _>>()
        .unwrap();
    paths.sort_unstable();

    assert_eq!(paths, vec![(0, 1, 6), (0, 2, 4), (1, 3, 5),],);
}

#[test]
fn match_single_undirected_edge() {
    let graph = create_test_graph();

    let mut paths = graph
        .prepare("MATCH (a) -[e]- (b) RETURN ID(a), ID(b), ID(e)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| {
            Ok((m.get(0)?, m.get(1)?, m.get(2)?))
        })
        .unwrap()
        .collect::<Result<Vec<(u64, u64, u64)>, _>>()
        .unwrap();
    paths.sort_unstable();

    assert_eq!(
        paths,
        vec![
            (0, 1, 6),
            (0, 2, 4),
            (1, 0, 6),
            (1, 3, 5),
            (2, 0, 4),
            (3, 1, 5)
        ]
    );
}

#[test]
fn match_single_path() {
    let graph = create_test_graph();

    let path = graph
        .prepare("MATCH (a) -> (b) -> (c) RETURN ID(a), ID(b), ID(c)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| {
            Ok((m.get(0)?, m.get(1)?, m.get(2)?))
        })
        .unwrap()
        .collect::<Result<Vec<(u64, u64, u64)>, _>>()
        .unwrap();

    assert_eq!(path, vec![(0, 1, 3)]);
}

#[test]
fn match_path_with_multiple_clauses() {
    let graph = create_test_graph();

    let path = graph
        .prepare("MATCH (a) -> (b) MATCH (b) -> (c) RETURN ID(a), ID(b), ID(c)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| {
            Ok((m.get(0)?, m.get(1)?, m.get(2)?))
        })
        .unwrap()
        .collect::<Result<Vec<(u64, u64, u64)>, _>>()
        .unwrap();

    assert_eq!(path, vec![(0, 1, 3)]);
}

#[test]
fn match_long_path() {
    let graph = create_test_graph();

    let mut nodes: Vec<(u64, u64, u64, u64)> = graph
        .prepare("MATCH (s) <- (p) -> (c) -> (j) RETURN ID(s), ID(p), ID(c), ID(j)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| {
            Ok((m.get(0)?, m.get(1)?, m.get(2)?, m.get(3)?))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    nodes.sort_unstable();
    assert_eq!(nodes, vec![(1, 0, 1, 3), (2, 0, 1, 3)]);
}

#[test]
fn match_labeled_nodes() {
    let graph = create_test_graph();

    let mut nodes = graph
        .prepare("MATCH (a:PERSON) RETURN ID(a)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .collect::<Result<Vec<u64>, _>>()
        .unwrap();
    nodes.sort_unstable();
    assert_eq!(nodes, vec![0, 1]);

    let mut nodes = graph
        .prepare("MATCH (a:STUDENT) RETURN ID(a)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .collect::<Result<Vec<u64>, _>>()
        .unwrap();
    nodes.sort_unstable();
    assert_eq!(nodes, vec![2]);

    let mut nodes = graph
        .prepare("MATCH (a:JOURNALIST) RETURN ID(a)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .collect::<Result<Vec<u64>, _>>()
        .unwrap();
    nodes.sort_unstable();
    assert_eq!(nodes, vec![3]);
}

#[test]
fn match_labeled_edges() {
    let graph = create_test_graph();

    let mut nodes = graph
        .prepare("MATCH () -[e:IS_A]-> () RETURN ID(e)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .collect::<Result<Vec<u64>, _>>()
        .unwrap();
    nodes.sort_unstable();
    assert_eq!(nodes, vec![4, 5]);

    let mut nodes = graph
        .prepare("MATCH () -[e:KNOWS]-> () RETURN ID(e)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .collect::<Result<Vec<u64>, _>>()
        .unwrap();
    nodes.sort_unstable();
    assert_eq!(nodes, vec![6]);
}

#[test]
fn match_nodes_with_properties() {
    let graph = create_test_graph();

    let node: u64 = graph
        .prepare("MATCH (a { name: 'Peter Parker' }) RETURN ID(a)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(node, 0);

    let node: u64 = graph
        .prepare("MATCH (a { age: 42 }) RETURN ID(a)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(node, 1);

    let node: u64 = graph
        .prepare("MATCH (a { name: 'Peter Parker', height: 176.5, age: 21 }) RETURN ID(a)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(node, 0);
}

#[test]
fn match_edges_with_properties() {
    let graph = create_test_graph();

    let edge: u64 = graph
        .prepare("MATCH () -[e { since: '24.08.2019' }]-> () RETURN ID(e)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(edge, 6);
}
