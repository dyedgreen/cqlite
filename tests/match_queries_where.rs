use gqlite::Graph;

fn create_test_graph() -> Graph {
    let graph = Graph::open_anon().unwrap();
    let mut txn = graph.mut_txn().unwrap();
    graph
        .prepare(
            "
            CREATE (peter:PERSON { name: 'Peter Parker', height: 176.5, age: 21, fictional: TRUE })
            CREATE (clark:PERSON { name: 'Clark Kent', height: 190.1, age: 42, fictional: TRUE })
            CREATE (stacey:PERSON { name: 'Stacey', height: 'smol', awesome: 99999999, fictional: FALSE })

            CREATE (student:STUDENT { salary: 0, permanent: FALSE })
            CREATE (journalist:JOURNALIST { salary: 32000, permanent: TRUE })

            CREATE (peter) -[:IS_A]-> (student)
            CREATE (clark) -[:IS_A]-> (journalist)
            CREATE (stacey) -[:IS_A { since: '2018.09.01' }]-> (student)

            CREATE (peter) -[:KNOWS { since: '2019.08.24' }]-> (clark)
            CREATE (stacey) -[:WATCHED_MOVIE_ABOUT { location: 'London' }]-> (peter)
            ",
        )
        .unwrap()
        .execute(&mut txn, ())
        .unwrap();
    txn.commit().unwrap();
    graph
}

#[test]
fn match_where_node_id_eq() {
    let graph = create_test_graph();

    let lhs = graph
        .prepare("MATCH (a) WHERE ID(a) = $id RETURN a.name")
        .unwrap();
    let rhs = graph
        .prepare("MATCH (a) WHERE $id = ID(a) RETURN a.name")
        .unwrap();

    for stmt in [lhs, rhs] {
        let nodes = stmt
            .query_map(&mut graph.txn().unwrap(), ("id", 0), |m| m.get(0))
            .unwrap()
            .collect::<Result<Vec<String>, _>>()
            .unwrap();
        assert_eq!(nodes, vec!["Peter Parker"]);

        let nodes = stmt
            .query_map(&mut graph.txn().unwrap(), ("id", 1), |m| m.get(0))
            .unwrap()
            .collect::<Result<Vec<String>, _>>()
            .unwrap();
        assert_eq!(nodes, vec!["Clark Kent"]);

        let nodes = stmt
            .query_map(&mut graph.txn().unwrap(), ("id", 2), |m| m.get(0))
            .unwrap()
            .collect::<Result<Vec<String>, _>>()
            .unwrap();
        assert_eq!(nodes, vec!["Stacey"]);
    }
}

#[test]
fn match_where_node_id_eq_non_id() {
    let graph = create_test_graph();

    let nodes = graph
        .prepare("MATCH (a) WHERE ID(a) = 'not an ID' RETURN a.name")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .collect::<Result<Vec<String>, _>>()
        .unwrap();
    assert!(nodes.is_empty());
}

#[test]
fn match_where_node_label_eq() {
    let graph = create_test_graph();

    let mut nodes_match = graph
        .prepare("MATCH (a:PERSON) RETURN a.name")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .collect::<Result<Vec<String>, _>>()
        .unwrap();
    nodes_match.sort();
    let mut nodes_where = graph
        .prepare("MATCH (a) WHERE LABEL(a) = 'PERSON' RETURN a.name")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .collect::<Result<Vec<String>, _>>()
        .unwrap();
    nodes_where.sort();

    assert_eq!(nodes_match, nodes_where);
    assert_eq!(nodes_where, vec!["Clark Kent", "Peter Parker", "Stacey"]);
}

#[test]
fn match_where_node_prop_eq() {
    let graph = create_test_graph();

    let stmt = graph
        .prepare("MATCH (p) WHERE p.name = $name RETURN ID(p)")
        .unwrap();

    let mut ids = vec![];
    for name in ["Peter Parker", "Clark Kent", "Stacey"] {
        let id: Vec<u64> = stmt
            .query_map(&mut graph.txn().unwrap(), ("name", name), |m| m.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(1, id.len());
        ids.push(id[0]);
    }
    assert_eq!(ids, vec![0, 1, 2]);
}

#[test]
fn match_where_node_prop() {
    let graph = create_test_graph();

    let job: Vec<u64> = graph
        .prepare("MATCH (job) WHERE job.permanent RETURN ID(job)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(job, vec![4]);
}

#[test]
fn match_where_not_node_prop() {
    let graph = create_test_graph();

    let mut nodes: Vec<u64> = graph
        .prepare("MATCH (job) WHERE NOT job.permanent RETURN ID(job)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    nodes.sort_unstable();
    assert_eq!(nodes, vec![0, 1, 2, 3]);
}

#[test]
fn match_where_node_prop_eq_true_false() {
    let graph = create_test_graph();

    let job: Vec<u64> = graph
        .prepare("MATCH (job) WHERE job.permanent = TRUE RETURN ID(job)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(job, vec![4]);

    let job: Vec<u64> = graph
        .prepare("MATCH (job) WHERE job.permanent = FALSE RETURN ID(job)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(job, vec![3]);
}

#[test]
fn match_where_node_prop_ne_null() {
    let graph = create_test_graph();

    let mut names: Vec<(u64, String)> = graph
        .prepare("MATCH (p) WHERE p.name <> NULL RETURN ID(p), p.name")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| {
            Ok((m.get(0)?, m.get(1)?))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    names.sort();
    assert_eq!(
        names,
        vec![
            (0, "Peter Parker".into()),
            (1, "Clark Kent".into()),
            (2, "Stacey".into())
        ],
    );
}

#[test]
fn match_where_node_prop_lt_or_gt() {
    let graph = create_test_graph();

    let names: Vec<String> = graph
        .prepare("MATCH (job) WHERE job.salary > 10000.0 RETURN LABEL(job)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(names, vec!["JOURNALIST"]);

    let names: Vec<String> = graph
        .prepare("MATCH (job) WHERE job.salary < 10000 RETURN LABEL(job)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(names, vec!["STUDENT"]);

    let names: Vec<String> = graph
        .prepare("MATCH (job) WHERE job.salary = 10000 RETURN LABEL(job)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert!(names.is_empty());
}

#[test]
fn match_where_edge_id_eq() {
    let graph = create_test_graph();

    let lhs = graph
        .prepare("MATCH (a) -[e]- (b) WHERE ID(e) = $id RETURN ID(a), ID(e), ID(b)")
        .unwrap();
    let rhs = graph
        .prepare("MATCH (a) -[e]- (b) WHERE $id = ID(e) RETURN ID(a), ID(e), ID(b)")
        .unwrap();

    for stmt in [lhs, rhs] {
        let mut paths: Vec<(u64, u64, u64)> = stmt
            .query_map(&mut graph.txn().unwrap(), ("id", 5), |m| {
                Ok((m.get(0)?, m.get(1)?, m.get(2)?))
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        paths.sort_unstable();
        assert_eq!(paths, vec![(0, 5, 3), (3, 5, 0)]);
    }
}

#[test]
fn match_where_edge_prop_eq() {
    let graph = create_test_graph();

    let path: Vec<(u64, u64, u64)> = graph
        .prepare("MATCH (a) <-[e]- (b) WHERE 'London' = e.location RETURN ID(a), ID(e), ID(b)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| {
            Ok((m.get(0)?, m.get(1)?, m.get(2)?))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(path, vec![(0, 9, 2)]);
}

#[test]
fn match_where_edge_prop_gt() {
    let graph = create_test_graph();

    let path: Vec<(u64, u64, u64)> = graph
        .prepare("MATCH (a) -[e]-> (b) WHERE e.since > '2019' RETURN ID(a), ID(e), ID(b)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| {
            Ok((m.get(0)?, m.get(1)?, m.get(2)?))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(path, vec![(0, 8, 1)]);
}

#[test]
fn match_where_a_or_b() {
    let graph = create_test_graph();

    let mut paths: Vec<(u64, u64, u64)> = graph
        .prepare(
            "
            MATCH (a) -[e]-> (b)
            WHERE a.fictional = TRUE OR b.fictional = TRUE
            RETURN ID(a), ID(e), ID(b)
            ",
        )
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| {
            Ok((m.get(0)?, m.get(1)?, m.get(2)?))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    paths.sort_unstable();
    assert_eq!(paths, vec![(0, 5, 3), (0, 8, 1), (1, 6, 4), (2, 9, 0)]);
}

#[test]
fn match_path_with_id_constraint() {
    let graph = create_test_graph();

    let mut paths: Vec<(u64, u64, u64)> = graph
        .prepare(
            "
            MATCH (a) -> (b) <- (c)
            WHERE ID(c) = 2
            RETURN ID(a), ID(b), ID(c)
            ",
        )
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| {
            Ok((m.get(0)?, m.get(1)?, m.get(2)?))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    paths.sort_unstable();
    assert_eq!(paths, [(0, 3, 2), (2, 0, 2), (2, 3, 2)]);

    let mut paths: Vec<(u64, u64, u64)> = graph
        .prepare(
            "
            MATCH (a) -> (b) <- (c)
            WHERE ID(c) = 2 AND ID(a) <> ID(c)
            RETURN ID(a), ID(b), ID(c)
            ",
        )
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| {
            Ok((m.get(0)?, m.get(1)?, m.get(2)?))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    paths.sort_unstable();
    assert_eq!(paths, [(0, 3, 2)]);
}
