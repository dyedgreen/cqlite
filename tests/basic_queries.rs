use cqlite::{Graph, Property};

#[test]
fn run_a_to_b() {
    let graph = Graph::open_anon().unwrap();

    let mut txn = graph.mut_txn().unwrap();
    graph
        .prepare("CREATE (:PERSON) CREATE (:PERSON)")
        .unwrap()
        .execute(&mut txn, ())
        .unwrap();
    graph
        .prepare("MATCH (a) MATCH (b) WHERE ID(a) <> ID(b) CREATE (a) -[:KNOWS]-> (b)")
        .unwrap()
        .execute(&mut txn, ())
        .unwrap();
    txn.commit().unwrap();

    let stmt = graph
        .prepare("MATCH (a) -[e]-> (b) RETURN ID(a), ID(b), ID(e)")
        .unwrap();
    let mut txn = graph.txn().unwrap();
    let mut matches = stmt.query(&mut txn, ()).unwrap();

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
fn run_a_edge_b() {
    let graph = Graph::open_anon().unwrap();

    let mut txn = graph.mut_txn().unwrap();
    graph
        .prepare("CREATE (a:PERSON) CREATE (b:PERSON) CREATE (a) -[:KNOWS]-> (b)")
        .unwrap()
        .execute(&mut txn, ())
        .unwrap();
    txn.commit().unwrap();

    let stmt = graph
        .prepare("MATCH (a) -[e]- (b) RETURN ID(a), ID(b), ID(e)")
        .unwrap();
    let mut txn = graph.txn().unwrap();
    let mut matches = stmt.query(&mut txn, ()).unwrap();

    let result = matches.step().unwrap().unwrap();
    assert_eq!(Property::Id(0), result.get(0).unwrap());
    assert_eq!(Property::Id(1), result.get(1).unwrap());
    assert_eq!(Property::Id(2), result.get(2).unwrap());

    let result = matches.step().unwrap().unwrap();
    assert_eq!(Property::Id(1), result.get(0).unwrap());
    assert_eq!(Property::Id(0), result.get(1).unwrap());
    assert_eq!(Property::Id(2), result.get(2).unwrap());

    assert!(matches.step().unwrap().is_none());
}

#[test]
fn run_a_to_a() {
    let graph = Graph::open_anon().unwrap();

    let stmt = graph
        .prepare(
            "
            CREATE (a:PERSON)
            CREATE (b:PERSON)
            CREATE (a) -[:KNOWS]-> (a)
            CREATE (b) <-[:KNOWS]- (b)
            ",
        )
        .unwrap();
    let mut txn = graph.mut_txn().unwrap();
    stmt.execute(&mut txn, ()).unwrap();
    txn.commit().unwrap();

    let stmt = graph
        .prepare("MATCH (a) -[e]-> (a) RETURN ID(a), ID(e)")
        .unwrap();
    let mut txn = graph.txn().unwrap();
    let mut matches = stmt.query(&mut txn, ()).unwrap();

    let result = matches.step().unwrap().unwrap();
    assert_eq!(Property::Id(0), result.get(0).unwrap());
    assert_eq!(Property::Id(2), result.get(1).unwrap());

    let result = matches.step().unwrap().unwrap();
    assert_eq!(Property::Id(1), result.get(0).unwrap());
    assert_eq!(Property::Id(3), result.get(1).unwrap());
}

#[test]
fn run_a_edge_a() {
    let graph = Graph::open_anon().unwrap();

    let stmt = graph
        .prepare(
            "
            CREATE (a:PERSON)
            CREATE (b:PERSON)
            CREATE (a) -[edge_a:KNOWS]-> (a)
            CREATE (b) -[edge_b:KNOWS]-> (b)
            RETURN ID(a), ID(b), ID(edge_a), ID(edge_b)
            ",
        )
        .unwrap();
    let mut txn = graph.mut_txn().unwrap();
    let mut query = stmt.query(&mut txn, ()).unwrap();
    let matches = query.step().unwrap().unwrap();
    let id_a: u64 = matches.get(0).unwrap();
    let id_b: u64 = matches.get(1).unwrap();
    let id_edge_a: u64 = matches.get(2).unwrap();
    let id_edge_b: u64 = matches.get(3).unwrap();
    txn.commit().unwrap();

    let stmt = graph
        .prepare("MATCH (a) -[e]- (a) RETURN ID(a), ID(e)")
        .unwrap();
    let mut txn = graph.txn().unwrap();

    let mut matches = stmt.query(&mut txn, ()).unwrap();

    let result = matches.step().unwrap().unwrap();
    assert_eq!(id_a, result.get(0).unwrap());
    assert_eq!(id_edge_a, result.get(1).unwrap());
    let result = matches.step().unwrap().unwrap();
    assert_eq!(id_a, result.get(0).unwrap());
    assert_eq!(id_edge_a, result.get(1).unwrap());

    let result = matches.step().unwrap().unwrap();
    assert_eq!(id_b, result.get(0).unwrap());
    assert_eq!(id_edge_b, result.get(1).unwrap());
    let result = matches.step().unwrap().unwrap();
    assert_eq!(id_b, result.get(0).unwrap());
    assert_eq!(id_edge_b, result.get(1).unwrap());

    assert!(matches.step().unwrap().is_none());
}

#[test]
fn run_a_knows_b() {
    let graph = Graph::open_anon().unwrap();

    let stmt = graph
        .prepare(
            "
            CREATE (a:PERSON)
            CREATE (b:PERSON)
            CREATE (a) -[:KNOWS]-> (b)
            CREATE (a) <-[:HEARD_OF]- (b)
            ",
        )
        .unwrap();
    let mut txn = graph.mut_txn().unwrap();
    stmt.execute(&mut txn, ()).unwrap();
    txn.commit().unwrap();

    let stmt = graph
        .prepare("MATCH (a) -[e:KNOWS]-> (b) RETURN ID(a), ID(b), ID(e)")
        .unwrap();
    let mut txn = graph.txn().unwrap();
    let mut matches = stmt.query(&mut txn, ()).unwrap();

    let result = matches.step().unwrap().unwrap();
    assert_eq!(Property::Id(0), result.get(0).unwrap());
    assert_eq!(Property::Id(1), result.get(1).unwrap());
    assert_eq!(Property::Id(2), result.get(2).unwrap());

    assert!(matches.step().unwrap().is_none());
}

#[test]
fn run_a_edge_b_with_where_property() {
    let graph = Graph::open_anon().unwrap();

    let stmt = graph
        .prepare(
            "
            CREATE (a:PERSON { answer: 42 })
            CREATE (b:PERSON)
            CREATE (c:PERSON)
            CREATE (a) -[:KNOWS]-> (b)
            ",
        )
        .unwrap();
    let mut txn = graph.mut_txn().unwrap();
    stmt.execute(&mut txn, ()).unwrap();
    txn.commit().unwrap();

    let stmt = graph
        .prepare(
            "
            MATCH (a:PERSON) -[:KNOWS]- (b:PERSON)
            WHERE a.answer = 42
            RETURN ID(a), ID(b)
            ",
        )
        .unwrap();
    let mut txn = graph.txn().unwrap();
    let mut matches = stmt.query(&mut txn, ()).unwrap();

    let result = matches.step().unwrap().unwrap();
    assert_eq!(Property::Id(0), result.get(0).unwrap());
    assert_eq!(Property::Id(1), result.get(1).unwrap());

    assert!(matches.step().unwrap().is_none());
}

#[test]
fn run_a_edge_b_with_property_map() {
    let graph = Graph::open_anon().unwrap();

    let stmt = graph
        .prepare(
            "
            CREATE ( a : PERSON { test: 'hello world!' } )
            CREATE ( b : PERSON )
            CREATE (a) -[:KNOWS]-> (b)
            ",
        )
        .unwrap();
    let mut txn = graph.mut_txn().unwrap();
    stmt.execute(&mut txn, ()).unwrap();
    txn.commit().unwrap();

    let stmt = graph
        .prepare(
            "
            MATCH (a:PERSON { test: 'hello world!' }) -[:KNOWS]- (b:PERSON)
            RETURN ID(a), ID(b)
            ",
        )
        .unwrap();
    let mut txn = graph.txn().unwrap();
    let mut matches = stmt.query(&mut txn, ()).unwrap();

    let result = matches.step().unwrap().unwrap();
    assert_eq!(Property::Id(0), result.get(0).unwrap());
    assert_eq!(Property::Id(1), result.get(1).unwrap());

    assert!(matches.step().unwrap().is_none());
}

#[test]
fn run_a_edge_b_with_where_id() {
    let graph = Graph::open_anon().unwrap();

    let stmt = graph
        .prepare("CREATE (a:PERSON) CREATE (b:PERSON)")
        .unwrap();
    let mut txn = graph.mut_txn().unwrap();
    stmt.execute(&mut txn, ()).unwrap();
    txn.commit().unwrap();

    let stmt = graph
        .prepare(
            "
            MATCH (a:PERSON)
            WHERE 1 = ID ( a )
            RETURN ID(a)
            ",
        )
        .unwrap();
    let mut txn = graph.txn().unwrap();
    let mut matches = stmt.query(&mut txn, ()).unwrap();

    let result = matches.step().unwrap().unwrap();
    assert_eq!(Property::Id(1), result.get(0).unwrap());

    assert!(matches.step().unwrap().is_none());
}

#[test]
fn run_a_where_with_parameters() {
    let graph = Graph::open_anon().unwrap();

    let stmt = graph
        .prepare(
            "
            CREATE (a:PERSON { name: 'Peter Parker', age: 21.0 })
            CREATE (b:PERSON)
            CREATE (a) -[:KNOWS]-> (b)
            ",
        )
        .unwrap();
    let mut txn = graph.mut_txn().unwrap();
    stmt.execute(&mut txn, ()).unwrap();
    txn.commit().unwrap();

    let stmt = graph
        .prepare(
            "
            MATCH (a:PERSON)
            WHERE a.age >= $min_age
            RETURN ID(a)
            ",
        )
        .unwrap();
    let mut txn = graph.txn().unwrap();
    let mut matches = stmt.query(&mut txn, ("min_age", 18i64)).unwrap();

    let result = matches.step().unwrap().unwrap();
    assert_eq!(Property::Id(0), result.get(0).unwrap());
    assert!(matches.step().unwrap().is_none());
}

#[test]
fn run_set() {
    let graph = Graph::open_anon().unwrap();

    let stmt = graph.prepare("CREATE (:PERSON)").unwrap();
    let mut txn = graph.mut_txn().unwrap();
    stmt.execute(&mut txn, ()).unwrap();
    txn.commit().unwrap();

    let stmt = graph
        .prepare("MATCH (a:PERSON) SET a.answer = $ans")
        .unwrap();
    let mut txn = graph.mut_txn().unwrap();
    stmt.execute(&mut txn, ("ans", 42)).unwrap();
    txn.commit().unwrap();

    let stmt = graph
        .prepare("MATCH (a:PERSON) WHERE ID(a) = 0 RETURN a.answer")
        .unwrap();
    let mut txn = graph.txn().unwrap();
    let mut query = stmt.query(&mut txn, ()).unwrap();
    let results = query.step().unwrap().unwrap();
    assert_eq!(Property::Integer(42), results.get(0).unwrap());
}

#[test]
fn return_from_set() {
    let graph = Graph::open_anon().unwrap();

    let stmt = graph.prepare("CREATE (:PERSON)").unwrap();
    let mut txn = graph.mut_txn().unwrap();
    stmt.execute(&mut txn, ()).unwrap();
    txn.commit().unwrap();

    let stmt = graph
        .prepare("MATCH (a:PERSON) SET a.answer = 42 RETURN ID(a), a.answer")
        .unwrap();
    let mut txn = graph.mut_txn().unwrap();
    let mut query = stmt.query(&mut txn, ()).unwrap();
    let results = query.step().unwrap().unwrap();
    assert_eq!(Property::Id(0), results.get(0).unwrap());
    assert_eq!(Property::Integer(42), results.get(1).unwrap());
    assert!(query.step().unwrap().is_none());
    txn.commit().unwrap();

    let stmt = graph
        .prepare("MATCH (a:PERSON) RETURN ID(a), a.answer")
        .unwrap();
    let mut txn = graph.mut_txn().unwrap();
    let mut query = stmt.query(&mut txn, ()).unwrap();
    let results = query.step().unwrap().unwrap();
    assert_eq!(Property::Id(0), results.get(0).unwrap());
    assert_eq!(Property::Integer(42), results.get(1).unwrap());
    assert!(query.step().unwrap().is_none());
}

#[test]
fn run_delete_node() {
    let graph = Graph::open_anon().unwrap();

    let stmt = graph.prepare("CREATE (:PERSON)").unwrap();
    let mut txn = graph.mut_txn().unwrap();
    stmt.execute(&mut txn, ()).unwrap();
    txn.commit().unwrap();

    let stmt = graph.prepare("MATCH (a:PERSON) RETURN ID(a)").unwrap();
    let mut txn = graph.txn().unwrap();
    let mut query = stmt.query(&mut txn, ()).unwrap();
    assert_eq!(
        Property::Id(0),
        query.step().unwrap().unwrap().get(0).unwrap(),
    );
    assert!(query.step().unwrap().is_none());

    let del_stmt = graph.prepare("MATCH (a:PERSON) DELETE a").unwrap();
    let mut txn = graph.mut_txn().unwrap();
    del_stmt.execute(&mut txn, ()).unwrap();
    txn.commit().unwrap();

    let mut txn = graph.txn().unwrap();
    let mut query = stmt.query(&mut txn, ()).unwrap();
    assert!(query.step().unwrap().is_none());
}

#[test]
fn run_delete_edge() {
    let graph = Graph::open_anon().unwrap();

    let stmt = graph
        .prepare("CREATE (a:PERSON) CREATE (a) -[:KNOWS]-> (a)")
        .unwrap();
    let mut txn = graph.mut_txn().unwrap();
    stmt.execute(&mut txn, ()).unwrap();
    txn.commit().unwrap();

    let stmt = graph.prepare("MATCH () -[e]-> () RETURN ID(e)").unwrap();
    let mut txn = graph.txn().unwrap();
    let mut query = stmt.query(&mut txn, ()).unwrap();
    assert_eq!(1u64, query.step().unwrap().unwrap().get(0).unwrap(),);
    assert!(query.step().unwrap().is_none());

    let del_stmt = graph.prepare("MATCH () -[e]- () DELETE e").unwrap();
    let mut txn = graph.mut_txn().unwrap();
    del_stmt.execute(&mut txn, ()).unwrap();
    txn.commit().unwrap();

    let mut txn = graph.txn().unwrap();
    let mut query = stmt.query(&mut txn, ()).unwrap();
    assert!(query.step().unwrap().is_none());
}

#[test]
fn run_bad_delete() {
    let graph = Graph::open_anon().unwrap();

    let stmt = graph
        .prepare("CREATE (a:PERSON) CREATE (a) -[:KNOWS]-> (a)")
        .unwrap();
    let mut txn = graph.mut_txn().unwrap();
    stmt.execute(&mut txn, ()).unwrap();
    txn.commit().unwrap();

    let del_stmt = graph.prepare("MATCH (a:PERSON) DELETE a").unwrap();
    let mut txn = graph.mut_txn().unwrap();
    assert!(del_stmt.execute(&mut txn, ()).is_err());
}

#[test]
fn run_return_label() {
    let graph = Graph::open_anon().unwrap();

    let stmt = graph
        .prepare("CREATE (a:PERSON) CREATE (a) -[:IS]-> (a)")
        .unwrap();
    let mut txn = graph.mut_txn().unwrap();
    stmt.execute(&mut txn, ()).unwrap();
    txn.commit().unwrap();

    let labels: Vec<(String, String)> = graph
        .prepare("MATCH (a) -[e]-> (a) RETURN LABEL ( a ), LABEL(e)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| {
            Ok((m.get(0)?, m.get(1)?))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(labels, vec![("PERSON".into(), "IS".into())]);
}

#[test]
fn match_return_count() {
    let graph = Graph::open_anon().unwrap();

    let count: Vec<usize> = graph
        .prepare("RETURN 1, 2, 3, 4, 5")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| Ok(m.count()))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(count, [5]);

    let count: Vec<usize> = graph
        .prepare("RETURN 1, 2, 3, 4, 5, 6, 7")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| Ok(m.count()))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(count, [7]);

    let count: Vec<usize> = graph
        .prepare("RETURN 1, 2")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| Ok(m.count()))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(count, [2]);
}

#[test]
fn match_multiple_edges() {
    let graph = Graph::open_anon().unwrap();

    let mut txn = graph.mut_txn().unwrap();
    graph
        .prepare(
            "
            CREATE (a:PERSON)
            CREATE (b:PERSON)
            CREATE (c:PERSON)
            CREATE (a) -[:KNOWS]-> (b)
            CREATE (a) -[:KNOWS]-> (c)
            ",
        )
        .unwrap()
        .execute(&mut txn, ())
        .unwrap();
    txn.commit().unwrap();

    let mut pairs: Vec<(u64, u64)> = graph
        .prepare("MATCH (a) -> (b) RETURN ID(a), ID(b)")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| {
            Ok((m.get(0)?, m.get(1)?))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    pairs.sort_unstable();

    assert_eq!(pairs, [(0, 1), (0, 2)]);
}
