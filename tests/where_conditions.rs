use gqlite::Graph;

#[test]
fn where_a_and_b() {
    let graph = Graph::open_anon().unwrap();

    let stmt = graph.prepare("WHERE $a AND $b RETURN 'test'").unwrap();
    let is_empty = |a: bool, b: bool| -> bool {
        stmt.query_map(&mut graph.txn().unwrap(), [("a", a), ("b", b)], |_| Ok(()))
            .unwrap()
            .collect::<Result<Vec<()>, _>>()
            .unwrap()
            .is_empty()
    };

    assert!(!is_empty(true, true));
    assert!(is_empty(false, true));
    assert!(is_empty(true, false));
    assert!(is_empty(false, false));
}

#[test]
fn where_a_or_b() {
    let graph = Graph::open_anon().unwrap();

    let stmt = graph.prepare("WHERE $a OR $b RETURN 'test'").unwrap();
    let is_empty = |a: bool, b: bool| -> bool {
        stmt.query_map(&mut graph.txn().unwrap(), [("a", a), ("b", b)], |_| Ok(()))
            .unwrap()
            .collect::<Result<Vec<()>, _>>()
            .unwrap()
            .is_empty()
    };

    assert!(!is_empty(true, true));
    assert!(!is_empty(false, true));
    assert!(!is_empty(true, false));
    assert!(is_empty(false, false));
}

#[test]
fn where_a() {
    let graph = Graph::open_anon().unwrap();

    let stmt = graph.prepare("WHERE $a RETURN 'test'").unwrap();
    let is_empty = |a: bool| -> bool {
        stmt.query_map(&mut graph.txn().unwrap(), [("a", a)], |_| Ok(()))
            .unwrap()
            .collect::<Result<Vec<()>, _>>()
            .unwrap()
            .is_empty()
    };

    assert!(!is_empty(true));
    assert!(is_empty(false));
}

#[test]
fn where_not_a() {
    let graph = Graph::open_anon().unwrap();

    let stmt = graph.prepare("WHERE NOT $a RETURN 'test'").unwrap();
    let is_empty = |a: bool| -> bool {
        stmt.query_map(&mut graph.txn().unwrap(), [("a", a)], |_| Ok(()))
            .unwrap()
            .collect::<Result<Vec<()>, _>>()
            .unwrap()
            .is_empty()
    };

    assert!(is_empty(true));
    assert!(!is_empty(false));
}

#[test]
fn where_nested_conditions() {
    let graph = Graph::open_anon().unwrap();

    let is_empty = |query: &str, a: bool, b: bool, c: bool| -> bool {
        graph
            .prepare(query)
            .unwrap()
            .query_map(
                &mut graph.txn().unwrap(),
                [("a", a), ("b", b), ("c", c)],
                |_| Ok(()),
            )
            .unwrap()
            .collect::<Result<Vec<()>, _>>()
            .unwrap()
            .is_empty()
    };

    for a in [true, false] {
        for b in [true, false] {
            for c in [true, false] {
                assert_eq!(
                    a && (b || !c),
                    !is_empty("WHERE $a AND ($b OR NOT $c) RETURN 'test'", a, b, c)
                );
                assert_eq!(
                    !a && b || c,
                    !is_empty("WHERE NOT $a AND $b OR $c RETURN 'test'", a, b, c)
                );
                println!("{}, {}, {}", a, b, c);
                assert_eq!(
                    !a && b && !c,
                    !is_empty("WHERE NOT $a AND $b AND NOT $c RETURN 'test'", a, b, c)
                );
                assert_eq!(
                    a || b || c,
                    !is_empty("WHERE $a OR $b OR $c RETURN 'test'", a, b, c)
                );
            }
        }
    }
}
