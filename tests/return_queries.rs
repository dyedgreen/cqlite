use gqlite::{Graph, Property};

#[test]
fn return_parameter() {
    let graph = Graph::open_anon().unwrap();

    let stmt = graph.prepare("RETURN $val").unwrap();

    let res: Vec<Property> = stmt
        .query_map(&mut graph.txn().unwrap(), ("val", 1u64), |m| m.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(vec![Property::Id(1)], res);

    let res: Vec<Property> = stmt
        .query_map(&mut graph.txn().unwrap(), ("val", 42), |m| m.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(vec![Property::Integer(42)], res);

    let res: Vec<Property> = stmt
        .query_map(&mut graph.txn().unwrap(), ("val", 0.5), |m| m.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(vec![Property::Real(0.5)], res);

    let res: Vec<Property> = stmt
        .query_map(&mut graph.txn().unwrap(), ("val", true), |m| m.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(vec![Property::Boolean(true)], res);

    let res: Vec<Property> = stmt
        .query_map(&mut graph.txn().unwrap(), ("val", "Hello World!"), |m| {
            m.get(0)
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(vec![Property::Text("Hello World!".into())], res);

    let res: Vec<Property> = stmt
        .query_map(&mut graph.txn().unwrap(), ("val", b"Hello World!"), |m| {
            m.get(0)
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(vec![Property::Blob(b"Hello World!".to_vec())], res);

    let res: Vec<Property> = stmt
        .query_map(&mut graph.txn().unwrap(), (), |m| m.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(vec![Property::Null], res);
}

#[test]
fn create_and_return() {
    let graph = Graph::open_anon().unwrap();

    let mut txn = graph.mut_txn().unwrap();
    let vals = graph
        .prepare("CREATE (a:TEST { name: 'Peter Parker', age: 42 }) RETURN a.name, a.age")
        .unwrap()
        .query_map(&mut txn, (), |m| Ok((m.get(0)?, m.get(1)?)))
        .unwrap()
        .collect::<Result<Vec<(String, i64)>, _>>()
        .unwrap();
    assert_eq!(vals, vec![("Peter Parker".into(), 42)]);
    txn.commit().unwrap();

    let vals = graph
        .prepare("MATCH (a) RETURN a.name, a.age")
        .unwrap()
        .query_map(&mut graph.txn().unwrap(), (), |m| {
            Ok((m.get(0)?, m.get(1)?))
        })
        .unwrap()
        .collect::<Result<Vec<(String, i64)>, _>>()
        .unwrap();
    assert_eq!(vals, vec![("Peter Parker".into(), 42)]);
}
