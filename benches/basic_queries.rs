use criterion::{black_box, criterion_group, criterion_main, Criterion};
use gqlite::Graph;

pub fn open_anon_create_nodes(c: &mut Criterion) {
    let graph = Graph::open_anon().unwrap();
    c.bench_function("create 1000 nodes", |b| {
        b.iter(|| {
            let mut txn = graph.mut_txn().unwrap();
            let stmt = graph.prepare("CREATE (:TEST { number: $num })").unwrap();
            for num in 0..1000 {
                stmt.execute(&mut txn, ("num", black_box(num))).unwrap();
            }
            txn.commit().unwrap();
        })
    });
}

pub fn open_anon_create_edges(c: &mut Criterion) {
    let graph = Graph::open_anon().unwrap();
    let mut txn = graph.mut_txn().unwrap();
    graph
        .prepare("CREATE (:TEST)")
        .unwrap()
        .execute(&mut txn, ())
        .unwrap();
    txn.commit().unwrap();

    c.bench_function("create 1000 edges", |b| {
        b.iter(|| {
            let mut txn = graph.mut_txn().unwrap();
            let stmt = graph
                .prepare("MATCH (n) CREATE (n) -[:TEST { number: $num }]-> (n)")
                .unwrap();
            for num in 0..1000 {
                stmt.execute(&mut txn, ("num", black_box(num))).unwrap();
            }
            txn.commit().unwrap();
        })
    });
}

pub fn open_anon_match_node_by_id(c: &mut Criterion) {
    let graph = Graph::open_anon().unwrap();
    let mut txn = graph.mut_txn().unwrap();
    let stmt = graph.prepare("CREATE (:TEST { number: $num })").unwrap();
    for num in 0..1000 {
        stmt.execute(&mut txn, ("num", num)).unwrap();
    }
    txn.commit().unwrap();

    c.bench_function("match node by id", |b| {
        b.iter(|| {
            let stmt = graph
                .prepare("MATCH (n) WHERE ID(n) = $id RETURN n.number")
                .unwrap();
            let mut txn = graph.txn().unwrap();
            let val = stmt
                .query_map(&mut txn, ("id", black_box(42)), |m| m.get::<i64, _>(0))
                .unwrap()
                .last()
                .unwrap()
                .unwrap();
            black_box(val);
        })
    });
}

pub fn open_anon_match_nodes_where(c: &mut Criterion) {
    let graph = Graph::open_anon().unwrap();
    let mut txn = graph.mut_txn().unwrap();
    let stmt = graph.prepare("CREATE (:TEST { number: $num })").unwrap();
    for num in 0..1000 {
        stmt.execute(&mut txn, ("num", num)).unwrap();
    }
    txn.commit().unwrap();

    c.bench_function("match nodes where", |b| {
        b.iter(|| {
            let stmt = graph
                .prepare("MATCH (n) WHERE n.number = $num RETURN ID(n)")
                .unwrap();
            let mut txn = graph.txn().unwrap();
            let val = stmt
                .query_map(&mut txn, ("num", black_box(42)), |m| m.get::<u64, _>(0))
                .unwrap()
                .last()
                .unwrap()
                .unwrap();
            black_box(val);
        })
    });
}

criterion_group! {
    benches,
    open_anon_create_nodes,
    open_anon_create_edges,
    open_anon_match_node_by_id,
    open_anon_match_nodes_where,
}
criterion_main!(benches);
