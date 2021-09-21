use criterion::{black_box, criterion_group, criterion_main, Criterion};
use gqlite::Graph;

pub fn create_nodes_in_memory(c: &mut Criterion) {
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

pub fn create_edges_in_memory(c: &mut Criterion) {
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

pub fn match_nodes_where_in_memory(c: &mut Criterion) {
    let graph = Graph::open_anon().unwrap();
    let mut txn = graph.mut_txn().unwrap();
    let stmt = graph.prepare("CREATE (:TEST { number: $num })").unwrap();
    for num in 0..1000 {
        stmt.execute(&mut txn, ("num", black_box(num))).unwrap();
    }
    txn.commit().unwrap();

    c.bench_function("match nodes where", |b| {
        b.iter(|| {
            let stmt = graph
                .prepare("MATCH (n) WHERE n.number = $num RETURN ID(n)")
                .unwrap();
            let mut txn = graph.txn().unwrap();
            for num in 0..1000 {
                let val = stmt
                    .query_map(&mut txn, ("num", black_box(num)), |m| m.get::<u64, _>(0))
                    .unwrap()
                    .last()
                    .unwrap()
                    .unwrap();
                black_box(val);
            }
        })
    });
}

criterion_group!(
    benches,
    create_nodes_in_memory,
    create_edges_in_memory,
    match_nodes_where_in_memory
);
criterion_main!(benches);
