use cqlite::Graph;
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn simple_bench(
    c: &mut Criterion,
    name: &str,
    init: impl Fn(&Graph),
    mut bench: impl FnMut(&Graph),
) {
    std::fs::remove_file("bench.graph").ok();
    let graph = Graph::open("bench.graph").unwrap();
    init(&graph);
    c.bench_function(name, |b| b.iter(|| bench(&graph)));
    std::fs::remove_file("bench.graph").ok();
    let graph = Graph::open_anon().unwrap();
    init(&graph);
    c.bench_function(&format!("{} (open anon)", name), |b| {
        b.iter(|| bench(&graph))
    });
}

pub fn create_nodes(c: &mut Criterion) {
    simple_bench(
        c,
        "create 1000 nodes",
        |_| {},
        |graph| {
            let mut txn = graph.mut_txn().unwrap();
            let stmt = graph.prepare("CREATE (:TEST { number: $num })").unwrap();
            for num in 0..1000 {
                stmt.execute(&mut txn, ("num", black_box(num))).unwrap();
            }
            txn.commit().unwrap();
        },
    );
}

pub fn create_edges(c: &mut Criterion) {
    simple_bench(
        c,
        "create 1000 edges",
        |graph| {
            let mut txn = graph.mut_txn().unwrap();
            graph
                .prepare("CREATE (:TEST)")
                .unwrap()
                .execute(&mut txn, ())
                .unwrap();
            txn.commit().unwrap();
        },
        |graph| {
            let mut txn = graph.mut_txn().unwrap();
            let stmt = graph
                .prepare("MATCH (n) CREATE (n) -[:TEST { number: $num }]-> (n)")
                .unwrap();
            for num in 0..1000 {
                stmt.execute(&mut txn, ("num", black_box(num))).unwrap();
            }
            txn.commit().unwrap();
        },
    );
}

pub fn match_node_by_id(c: &mut Criterion) {
    simple_bench(
        c,
        "match node by id",
        |graph| {
            let mut txn = graph.mut_txn().unwrap();
            let stmt = graph.prepare("CREATE (:TEST { number: $num })").unwrap();
            for num in 0..1000 {
                stmt.execute(&mut txn, ("num", num)).unwrap();
            }
            txn.commit().unwrap();
        },
        |graph| {
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
        },
    );
}

pub fn match_nodes_where(c: &mut Criterion) {
    simple_bench(
        c,
        "match nodes where",
        |graph| {
            let mut txn = graph.mut_txn().unwrap();
            let stmt = graph.prepare("CREATE (:TEST { number: $num })").unwrap();
            for num in 0..1000 {
                stmt.execute(&mut txn, ("num", num)).unwrap();
            }
            txn.commit().unwrap();
        },
        |graph| {
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
        },
    );
}

criterion_group! {
    benches,
    create_nodes,
    create_edges,
    match_node_by_id,
    match_nodes_where,
}
criterion_main!(benches);
