use criterion::{black_box, criterion_group, criterion_main, Criterion};
use gqlite::Graph;

fn build_test_graph() -> Graph {
    let graph = Graph::open_anon().unwrap();
    let mut txn = graph.mut_txn().unwrap();

    let create_node = graph
        .prepare("CREATE (:PERSON { name: $name, number: $num })")
        .unwrap();
    let create_edge = graph
        .prepare(
            "
            MATCH (a) MATCH (b)
            WHERE ID(a) = $a AND ID(b) = $b
            CREATE (a) -[:KNOWS]-> (b)
            ",
        )
        .unwrap();

    let names = ["Peter Parker", "Clark Kent", "Stacey", "Bruce"];
    for num in 0..1000 {
        create_node
            .execute(
                &mut txn,
                (("num", num), ("name", names[num as usize % names.len()])),
            )
            .unwrap();
    }
    for num in 0..1000 {
        let start = num;
        let end = (num + 42) % 1000;
        create_edge
            .execute(&mut txn, (("a", start), ("b", end)))
            .unwrap();
    }

    txn.commit().unwrap();
    graph
}

pub fn long_path_where_id_eq(c: &mut Criterion) {
    let graph = build_test_graph();

    c.bench_function("match path where id eq", |b| {
        b.iter(|| {
            let stmt = graph
                .prepare("MATCH (a) -> (b) <- (c) WHERE ID(c) = $id RETURN a.number")
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

criterion_group! {
    benches,
    long_path_where_id_eq,
}
criterion_main!(benches);
