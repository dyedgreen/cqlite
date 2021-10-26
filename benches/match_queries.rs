use cqlite::Graph;
use criterion::{black_box, criterion_group, criterion_main, Criterion};

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

pub fn short_path_where_id_eq(c: &mut Criterion) {
    let graph = build_test_graph();

    c.bench_function("match path where id eq", |b| {
        b.iter(|| {
            let stmt = graph
                .prepare("MATCH (a) - (b) WHERE ID(b) = $id RETURN a.number")
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

pub fn match_node_with_label(c: &mut Criterion) {
    let graph = Graph::open_anon().unwrap();

    let create_node = |label: &str| {
        graph
            .prepare(&format!(
                "CREATE (:{} {{ name: 'test name', number: 42 }})",
                label
            ))
            .unwrap()
    };

    let mut txn = graph.mut_txn().unwrap();
    for label in [
        "THING",
        "PERSON",
        "CAKE",
        "UNIVERSITY",
        "EXCHANGE",
        "CITY",
        "PLANET",
        "SCIENTIST",
        "BOOK",
        "COMPUTER",
    ] {
        let stmt = create_node(label);
        for _ in 0..1000 {
            stmt.execute(&mut txn, ()).unwrap();
        }
    }
    txn.commit().unwrap();

    c.bench_function("match node with label", |b| {
        b.iter(|| {
            let stmt = graph
                .prepare("MATCH (a:BOOK) RETURN a.name, a.number")
                .unwrap();
            let mut txn = graph.txn().unwrap();
            let val = stmt
                .query_map(&mut txn, (), |m| {
                    Ok((m.get::<String, _>(0)?, m.get::<i64, _>(1)?))
                })
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
    match_node_with_label,
}
criterion_main!(benches);
