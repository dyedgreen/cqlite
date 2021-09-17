use super::*;
use ast::*;

#[test]
fn match_clauses_work() {
    assert_eq!(
        cypher::query("MATCH (a) - (b) RETURN a.name "),
        Ok(Query {
            match_clauses: vec![MatchClause {
                start: Node::with_annotation(Annotation::with_name("a")),
                edges: vec![(
                    Edge::either(Annotation::empty(), vec![]),
                    Node::with_annotation(Annotation::with_name("b"))
                )],
            }],
            where_clauses: vec![],
            create_clauses: vec![],
            set_clauses: vec![],
            delete_clauses: vec![],
            return_clause: vec![Expression::property("a", "name")],
        })
    );
    assert_eq!(
        cypher::query("MATCH (a:LABEL) <- ( )\nRETURN ID(a)"),
        Ok(Query {
            match_clauses: vec![MatchClause {
                start: Node::with_annotation(Annotation::new("a", "LABEL")),
                edges: vec![(
                    Edge::left(Annotation::empty(), vec![]),
                    Node::with_annotation(Annotation::empty())
                )],
            }],
            where_clauses: vec![],
            create_clauses: vec![],
            set_clauses: vec![],
            delete_clauses: vec![],
            return_clause: vec![Expression::id_of("a")],
        })
    );
    assert_eq!(
        cypher::query(" MATCH () -> (:LABEL_ONLY) RETURN a.test"),
        Ok(Query {
            match_clauses: vec![MatchClause {
                start: Node::with_annotation(Annotation::empty()),
                edges: vec![(
                    Edge::right(Annotation::empty(), vec![]),
                    Node::with_annotation(Annotation::with_label("LABEL_ONLY"))
                )],
            }],
            where_clauses: vec![],
            create_clauses: vec![],
            set_clauses: vec![],
            delete_clauses: vec![],
            return_clause: vec![Expression::property("a", "test")],
        })
    );

    assert_eq!(
        cypher::query("MATCH \n (a)  -[edge]->  (b) RETURN ID(edge)"),
        Ok(Query {
            match_clauses: vec![MatchClause {
                start: Node::with_annotation(Annotation::with_name("a")),
                edges: vec![(
                    Edge::right(Annotation::with_name("edge"), vec![]),
                    Node::with_annotation(Annotation::with_name("b"))
                )],
            }],
            where_clauses: vec![],
            create_clauses: vec![],
            set_clauses: vec![],
            delete_clauses: vec![],
            return_clause: vec![Expression::id_of("edge")],
        })
    );
    assert_eq!(
        cypher::query("MATCH (a) <-[e:KNOWS]- (b) RETURN e.since, b.name"),
        Ok(Query {
            match_clauses: vec![MatchClause {
                start: Node::with_annotation(Annotation::with_name("a")),
                edges: vec![(
                    Edge::left(Annotation::new("e", "KNOWS"), vec![]),
                    Node::with_annotation(Annotation::with_name("b"))
                )],
            }],
            where_clauses: vec![],
            create_clauses: vec![],
            set_clauses: vec![],
            delete_clauses: vec![],
            return_clause: vec![
                Expression::property("e", "since"),
                Expression::property("b", "name"),
            ],
        })
    );
    assert_eq!(
        cypher::query("MATCH (a) -[]- (b) RETURN ID(a), $test"),
        Ok(Query {
            match_clauses: vec![MatchClause {
                start: Node::with_annotation(Annotation::with_name("a")),
                edges: vec![(
                    Edge::either(Annotation::empty(), vec![]),
                    Node::with_annotation(Annotation::with_name("b"))
                )],
            }],
            where_clauses: vec![],
            create_clauses: vec![],
            set_clauses: vec![],
            delete_clauses: vec![],
            return_clause: vec![Expression::id_of("a"), Expression::Parameter("test")],
        })
    );

    assert_eq!(
        cypher::query("MATCH (a) -> (b) - (c) RETURN a.a , b.b, c.c"),
        Ok(Query {
            match_clauses: vec![MatchClause {
                start: Node::with_annotation(Annotation::with_name("a")),
                edges: vec![
                    (
                        Edge::right(Annotation::empty(), vec![]),
                        Node::with_annotation(Annotation::with_name("b"))
                    ),
                    (
                        Edge::either(Annotation::empty(), vec![]),
                        Node::with_annotation(Annotation::with_name("c"))
                    )
                ],
            }],
            where_clauses: vec![],
            create_clauses: vec![],
            set_clauses: vec![],
            delete_clauses: vec![],
            return_clause: vec![
                Expression::property("a", "a"),
                Expression::property("b", "b"),
                Expression::property("c", "c"),
            ],
        })
    );
    assert_eq!(
        cypher::query("MATCH (a) -> (b) MATCH (b) -> (c) RETURN a.a,b.b,c.c"),
        Ok(Query {
            match_clauses: vec![
                MatchClause {
                    start: Node::with_annotation(Annotation::with_name("a")),
                    edges: vec![(
                        Edge::right(Annotation::empty(), vec![]),
                        Node::with_annotation(Annotation::with_name("b"))
                    )],
                },
                MatchClause {
                    start: Node::with_annotation(Annotation::with_name("b")),
                    edges: vec![(
                        Edge::right(Annotation::empty(), vec![]),
                        Node::with_annotation(Annotation::with_name("c"))
                    )],
                }
            ],
            where_clauses: vec![],
            create_clauses: vec![],
            set_clauses: vec![],
            delete_clauses: vec![],
            return_clause: vec![
                Expression::property("a", "a"),
                Expression::property("b", "b"),
                Expression::property("c", "c"),
            ],
        })
    );
}

#[test]
fn property_maps_work() {
    assert_eq!(
        cypher::query("MATCH (a { answer: 42, book: $book}) - (b) RETURN ID(a)"),
        Ok(Query {
            match_clauses: vec![MatchClause {
                start: Node::new(
                    Annotation::with_name("a"),
                    vec![
                        ("answer", Expression::Literal(Literal::Integer(42))),
                        ("book", Expression::Parameter("book")),
                    ]
                ),
                edges: vec![(
                    Edge::either(Annotation::empty(), vec![]),
                    Node::with_annotation(Annotation::with_name("b"))
                )],
            }],
            where_clauses: vec![],
            create_clauses: vec![],
            set_clauses: vec![],
            delete_clauses: vec![],
            return_clause: vec![Expression::id_of("a")],
        })
    );

    assert_eq!(
        cypher::query("MATCH (a) -[:KNOWS{since: 'February' } ]- (b)"),
        Ok(Query {
            match_clauses: vec![MatchClause {
                start: Node::with_annotation(Annotation::with_name("a"),),
                edges: vec![(
                    Edge::either(
                        Annotation::with_label("KNOWS"),
                        vec![("since", Expression::Literal(Literal::Text("February"))),]
                    ),
                    Node::with_annotation(Annotation::with_name("b"))
                )],
            }],
            where_clauses: vec![],
            create_clauses: vec![],
            set_clauses: vec![],
            delete_clauses: vec![],
            return_clause: vec![],
        })
    );
}

#[test]
fn where_clauses_work() {
    assert_eq!(
        cypher::query("MATCH (a) WHERE ID(a) = 42 RETURN a.name"),
        Ok(Query {
            match_clauses: vec![MatchClause {
                start: Node::with_annotation(Annotation::with_name("a")),
                edges: vec![],
            }],
            where_clauses: vec![Condition::IdEq(
                "a",
                Expression::Literal(Literal::Integer(42))
            )],
            create_clauses: vec![],
            set_clauses: vec![],
            delete_clauses: vec![],
            return_clause: vec![Expression::property("a", "name")],
        })
    );

    assert_eq!(
        cypher::query("MATCH (a) WHERE a.age >= $min_age RETURN a.age"),
        Ok(Query {
            match_clauses: vec![MatchClause {
                start: Node::with_annotation(Annotation::with_name("a")),
                edges: vec![],
            }],
            where_clauses: vec![Condition::Ge(
                Expression::Property {
                    name: "a",
                    key: "age"
                },
                Expression::Parameter("min_age"),
            )],
            create_clauses: vec![],
            set_clauses: vec![],
            delete_clauses: vec![],
            return_clause: vec![Expression::property("a", "age")],
        })
    );

    assert_eq!(
        cypher::query(
            "
                MATCH (a) -[e:KNOWS]-> (b)
                WHERE a.age > 42 AND b.name = 'Peter Parker' OR NOT e.fake
                RETURN e.since
                "
        ),
        Ok(Query {
            match_clauses: vec![MatchClause {
                start: Node::with_annotation(Annotation::with_name("a")),
                edges: vec![(
                    Edge::right(Annotation::new("e", "KNOWS"), vec![]),
                    Node::with_annotation(Annotation::with_name("b"))
                )],
            }],
            where_clauses: vec![Condition::or(
                Condition::and(
                    Condition::Gt(
                        Expression::Property {
                            name: "a",
                            key: "age",
                        },
                        Expression::Literal(Literal::Integer(42))
                    ),
                    Condition::Eq(
                        Expression::Property {
                            name: "b",
                            key: "name",
                        },
                        Expression::Literal(Literal::Text("Peter Parker"))
                    )
                ),
                Condition::not(Condition::Expression(Expression::Property {
                    name: "e",
                    key: "fake",
                })),
            )],
            create_clauses: vec![],
            set_clauses: vec![],
            delete_clauses: vec![],
            return_clause: vec![Expression::property("e", "since")],
        })
    );
}

#[test]
fn create_clauses_work() {
    assert_eq!(
        cypher::query("CREATE (node:PERSON { name: 'Peter Parker', answer: 42 }) RETURN ID(node)"),
        Ok(Query {
            match_clauses: vec![],
            where_clauses: vec![],
            create_clauses: vec![CreateClause::CreateNode {
                name: Some("node"),
                label: "PERSON",
                properties: vec![
                    ("name", Expression::Literal(Literal::Text("Peter Parker"))),
                    ("answer", Expression::Literal(Literal::Integer(42))),
                ],
            }],
            set_clauses: vec![],
            delete_clauses: vec![],
            return_clause: vec![Expression::id_of("node")],
        })
    );

    assert_eq!(
        cypher::query("MATCH (a) MATCH (b) WHERE ID(a) = $id CREATE ( a ) -[:KNOWS]-> ( b )"),
        Ok(Query {
            match_clauses: vec![
                MatchClause {
                    start: Node::with_annotation(Annotation::with_name("a")),
                    edges: vec![],
                },
                MatchClause {
                    start: Node::with_annotation(Annotation::with_name("b")),
                    edges: vec![],
                }
            ],
            where_clauses: vec![Condition::IdEq("a", Expression::Parameter("id"))],
            create_clauses: vec![CreateClause::CreateEdge {
                name: None,
                label: "KNOWS",
                origin: "a",
                target: "b",
                properties: vec![],
            }],
            set_clauses: vec![],
            delete_clauses: vec![],
            return_clause: vec![],
        })
    );
}

#[test]
fn set_clauses_work() {
    assert_eq!(
        cypher::query("MATCH (a) SET a.answer = 42"),
        Ok(Query {
            match_clauses: vec![MatchClause {
                start: Node::with_annotation(Annotation::with_name("a")),
                edges: vec![],
            }],
            where_clauses: vec![],
            create_clauses: vec![],
            set_clauses: vec![SetClause {
                name: "a",
                key: "answer",
                value: Expression::Literal(Literal::Integer(42)),
            }],
            delete_clauses: vec![],
            return_clause: vec![],
        })
    );

    assert_eq!(
        cypher::query("MATCH (a:PERSON) SET a.first = 'Peter' SET a.last = $last_name"),
        Ok(Query {
            match_clauses: vec![MatchClause {
                start: Node::with_annotation(Annotation::new("a", "PERSON")),
                edges: vec![],
            }],
            where_clauses: vec![],
            create_clauses: vec![],
            set_clauses: vec![
                SetClause {
                    name: "a",
                    key: "first",
                    value: Expression::Literal(Literal::Text("Peter")),
                },
                SetClause {
                    name: "a",
                    key: "last",
                    value: Expression::Parameter("last_name"),
                }
            ],
            delete_clauses: vec![],
            return_clause: vec![],
        })
    );
}

#[test]
fn delete_clauses_work() {
    assert_eq!(
        cypher::query("MATCH (a:DEATH_STAR) DELETE a RETURN ID(a)"),
        Ok(Query {
            match_clauses: vec![MatchClause {
                start: Node::with_annotation(Annotation::new("a", "DEATH_STAR")),
                edges: vec![],
            }],
            where_clauses: vec![],
            create_clauses: vec![],
            set_clauses: vec![],
            delete_clauses: vec!["a"],
            return_clause: vec![Expression::id_of("a")],
        })
    );

    assert_eq!(
        cypher::query("MATCH (a) -[e:KNOWS]-> (b) DELETE b DELETE e"),
        Ok(Query {
            match_clauses: vec![MatchClause {
                start: Node::with_annotation(Annotation::with_name("a")),
                edges: vec![(
                    Edge::right(Annotation::new("e", "KNOWS"), vec![]),
                    Node::with_annotation(Annotation::with_name("b")),
                )],
            }],
            where_clauses: vec![],
            create_clauses: vec![],
            set_clauses: vec![],
            delete_clauses: vec!["b", "e"],
            return_clause: vec![],
        })
    );
}
