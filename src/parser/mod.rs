use peg::{error::ParseError, str::LineCol};

pub mod ast;

peg::parser! {
    grammar cypher() for str {
        use ast::*;

        rule kw_match()     = "MATCH"
        rule kw_create()    = "CREATE"
        rule kw_set()       = "SET"
        rule kw_delete()    = "DELETE"
        rule kw_where()     = "WHERE"
        rule kw_return()    = "RETURN"
        rule kw_true()      = "TRUE"
        rule kw_false()     = "FALSE"
        rule kw_null()      = "NULL"
        rule kw_and()       = "AND"
        rule kw_or()        = "OR"
        rule kw_not()       = "NOT"
        rule kw_id()        = "ID"

        rule _()
            = [' ']

        rule __()
            = [' ' | '\n' | '\t']

        rule alpha()
            = ['a'..='z' | 'A'..='Z']

        rule num()
            = ['0'..='9']

        rule alpha_num()
            = ['a'..='z' | 'A'..='Z' | '0'..='9' | '_']


        // e.g. '42', '-1'
        rule integer() -> i64
            = integer:$("-"?num()+) {? integer.parse().or(Err("invalid integer")) }

        // e.g. '-0.53', '34346.245', '236'
        rule real() -> f64
            = real:$("-"? num()+ ("." num()+)?) {? real.parse().or(Err("invalid real"))}

        // e.g. 'TRUE', 'FALSE'
        rule boolean() -> bool
            = kw_true() { true } / kw_false() { false }

        // e.g. 'hello world'
        rule text() -> &'input str
            = "'" text:$([^ '\'' | '\n' | '\r']*) "'" { text }

        // e.g. 'TRUE', '42', 'hello world'
        rule literal() -> Literal<'input>
            = i:integer() { Literal::Integer(i) }
            / r:real() { Literal::Real(r) }
            / b:boolean() { Literal::Boolean(b) }
            / t:text() { Literal::Text(t) }
            / kw_null() { Literal::Null }

        rule expression() -> Expression<'input>
            = "$" name:ident() { Expression::Parameter(name) }
            / l:literal() { Expression::Literal(l) }
            / p:property() { Expression::Property { name: p.0, key: p.1 } }

        // e.g. 'hello_world', 'Rust', 'HAS_PROPERTY'
        rule ident() -> &'input str
            = ident:$(alpha()alpha_num()*) { ident }


        // e.g. 'a', 'a : PERSON', ': KNOWS'
        rule annotation() -> Annotation<'input>
            = name:ident()? label:( _* ":" _* k:ident() { k } )? { Annotation { name, label } }

        // e.g. '{answer: 42, book: 'Hitchhikers Guide'}'
        rule property_map() -> Vec<(&'input str, Expression<'input>)>
            = "{" __* entries:( (k:ident() _* ":" _* v:expression() { (k, v) }) ++ (_* "," _*) ) __* "}" { entries }

        // e.g. '()', '( a:PERSON )', '(b)', '(a : OTHER_THING)'
        rule node() -> Node<'input>
            = "(" _* a:annotation() _* p:property_map()? _* ")" {
                Node::new(a, p.unwrap_or_else(Vec::new))
            }

        // e.g. '-', '<-', '-[ name:KIND ]-', '<-[name]-'
        rule edge() -> Edge<'input>
            =  "-[" _* a:annotation() _* p:property_map()? _* "]->" {
                Edge::right(a, p.unwrap_or_else(Vec::new))
            }
            /  "-[" _* a:annotation() _* p:property_map()? _* "]-"  {
                Edge::either(a, p.unwrap_or_else(Vec::new))
            }
            / "<-[" _* a:annotation() _* p:property_map()? _* "]-"  {
                Edge::left(a, p.unwrap_or_else(Vec::new))
            }
            / "<-" { Edge::left(Annotation::empty(), Vec::new()) }
            / "->" { Edge::right(Annotation::empty(), Vec::new()) }
            / "-" { Edge::either(Annotation::empty(), Vec::new()) }


        rule property() -> (&'input str, &'input str)
            = name:ident() "." key:ident() { (name, key) }

        rule condition() -> Condition<'input>= precedence!{
            a:(@) __* kw_and() __* b:@ { Condition::and(a, b) }
            a:(@) __* kw_or() __* b:@ { Condition::or(a, b) }
            --
            kw_not() _* c:(@) { Condition::not(c) }
            --
            a:expression() _* "="  _* b:expression() { Condition::Eq(a, b) }
            a:expression() _* "<>" _* b:expression() { Condition::Ne(a, b) }
            a:expression() _* "<"  _* b:expression() { Condition::Lt(a, b) }
            a:expression() _* "<=" _* b:expression() { Condition::Le(a, b) }
            a:expression() _* ">"  _* b:expression() { Condition::Gt(a, b) }
            a:expression() _* ">=" _* b:expression() { Condition::Ge(a, b) }
            kw_id() _* "(" _* n:ident() _* ")" _* "=" _* e:expression() { Condition::IdEq(n, e) }
            e:expression() _* "=" _* kw_id() _* "(" _* n:ident() _* ")" { Condition::IdEq(n, e) }
            --
            e:expression() { Condition::Expression(e) }
            "(" __* c:condition() __* ")" { c }
        }


        // e.g. 'MATCH (a)', 'MATCH (a) -> (b) <- (c)', ...
        rule match_clause() -> MatchClause<'input>
            = kw_match() __+ start:node()
              edges:( (__* e:edge() __* n:node() { (e, n) }) ** "" ) {
                MatchClause { start, edges }
            }

        // e.g. 'WHERE a.name <> b.name', 'WHERE a.age > b.age AND a.age <= 42'
        rule where_clause() -> Condition<'input>
            = kw_where() __+ c:condition() { c }

        // e.g. 'SET a.name = 'Peter Parker''
        rule set_clause() -> SetClause<'input>
            = kw_set() __+ p:property() _* "=" _* e:expression() {
                SetClause { name: p.0, key: p.1, value: e }
            }

        // e.g. 'DELETE a'
        rule delete_clause() -> &'input str
            = kw_delete() __+ name:ident() { name }

        // e.g. 'RETURN a, b'
        rule return_clause() -> Vec<&'input str>
            = kw_return() __+ items:( ident() ++ (__* "," __*) ) { items }

        pub rule query() -> Query<'input>
            = __*
              match_clauses:( match_clause() ** (__+) )
              where_clauses:( __* w:( where_clause() ** (__+) )? { w.unwrap_or_else(Vec::new) } )
              set_clauses:( __* s:(set_clause() ** (__+) )? { s.unwrap_or_else(Vec::new) } )
              delete_clauses:( __* d:(delete_clause() ** (__+) )? { d.unwrap_or_else(Vec::new) } )
              return_clause:( __* r:return_clause()? { r.unwrap_or_else(Vec::new) })
              __* {
                Query { match_clauses, where_clauses, set_clauses, delete_clauses, return_clause }
            }
    }
}

pub fn parse(input: &str) -> Result<ast::Query<'_>, ParseError<LineCol>> {
    cypher::query(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::*;

    #[test]
    fn match_clauses_work() {
        assert_eq!(
            cypher::query("MATCH (a) - (b) RETURN a "),
            Ok(Query {
                match_clauses: vec![MatchClause {
                    start: Node::with_annotation(Annotation::with_name("a")),
                    edges: vec![(
                        Edge::either(Annotation::empty(), vec![]),
                        Node::with_annotation(Annotation::with_name("b"))
                    )],
                }],
                where_clauses: vec![],
                set_clauses: vec![],
                delete_clauses: vec![],
                return_clause: vec!["a"],
            })
        );
        assert_eq!(
            cypher::query("MATCH (a:LABEL) <- ( )\nRETURN a"),
            Ok(Query {
                match_clauses: vec![MatchClause {
                    start: Node::with_annotation(Annotation::new("a", "LABEL")),
                    edges: vec![(
                        Edge::left(Annotation::empty(), vec![]),
                        Node::with_annotation(Annotation::empty())
                    )],
                }],
                where_clauses: vec![],
                set_clauses: vec![],
                delete_clauses: vec![],
                return_clause: vec!["a"],
            })
        );
        assert_eq!(
            cypher::query(" MATCH () -> (:LABEL_ONLY) RETURN a"),
            Ok(Query {
                match_clauses: vec![MatchClause {
                    start: Node::with_annotation(Annotation::empty()),
                    edges: vec![(
                        Edge::right(Annotation::empty(), vec![]),
                        Node::with_annotation(Annotation::with_label("LABEL_ONLY"))
                    )],
                }],
                where_clauses: vec![],
                set_clauses: vec![],
                delete_clauses: vec![],
                return_clause: vec!["a"],
            })
        );

        assert_eq!(
            cypher::query("MATCH \n (a)  -[edge]->  (b) RETURN a"),
            Ok(Query {
                match_clauses: vec![MatchClause {
                    start: Node::with_annotation(Annotation::with_name("a")),
                    edges: vec![(
                        Edge::right(Annotation::with_name("edge"), vec![]),
                        Node::with_annotation(Annotation::with_name("b"))
                    )],
                }],
                where_clauses: vec![],
                set_clauses: vec![],
                delete_clauses: vec![],
                return_clause: vec!["a"],
            })
        );
        assert_eq!(
            cypher::query("MATCH (a) <-[e:KNOWS]- (b) RETURN e, b"),
            Ok(Query {
                match_clauses: vec![MatchClause {
                    start: Node::with_annotation(Annotation::with_name("a")),
                    edges: vec![(
                        Edge::left(Annotation::new("e", "KNOWS"), vec![]),
                        Node::with_annotation(Annotation::with_name("b"))
                    )],
                }],
                where_clauses: vec![],
                set_clauses: vec![],
                delete_clauses: vec![],
                return_clause: vec!["e", "b"],
            })
        );
        assert_eq!(
            cypher::query("MATCH (a) -[]- (b) RETURN a, b"),
            Ok(Query {
                match_clauses: vec![MatchClause {
                    start: Node::with_annotation(Annotation::with_name("a")),
                    edges: vec![(
                        Edge::either(Annotation::empty(), vec![]),
                        Node::with_annotation(Annotation::with_name("b"))
                    )],
                }],
                where_clauses: vec![],
                set_clauses: vec![],
                delete_clauses: vec![],
                return_clause: vec!["a", "b"],
            })
        );

        assert_eq!(
            cypher::query("MATCH (a) -> (b) - (c) RETURN a , b, c"),
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
                set_clauses: vec![],
                delete_clauses: vec![],
                return_clause: vec!["a", "b", "c"],
            })
        );
        assert_eq!(
            cypher::query("MATCH (a) -> (b) MATCH (b) -> (c) RETURN a,b,c"),
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
                set_clauses: vec![],
                delete_clauses: vec![],
                return_clause: vec!["a", "b", "c"],
            })
        );
    }

    #[test]
    fn property_maps_work() {
        assert_eq!(
            cypher::query("MATCH (a { answer: 42, book: $book}) - (b) RETURN a "),
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
                set_clauses: vec![],
                delete_clauses: vec![],
                return_clause: vec!["a"],
            })
        );

        assert_eq!(
            cypher::query("MATCH (a) -[:KNOWS{since: 'February' } ]- (b) RETURN a "),
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
                set_clauses: vec![],
                delete_clauses: vec![],
                return_clause: vec!["a"],
            })
        );
    }

    #[test]
    fn where_clauses_work() {
        assert_eq!(
            cypher::query("MATCH (a) WHERE ID(a) = 42 RETURN a"),
            Ok(Query {
                match_clauses: vec![MatchClause {
                    start: Node::with_annotation(Annotation::with_name("a")),
                    edges: vec![],
                }],
                where_clauses: vec![Condition::IdEq(
                    "a",
                    Expression::Literal(Literal::Integer(42))
                )],
                set_clauses: vec![],
                delete_clauses: vec![],
                return_clause: vec!["a"],
            })
        );

        assert_eq!(
            cypher::query("MATCH (a) WHERE a.age >= $min_age RETURN a"),
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
                set_clauses: vec![],
                delete_clauses: vec![],
                return_clause: vec!["a"],
            })
        );

        assert_eq!(
            cypher::query(
                "
                MATCH (a) -[e:KNOWS]-> (b)
                WHERE a.age > 42 AND b.name = 'Peter Parker' OR NOT e.fake
                RETURN e
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
                set_clauses: vec![],
                delete_clauses: vec![],
                return_clause: vec!["e"],
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
            cypher::query("MATCH (a:DEATH_STAR) DELETE a RETURN a"),
            Ok(Query {
                match_clauses: vec![MatchClause {
                    start: Node::with_annotation(Annotation::new("a", "DEATH_STAR")),
                    edges: vec![],
                }],
                where_clauses: vec![],
                set_clauses: vec![],
                delete_clauses: vec!["a"],
                return_clause: vec!["a"],
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
                set_clauses: vec![],
                delete_clauses: vec!["b", "e"],
                return_clause: vec![],
            })
        );
    }
}
