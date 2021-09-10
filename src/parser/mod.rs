use peg::{error::ParseError, str::LineCol};

pub mod ast;

peg::parser! {
    grammar cypher() for str {
        use ast::*;

        rule kw_match()  = "MATCH"
        rule kw_where()  = "WHERE"
        rule kw_return() = "RETURN"
        rule kw_true()   = "TRUE"
        rule kw_false()  = "FALSE"
        rule kw_null() = "NULL"
        rule kw_and() = "AND"
        rule kw_or() = "OR"
        rule kw_not() = "NOT"
        rule kw_id() = "ID"

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


        // e.g. 'hello_world', 'Rust', 'HAS_PROPERTY'
        rule ident() -> &'input str
            = ident:$(alpha()alpha_num()*) { ident }

        // e.g. 'a', 'a : PERSON', ': KNOWS'
        rule label() -> Label<'input>
            = name:ident()? kind:( _* ":" _* k:ident() { k } )? { Label { name, kind } }

        // e.g. '()', '( a:PERSON )', '(b)', '(a : OTHER_THING)'
        rule node() -> Node<'input>
            = "(" _* label:label() _* ")" { Node::with_label(label) }

        // e.g. '-', '<-', '-[ name:KIND ]-', '<-[name]-'
        rule edge() -> Edge<'input>
            =  "-[" _* l:label() _* "]->" { Edge::right(l) }
            /  "-[" _* l:label() _* "]-"  { Edge::either(l) }
            / "<-[" _* l:label() _* "]-"  { Edge::left(l) }
            / "<-" { Edge::left(Label::empty()) }
            / "->" { Edge::right(Label::empty()) }
            / "-" { Edge::either(Label::empty()) }


        rule expression() -> Expression<'input>
            = "?" { Expression::Placeholder }
            / l:literal() { Expression::Literal(l) }
            / name:ident() "." key:ident() { Expression::Property { name, key } }
            / kw_id() _* "(" _* name:ident() _* ")" { Expression::IdOf(name) }

        rule condition() -> Condition<'input>= precedence!{
            a:(@) __* kw_and() __* b:@ { Condition::and(a, b) }
            a:(@) __* kw_or() __* b:@ { Condition::or(a, b) }
            --
            kw_not() _* c:(@) { Condition::not(c) }
            --
            a:expression() _* "=" _* b:expression() { Condition::Eq(a, b) }
            a:expression() _* "<>" _* b:expression() { Condition::Ne(a, b) }
            a:expression() _* "<" _* b:expression() { Condition::Lt(a, b) }
            a:expression() _* "<=" _* b:expression() { Condition::Le(a, b) }
            a:expression() _* ">" _* b:expression() { Condition::Gt(a, b) }
            a:expression() _* ">=" _* b:expression() { Condition::Ge(a, b) }
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

        // e.g. 'RETURN a, b'
        rule return_clause() -> Vec<&'input str>
            = kw_return() __+ items:( ident() ++ (__* "," __*) ) { items }

        pub rule query() -> Query<'input>
            = __*
              match_clauses:( match_clause() ** (__+) )
              where_clauses:( __* w:( where_clause() ** (__+) )? { w.unwrap_or_else(Vec::new) } )
              return_clause:( __* r:return_clause()? { r.unwrap_or_else(Vec::new) })
              __* { Query { match_clauses, where_clauses, return_clause } }
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
                    start: Node::with_label(Label::with_name("a")),
                    edges: vec![(
                        Edge::either(Label::empty()),
                        Node::with_label(Label::with_name("b"))
                    )],
                }],
                where_clauses: vec![],
                return_clause: vec!["a"],
            })
        );
        assert_eq!(
            cypher::query("MATCH (a:KIND) <- ( )\nRETURN a"),
            Ok(Query {
                match_clauses: vec![MatchClause {
                    start: Node::with_label(Label::new("a", "KIND")),
                    edges: vec![(Edge::left(Label::empty()), Node::with_label(Label::empty()))],
                }],
                where_clauses: vec![],
                return_clause: vec!["a"],
            })
        );
        assert_eq!(
            cypher::query(" MATCH () -> (:KIND_ONLY) RETURN a"),
            Ok(Query {
                match_clauses: vec![MatchClause {
                    start: Node::with_label(Label::empty()),
                    edges: vec![(
                        Edge::right(Label::empty()),
                        Node::with_label(Label::with_kind("KIND_ONLY"))
                    )],
                }],
                where_clauses: vec![],
                return_clause: vec!["a"],
            })
        );

        assert_eq!(
            cypher::query("MATCH \n (a)  -[edge]->  (b) RETURN a"),
            Ok(Query {
                match_clauses: vec![MatchClause {
                    start: Node::with_label(Label::with_name("a")),
                    edges: vec![(
                        Edge::right(Label::with_name("edge")),
                        Node::with_label(Label::with_name("b"))
                    )],
                }],
                where_clauses: vec![],
                return_clause: vec!["a"],
            })
        );
        assert_eq!(
            cypher::query("MATCH (a) <-[e:KNOWS]- (b) RETURN e, b"),
            Ok(Query {
                match_clauses: vec![MatchClause {
                    start: Node::with_label(Label::with_name("a")),
                    edges: vec![(
                        Edge::left(Label::new("e", "KNOWS")),
                        Node::with_label(Label::with_name("b"))
                    )],
                }],
                where_clauses: vec![],
                return_clause: vec!["e", "b"],
            })
        );
        assert_eq!(
            cypher::query("MATCH (a) -[]- (b) RETURN a, b"),
            Ok(Query {
                match_clauses: vec![MatchClause {
                    start: Node::with_label(Label::with_name("a")),
                    edges: vec![(
                        Edge::either(Label::empty()),
                        Node::with_label(Label::with_name("b"))
                    )],
                }],
                where_clauses: vec![],
                return_clause: vec!["a", "b"],
            })
        );

        assert_eq!(
            cypher::query("MATCH (a) -> (b) - (c) RETURN a , b, c"),
            Ok(Query {
                match_clauses: vec![MatchClause {
                    start: Node::with_label(Label::with_name("a")),
                    edges: vec![
                        (
                            Edge::right(Label::empty()),
                            Node::with_label(Label::with_name("b"))
                        ),
                        (
                            Edge::either(Label::empty()),
                            Node::with_label(Label::with_name("c"))
                        )
                    ],
                }],
                where_clauses: vec![],
                return_clause: vec!["a", "b", "c"],
            })
        );
        assert_eq!(
            cypher::query("MATCH (a) -> (b) MATCH (b) -> (c) RETURN a,b,c"),
            Ok(Query {
                match_clauses: vec![
                    MatchClause {
                        start: Node::with_label(Label::with_name("a")),
                        edges: vec![(
                            Edge::right(Label::empty()),
                            Node::with_label(Label::with_name("b"))
                        )],
                    },
                    MatchClause {
                        start: Node::with_label(Label::with_name("b")),
                        edges: vec![(
                            Edge::right(Label::empty()),
                            Node::with_label(Label::with_name("c"))
                        )],
                    }
                ],
                where_clauses: vec![],
                return_clause: vec!["a", "b", "c"],
            })
        );
    }

    #[test]
    fn where_clauses_work() {
        assert_eq!(
            cypher::query("MATCH (a) WHERE ID(a) = 42 RETURN a"),
            Ok(Query {
                match_clauses: vec![MatchClause {
                    start: Node::with_label(Label::with_name("a")),
                    edges: vec![],
                }],
                where_clauses: vec![Condition::Eq(
                    Expression::IdOf("a"),
                    Expression::Literal(Literal::Integer(42))
                )],
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
                    start: Node::with_label(Label::with_name("a")),
                    edges: vec![(
                        Edge::right(Label::new("e", "KNOWS")),
                        Node::with_label(Label::with_name("b"))
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
                return_clause: vec!["e"],
            })
        );
    }
}
