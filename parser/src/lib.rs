use peg::{error::ParseError, str::LineCol};

pub mod ast;

peg::parser! {
    grammar cypher() for str {
        use ast::*;

        rule _()
            = [' ']

        rule __()
            = [' ' | '\n']

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

        // e.g. 'hello_world', 'Rust', 'HAS_PROPERTY'
        rule ident() -> &'input str
            = ident:$(alpha()alpha_num()*) { ident }

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

        // e.g. 'MATCH (a)', 'MATCH (a) -> (b) <- (c)', ...
        rule match_clause() -> MatchClause<'input>
            = "MATCH" __+ start:node()
              edges:( (__* e:edge() __* n:node() { (e, n) }) ** "" ) {
                MatchClause { start, edges }
            }

        rule return_clause() -> Vec<&'input str>
            = "RETURN" __+ items:( ident() ++ (__* "," __*) ) { items }

        pub rule query() -> Query<'input>
            = __*
              match_clauses:( match_clause() ** (__+) )
              return_clause:( r:(__+ r:return_clause() {r})? { r.unwrap_or_else(Vec::new) } )
              __* { Query { match_clauses, create_clause: (), return_clause } }
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
        // simple
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
                create_clause: (),
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
                create_clause: (),
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
                create_clause: (),
                return_clause: vec!["a"],
            })
        );

        // fat edges
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
                create_clause: (),
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
                create_clause: (),
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
                create_clause: (),
                return_clause: vec!["a", "b"],
            })
        );

        // match multiple
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
                create_clause: (),
                return_clause: vec!["a", "b", "c"],
            })
        );
    }
}
