#![allow(clippy::redundant_closure_call)]

use peg::{error::ParseError, str::LineCol};

pub mod ast;

#[cfg(test)]
mod tests;

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

        // e.g. '-0.53', '34346.245', '236.0'
        rule real() -> f64
            = real:$("-"? num()+ "." num()+) {? real.parse().or(Err("invalid real"))}

        // e.g. 'TRUE', 'FALSE'
        rule boolean() -> bool
            = kw_true() { true } / kw_false() { false }

        // e.g. 'hello world'
        rule text() -> &'input str
            = "'" text:$([^ '\'' | '\n' | '\r']*) "'" { text }

        // e.g. 'TRUE', '42', 'hello world'
        rule literal() -> Literal<'input>
            = r:real() { Literal::Real(r) }
            / i:integer() { Literal::Integer(i) }
            / b:boolean() { Literal::Boolean(b) }
            / t:text() { Literal::Text(t) }
            / kw_null() { Literal::Null }

        rule expression() -> Expression<'input>
            = "$" name:ident() { Expression::Parameter(name) }
            / l:literal() { Expression::Literal(l) }
            / kw_id() _* "(" _* n:ident() _* ")" { Expression::IdOf { name: n } }
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
            kw_id() _* "(" _* n:ident() _* ")" _* "=" _* e:expression() { Condition::IdEq(n, e) }
            e:expression() _* "=" _* kw_id() _* "(" _* n:ident() _* ")" { Condition::IdEq(n, e) }
            --
            a:expression() _* "="  _* b:expression() { Condition::Eq(a, b) }
            a:expression() _* "<>" _* b:expression() { Condition::Ne(a, b) }
            a:expression() _* "<"  _* b:expression() { Condition::Lt(a, b) }
            a:expression() _* "<=" _* b:expression() { Condition::Le(a, b) }
            a:expression() _* ">"  _* b:expression() { Condition::Gt(a, b) }
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

        // e.g. 'CREATE (node:LABEL { name: 'hello', answer: 42.0 })', 'CREATE (a) -[:LABEL]-> (b)'
        rule create_clause() -> CreateClause<'input>
            = kw_create() __+ n:node() {?
                let name = n.annotation.name;
                let label = n.annotation.label.ok_or("a label is required")?;
                Ok(CreateClause::CreateNode { name, label, properties: n.properties })
            }
            / kw_create() __+ "(" _* lhs:ident() _* ")" __* e:edge() __* "(" _* rhs:ident() _* ")" {?
                let name = e.annotation.name;
                let label = e.annotation.label.ok_or("a label is required")?;
                let (origin, target) = match e.direction {
                    Direction::Left => (rhs, lhs),
                    Direction::Right => (lhs, rhs),
                    Direction::Either => return Err("edge must be directed"),
                };
                Ok(CreateClause::CreateEdge {
                    name,
                    label,
                    origin,
                    target,
                    properties: e.properties
                })
            }

        // e.g. 'SET a.name = 'Peter Parker''
        rule set_clause() -> SetClause<'input>
            = kw_set() __+ p:property() _* "=" _* e:expression() {
                SetClause { name: p.0, key: p.1, value: e }
            }

        // e.g. 'DELETE a'
        rule delete_clause() -> &'input str
            = kw_delete() __+ name:ident() { name }

        // e.g. 'RETURN a, b'
        rule return_clause() -> Vec<Expression<'input>>
            = kw_return() __+ items:( expression() ++ (__* "," __*) ) { items }

        pub rule query() -> Query<'input>
            = __*
              match_clauses:( match_clause() ** (__+) )
              where_clauses:( __* w:( where_clause() ** (__+) )? { w.unwrap_or_else(Vec::new) } )
              create_clauses:( __* c:(create_clause() ** (__+) )? { c.unwrap_or_else(Vec::new) } )
              set_clauses:( __* s:(set_clause() ** (__+) )? { s.unwrap_or_else(Vec::new) } )
              delete_clauses:( __* d:(delete_clause() ** (__+) )? { d.unwrap_or_else(Vec::new) } )
              return_clause:( __* r:return_clause()? { r.unwrap_or_else(Vec::new) })
              __* {
                Query {
                    match_clauses,
                    where_clauses,
                    create_clauses,
                    set_clauses,
                    delete_clauses,
                    return_clause,
                }
            }
    }
}

pub fn parse(input: &str) -> Result<ast::Query<'_>, ParseError<LineCol>> {
    cypher::query(input)
}
