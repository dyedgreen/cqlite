mod build;
mod plan;

pub(crate) use plan::{Filter, LoadProperty, MatchStep, QueryPlan, UpdateStep};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast;
    use plan::*;

    #[test]
    fn build_a_to_b() {
        // (a) -> (b)
        let query = ast::Query {
            match_clauses: vec![ast::MatchClause {
                start: ast::Node::with_annotation(ast::Annotation::with_name("a")),
                edges: vec![(
                    ast::Edge::right(ast::Annotation::empty(), vec![]),
                    ast::Node::with_annotation(ast::Annotation::with_name("b")),
                )],
            }],
            where_clauses: vec![],
            set_clauses: vec![],
            delete_clauses: vec![],
            return_clause: vec![
                ast::Expression::property("a", "name"),
                ast::Expression::property("b", "name"),
            ],
        };

        let plan = QueryPlan {
            steps: vec![
                MatchStep::LoadAnyNode { name: 0 },
                MatchStep::LoadOriginEdge { name: 1, node: 0 },
                MatchStep::LoadTargetNode { name: 2, edge: 1 },
            ],
            updates: vec![],
            returns: vec![
                LoadProperty::PropertyOfNode {
                    node: 0,
                    key: "name".into(),
                },
                LoadProperty::PropertyOfNode {
                    node: 2,
                    key: "name".into(),
                },
            ],
        };

        assert_eq!(plan, QueryPlan::new(&query).unwrap());
    }
}
