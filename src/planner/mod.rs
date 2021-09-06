mod build;
mod compile;
mod plan;

pub(crate) use plan::QueryPlan;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast;
    use crate::runtime::Instruction;
    use plan::*;

    #[test]
    fn build_a_to_b() {
        // (a) -> (b)
        let query = ast::Query {
            match_clauses: vec![ast::MatchClause {
                start: ast::Node::with_label(ast::Label::with_name("a")),
                edges: vec![(
                    ast::Edge::right(ast::Label::empty()),
                    ast::Node::with_label(ast::Label::with_name("b")),
                )],
            }],
            return_clause: vec!["a", "b"],
        };

        let plan = QueryPlan {
            matches: vec![
                MatchStep::LoadAnyNode { name: 0 },
                MatchStep::LoadOriginEdge { name: 1, node: 0 },
                MatchStep::LoadTargetNode { name: 2, edge: 1 },
            ],
            returns: vec![NamedValue::Node(0), NamedValue::Node(2)],
        };

        assert_eq!(plan, QueryPlan::new(&query).unwrap());
    }

    #[test]
    fn compile_a_to_b() {
        let plan = QueryPlan {
            matches: vec![
                MatchStep::LoadAnyNode { name: 0 },
                MatchStep::LoadOriginEdge { name: 1, node: 0 },
                MatchStep::LoadTargetNode { name: 2, edge: 1 },
            ],
            returns: vec![NamedValue::Node(0), NamedValue::Node(2)],
        };

        let code = {
            use Instruction::*;
            vec![
                IterNodes,
                NextNode(11),
                IterOriginEdges(0),
                NextEdge(9),
                LoadTargetNode(0),
                Yield,
                PopNode,
                PopEdge,
                Jump(3),
                PopNode,
                Jump(1),
                Halt,
            ]
        };

        assert_eq!(code, plan.compile().unwrap());
    }
}
