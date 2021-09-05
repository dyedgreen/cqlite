use crate::Error;
use crate::{parser::ast, runtime::Instruction};
use compile::{Compile, CompileEnv};
use plan::{MatchNode, ReturnValue};

mod build;
mod compile;
mod plan;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct QueryPlan {
    match_clauses: Vec<MatchNode>,
    return_clause: Vec<ReturnValue>,
}

impl QueryPlan {
    pub fn new(query: &ast::Query) -> Result<Self, Error> {
        build::build_plan(query)
    }

    pub fn optimize(&mut self) {
        // we could re-write the plan ...
        unimplemented!()
    }

    // return program instructions and map from return position to
    // node / edge stack
    pub fn compile(&self) -> Result<(Vec<Instruction>, Vec<ReturnValue>), Error> {
        assert_eq!(1, self.match_clauses.len()); // TODO

        let mut code = Vec::new();
        self.match_clauses[0].compile(&mut code, &mut CompileEnv::empty())?;
        code.push(Instruction::Halt);

        Ok((code, vec![]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use plan::*;
    use std::rc::Rc;

    #[test]
    fn build_a_to_b() {
        // (a) -> (b)
        let query = ast::Query {
            match_clauses: vec![ast::MatchClause {
                start: ast::Node::with_label(ast::Label::with_name("a")),
                edges: vec![(
                    ast::Edge::left(ast::Label::empty()),
                    ast::Node::with_label(ast::Label::with_name("b")),
                )],
            }],
            return_clause: vec!["a", "b"],
        };

        let plan = QueryPlan {
            match_clauses: vec![MatchNode {
                name: 0,
                load: LoadNode::Any,
                next: Some(Rc::new(MatchEdge {
                    name: 1,
                    load: LoadEdge::Origin(0),
                    next: MatchNode {
                        name: 2,
                        load: LoadNode::Target(1),
                        next: None,
                    },
                })),
            }],
            return_clause: vec![ReturnValue::Node(0), ReturnValue::Node(2)],
        };

        let build_plan = QueryPlan::new(&query).unwrap();
        assert_eq!(plan, build_plan);
    }

    #[test]
    fn compile_a_to_b() {
        let plan = QueryPlan {
            match_clauses: vec![MatchNode {
                name: 0,
                load: LoadNode::Any,
                next: Some(Rc::new(MatchEdge {
                    name: 1,
                    load: LoadEdge::Origin(0),
                    next: MatchNode {
                        name: 2,
                        load: LoadNode::Target(1),
                        next: None,
                    },
                })),
            }],
            return_clause: vec![ReturnValue::Node(0), ReturnValue::Node(2)],
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

        let (compile_code, _) = plan.compile().unwrap();
        assert_eq!(code, compile_code);
    }
}
