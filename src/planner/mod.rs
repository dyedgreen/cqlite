use crate::Error;
use crate::{parser::ast, runtime::Instruction};
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Debug, Clone)]
struct QueryPlan {
    match_clauses: Vec<MatchNode>,
    return_clause: Vec<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LoadNode {
    Any,           // iterate all nodes / edges
    Named(usize),  // refer to already loaded node/ edge
    Origin(usize), // origin node of edge / iter edges originating from node
    Target(usize), // target node of edge / iter edges targeting node
                   // TODO: indexing on kind, will need special case (?)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LoadEdge {
    Named(usize),
    Origin(usize),
    Target(usize),
}

#[derive(Debug, Clone)]
struct MatchNode {
    name: usize,
    load: LoadNode,
    next: Option<Rc<MatchEdge>>,
}

#[derive(Debug, Clone)]
struct MatchEdge {
    name: usize,
    load: LoadEdge,
    next: MatchNode,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ReturnValue {
    Node(usize),
    Edge(usize),
}

struct CompileEnv {
    names: HashMap<usize, usize>, // map names to stack position
    node_stack_len: usize,
    edge_stack_len: usize,
}

impl CompileEnv {
    fn empty() -> Self {
        Self {
            names: HashMap::new(),
            node_stack_len: 0,
            edge_stack_len: 0,
        }
    }

    fn push_node(&mut self, name: usize) {
        self.names.insert(name, self.node_stack_len);
        self.node_stack_len += 1;
    }

    fn pop_node(&mut self, name: usize) {
        self.names.remove(&name);
        self.node_stack_len -= 1;
    }

    fn push_edge(&mut self, name: usize) {
        self.names.insert(name, self.edge_stack_len);
        self.edge_stack_len += 1;
    }

    fn pop_edge(&mut self, name: usize) {
        self.names.remove(&name);
        self.edge_stack_len -= 1;
    }

    fn get_stack_idx(&self, name: usize) -> Result<usize, Error> {
        self.names.get(&name).map(|idx| *idx).ok_or(Error::Todo)
    }
}

trait Compile {
    fn compile(&self, code: &mut Vec<Instruction>, env: &mut CompileEnv) -> Result<(), Error>;
}

impl Compile for MatchNode {
    fn compile(&self, code: &mut Vec<Instruction>, env: &mut CompileEnv) -> Result<(), Error> {
        let start = code.len();

        match self.load {
            LoadNode::Any => {
                code.push(Instruction::IterNodes);
                code.push(Instruction::NoOp); // set after, since jump position is unknown
                env.push_node(self.name);
            }
            LoadNode::Target(edge) => {
                code.push(Instruction::LoadTargetNode(env.get_stack_idx(edge)?));
                env.push_node(self.name);
            }
            _ => unimplemented!(),
        }

        if let Some(next) = &self.next {
            next.compile(code, env)?;
        } else {
            code.push(Instruction::Yield);
        }

        match self.load {
            LoadNode::Any => {
                env.pop_node(self.name);
                code[start + 1] = Instruction::NextNode(code.len() + 2);
                code.push(Instruction::PopNode);
                code.push(Instruction::Jump(start + 1));
            }
            LoadNode::Target(_) => {
                env.pop_node(self.name);
                code.push(Instruction::PopNode);
            }
            _ => unimplemented!(),
        }

        Ok(())
    }
}

impl Compile for MatchEdge {
    fn compile(&self, code: &mut Vec<Instruction>, env: &mut CompileEnv) -> Result<(), Error> {
        let start = code.len();

        match self.load {
            LoadEdge::Origin(node) => {
                code.push(Instruction::IterOriginEdges(env.get_stack_idx(node)?));
                code.push(Instruction::NoOp); // set after, since jump position is unknown
                env.push_edge(self.name);
            }
            _ => unimplemented!(),
        }

        self.next.compile(code, env)?;

        match self.load {
            LoadEdge::Origin(_) => {
                env.pop_edge(self.name);
                code[start + 1] = Instruction::NextEdge(code.len() + 2);
                code.push(Instruction::PopEdge);
                code.push(Instruction::Jump(start + 1));
            }
            _ => unimplemented!(),
        }

        Ok(())
    }
}

impl QueryPlan {
    pub fn new(query: &ast::Query) -> Result<Self, Error> {
        unimplemented!()
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
            return_clause: vec![0, 2],
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
