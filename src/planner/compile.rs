use crate::runtime::Instruction;
use crate::Error;
use std::collections::HashMap;

use super::plan::{LoadEdge, LoadNode, MatchEdge, MatchNode};

pub struct CompileEnv {
    names: HashMap<usize, usize>, // map names to stack position
    node_stack_len: usize,
    edge_stack_len: usize,
}

impl CompileEnv {
    pub fn empty() -> Self {
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

pub(crate) trait Compile {
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
