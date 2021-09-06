use crate::runtime::Instruction;
use crate::Error;
use std::collections::HashMap;

use super::plan::{MatchStep, QueryPlan};

pub struct CompileEnv {
    names: HashMap<usize, usize>, // map names to stack position
    node_stack_len: usize,
    edge_stack_len: usize,
}

impl CompileEnv {
    pub fn new() -> Self {
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

    fn compile_step(
        &mut self,
        code: &mut Vec<Instruction>,
        steps: &[MatchStep],
    ) -> Result<(), Error> {
        if let Some(step) = steps.get(0) {
            let start = code.len();
            match step {
                MatchStep::LoadAnyNode { name } => {
                    code.push(Instruction::IterNodes);
                    code.push(Instruction::NoOp); // set after to calc jump
                    self.push_node(*name);
                    self.compile_step(code, &steps[1..])?;
                    self.pop_node(*name);
                    code.push(Instruction::PopNode);
                    code.push(Instruction::Jump(start + 1));
                    code[start + 1] = Instruction::NextNode(code.len());
                }
                MatchStep::LoadTargetNode { name, edge } => {
                    code.push(Instruction::LoadTargetNode(self.get_stack_idx(*edge)?));
                    self.push_node(*name);
                    self.compile_step(code, &steps[1..])?;
                    self.pop_node(*name);
                    code.push(Instruction::PopNode);
                }
                MatchStep::LoadOriginNode { name, edge } => {
                    code.push(Instruction::LoadOriginNode(self.get_stack_idx(*edge)?));
                    self.push_node(*name);
                    self.compile_step(code, &steps[1..])?;
                    self.pop_node(*name);
                    code.push(Instruction::PopNode);
                }

                MatchStep::LoadOriginEdge { name, node } => {
                    code.push(Instruction::IterOriginEdges(self.get_stack_idx(*node)?));
                    code.push(Instruction::NoOp); // set after to calc jump
                    self.push_edge(*name);
                    self.compile_step(code, &steps[1..])?;
                    self.pop_edge(*name);
                    code.push(Instruction::PopEdge);
                    code.push(Instruction::Jump(start + 1));
                    code[start + 1] = Instruction::NextEdge(code.len());
                }
                MatchStep::LoadTargetEdge { name, node } => {
                    code.push(Instruction::IterTargetEdges(self.get_stack_idx(*node)?));
                    code.push(Instruction::NoOp); // set after to calc jump
                    self.push_edge(*name);
                    self.compile_step(code, &steps[1..])?;
                    self.pop_edge(*name);
                    code.push(Instruction::PopEdge);
                    code.push(Instruction::Jump(start + 1));
                    code[start + 1] = Instruction::NextEdge(code.len());
                }

                _ => unimplemented!(),
            }
            Ok(())
        } else {
            code.push(Instruction::Yield);
            Ok(())
        }
    }
}

impl QueryPlan {
    /// TODO: Execution/ Program separate things and return a program here ...
    pub fn compile(&self) -> Result<Vec<Instruction>, Error> {
        let mut code = vec![];
        let mut env = CompileEnv::new();
        env.compile_step(&mut code, &self.matches)?;
        code.push(Instruction::Halt);
        Ok(code)
    }
}
