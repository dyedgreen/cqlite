use crate::runtime::{Instruction, Program, StackValue};
use crate::Error;
use std::collections::HashMap;

use super::plan::{MatchStep, NamedValue, QueryPlan};

pub struct CompileEnv {
    names: HashMap<usize, usize>, // map names to stack position
    node_stack_len: usize,
    edge_stack_len: usize,

    instructions: Vec<Instruction>,
    returns: Vec<StackValue>,
}

impl CompileEnv {
    pub fn new() -> Self {
        Self {
            names: HashMap::new(),
            node_stack_len: 0,
            edge_stack_len: 0,

            instructions: Vec::new(),
            returns: Vec::new(),
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

    fn compile_step(&mut self, plan: &QueryPlan, steps: &[MatchStep]) -> Result<(), Error> {
        if let Some(step) = steps.get(0) {
            let start = self.instructions.len();
            match step {
                MatchStep::LoadAnyNode { name } => {
                    self.instructions.push(Instruction::IterNodes);
                    self.instructions.push(Instruction::NoOp); // set after to calc jump
                    self.push_node(*name);
                    self.compile_step(plan, &steps[1..])?;
                    self.pop_node(*name);
                    self.instructions.push(Instruction::PopNode);
                    self.instructions.push(Instruction::Jump(start + 1));
                    self.instructions[start + 1] = Instruction::NextNode(self.instructions.len());
                }
                MatchStep::LoadTargetNode { name, edge } => {
                    self.instructions
                        .push(Instruction::LoadTargetNode(self.get_stack_idx(*edge)?));
                    self.push_node(*name);
                    self.compile_step(plan, &steps[1..])?;
                    self.pop_node(*name);
                    self.instructions.push(Instruction::PopNode);
                }
                MatchStep::LoadOriginNode { name, edge } => {
                    self.instructions
                        .push(Instruction::LoadOriginNode(self.get_stack_idx(*edge)?));
                    self.push_node(*name);
                    self.compile_step(plan, &steps[1..])?;
                    self.pop_node(*name);
                    self.instructions.push(Instruction::PopNode);
                }

                MatchStep::LoadOriginEdge { name, node } => {
                    self.instructions
                        .push(Instruction::IterOriginEdges(self.get_stack_idx(*node)?));
                    self.instructions.push(Instruction::NoOp); // set after to calc jump
                    self.push_edge(*name);
                    self.compile_step(plan, &steps[1..])?;
                    self.pop_edge(*name);
                    self.instructions.push(Instruction::PopEdge);
                    self.instructions.push(Instruction::Jump(start + 1));
                    self.instructions[start + 1] = Instruction::NextEdge(self.instructions.len());
                }
                MatchStep::LoadTargetEdge { name, node } => {
                    self.instructions
                        .push(Instruction::IterTargetEdges(self.get_stack_idx(*node)?));
                    self.instructions.push(Instruction::NoOp); // set after to calc jump
                    self.push_edge(*name);
                    self.compile_step(plan, &steps[1..])?;
                    self.pop_edge(*name);
                    self.instructions.push(Instruction::PopEdge);
                    self.instructions.push(Instruction::Jump(start + 1));
                    self.instructions[start + 1] = Instruction::NextEdge(self.instructions.len());
                }

                _ => unimplemented!(),
            }
            Ok(())
        } else {
            self.instructions.push(Instruction::Yield);
            if self.returns.is_empty() {
                for value in &plan.returns {
                    self.returns.push(match value {
                        &NamedValue::Node(name) => StackValue::Node(self.get_stack_idx(name)?),
                        &NamedValue::Edge(name) => StackValue::Edge(self.get_stack_idx(name)?),
                    });
                }
            }
            Ok(())
        }
    }
}

impl QueryPlan {
    pub fn compile(&self) -> Result<Program, Error> {
        let mut env = CompileEnv::new();
        env.compile_step(&self, &self.matches)?;
        env.instructions.push(Instruction::Halt);
        Ok(Program {
            instructions: env.instructions,
            returns: env.returns,
        })
    }
}
