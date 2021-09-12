use crate::planner::{Filter, LoadProperty, MatchStep, NamedEntity, QueryPlan};
use crate::runtime::{Access, Instruction, Program};
use crate::Error;
use std::collections::HashMap;

const JUMP_PLACEHOLDER: usize = usize::MAX;

pub struct CompileEnv {
    names: HashMap<usize, usize>, // map names to stack position
    node_stack_len: usize,
    edge_stack_len: usize,

    instructions: Vec<Instruction>,
    accesses: Vec<Access>,
    returns: Vec<Access>,
}

impl CompileEnv {
    pub fn new() -> Self {
        Self {
            names: HashMap::new(),
            node_stack_len: 0,
            edge_stack_len: 0,

            instructions: Vec::new(),
            accesses: Vec::new(),
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

    fn adjust_jumps(instructions: &mut [Instruction], from: usize, to: usize) {
        for inst in instructions {
            use Instruction::*;
            match inst {
                Jump(t)
                | NextNode(t)
                | NextEdge(t)
                | CheckIsOrigin(t, _, _)
                | CheckIsTarget(t, _, _)
                | CheckNodeLabel(t, _, _)
                | CheckEdgeLabel(t, _, _)
                | CheckNodeId(t, _, _)
                | CheckEdgeId(t, _, _)
                | CheckTrue(t, _)
                | CheckEq(t, _, _)
                | CheckLt(t, _, _)
                | CheckGt(t, _, _) => {
                    if *t == from {
                        *t = to;
                    }
                }
                NoOp
                | Yield
                | Halt
                | IterNodes
                | IterOriginEdges(_)
                | IterTargetEdges(_)
                | IterBothEdges(_)
                | LoadOriginNode(_)
                | LoadTargetNode(_)
                | LoadOtherNode(_, _)
                | PopNode
                | PopEdge => (),
            }
        }
    }

    fn compile_access(&mut self, access: &LoadProperty) -> Result<usize, Error> {
        let access = match access {
            LoadProperty::Constant(val) => Access::Constant(val.clone()),
            LoadProperty::PropertyOfNode { node, key } => {
                let node = self.get_stack_idx(*node)?;
                Access::NodeProperty(node, key.clone())
            }
            LoadProperty::PropertyOfEdge { edge, key } => {
                let edge = self.get_stack_idx(*edge)?;
                Access::EdgeProperty(edge, key.clone())
            }
        };
        if let Some(idx) =
            self.accesses
                .iter()
                .enumerate()
                .find_map(|(i, a)| if *a == access { Some(i) } else { None })
        {
            Ok(idx)
        } else {
            self.accesses.push(access);
            Ok(self.accesses.len() - 1)
        }
    }

    /// Uses `JUMP_PLACEHOLDER` as a place-holder for the failed condition jump to
    /// be replaced after the position is known.
    fn compile_filter(&mut self, plan: &QueryPlan, filter: &Filter) -> Result<(), Error> {
        match filter {
            Filter::And(a, b) => {
                self.compile_filter(plan, a)?;
                self.compile_filter(plan, b)?;
            }
            Filter::Or(a, b) => {
                let start = self.instructions.len();
                self.compile_filter(plan, a)?;
                let inner_jmp = self.instructions.len();
                Self::adjust_jumps(
                    &mut self.instructions[start..],
                    JUMP_PLACEHOLDER,
                    inner_jmp + 1,
                );
                self.instructions.push(Instruction::NoOp);
                self.compile_filter(plan, b)?;
                self.instructions[inner_jmp] = Instruction::Jump(self.instructions.len());
            }
            Filter::Not(inner) => {
                let start = self.instructions.len();
                self.compile_filter(plan, inner)?;
                let end = self.instructions.len();
                Self::adjust_jumps(&mut self.instructions[start..], JUMP_PLACEHOLDER, end + 1);
                self.instructions.push(Instruction::Jump(JUMP_PLACEHOLDER));
            }

            Filter::IsOrigin { node, edge } => {
                let node = self.get_stack_idx(*node)?;
                let edge = self.get_stack_idx(*edge)?;
                self.instructions
                    .push(Instruction::CheckIsOrigin(JUMP_PLACEHOLDER, node, edge))
            }
            Filter::IsTarget { node, edge } => {
                let node = self.get_stack_idx(*node)?;
                let edge = self.get_stack_idx(*edge)?;
                self.instructions
                    .push(Instruction::CheckIsTarget(JUMP_PLACEHOLDER, node, edge))
            }

            Filter::NodeHasLabel { node, label } => {
                let node = self.get_stack_idx(*node)?;
                self.instructions.push(Instruction::CheckNodeLabel(
                    JUMP_PLACEHOLDER,
                    node,
                    label.clone(),
                ));
            }
            Filter::EdgeHasLabel { edge, label } => {
                let edge = self.get_stack_idx(*edge)?;
                self.instructions.push(Instruction::CheckEdgeLabel(
                    JUMP_PLACEHOLDER,
                    edge,
                    label.clone(),
                ));
            }

            Filter::NodeHasId { node, id } => {
                let node = self.get_stack_idx(*node)?;
                let acc = self.compile_access(id)?;
                self.instructions
                    .push(Instruction::CheckNodeId(JUMP_PLACEHOLDER, node, acc));
            }
            Filter::EdgeHasId { edge, id } => {
                let edge = self.get_stack_idx(*edge)?;
                let acc = self.compile_access(id)?;
                self.instructions
                    .push(Instruction::CheckNodeId(JUMP_PLACEHOLDER, edge, acc));
            }

            Filter::IsTruthy(load) => {
                let acc = self.compile_access(load)?;
                self.instructions
                    .push(Instruction::CheckTrue(JUMP_PLACEHOLDER, acc));
            }

            Filter::Eq(lhs, rhs) => {
                let lhs = self.compile_access(lhs)?;
                let rhs = self.compile_access(rhs)?;
                self.instructions
                    .push(Instruction::CheckEq(JUMP_PLACEHOLDER, lhs, rhs));
            }
            Filter::Lt(lhs, rhs) => {
                let lhs = self.compile_access(lhs)?;
                let rhs = self.compile_access(rhs)?;
                self.instructions
                    .push(Instruction::CheckLt(JUMP_PLACEHOLDER, lhs, rhs));
            }
            Filter::Gt(lhs, rhs) => {
                let lhs = self.compile_access(lhs)?;
                let rhs = self.compile_access(rhs)?;
                self.instructions
                    .push(Instruction::CheckGt(JUMP_PLACEHOLDER, lhs, rhs));
            }
        }
        Ok(())
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
                MatchStep::LoadOriginNode { name, edge } => {
                    self.instructions
                        .push(Instruction::LoadOriginNode(self.get_stack_idx(*edge)?));
                    self.push_node(*name);
                    self.compile_step(plan, &steps[1..])?;
                    self.pop_node(*name);
                    self.instructions.push(Instruction::PopNode);
                }
                MatchStep::LoadTargetNode { name, edge } => {
                    self.instructions
                        .push(Instruction::LoadTargetNode(self.get_stack_idx(*edge)?));
                    self.push_node(*name);
                    self.compile_step(plan, &steps[1..])?;
                    self.pop_node(*name);
                    self.instructions.push(Instruction::PopNode);
                }
                MatchStep::LoadOtherNode { name, node, edge } => {
                    self.instructions.push(Instruction::LoadOtherNode(
                        self.get_stack_idx(*node)?,
                        self.get_stack_idx(*edge)?,
                    ));
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
                MatchStep::LoadEitherEdge { name, node } => {
                    self.instructions
                        .push(Instruction::IterBothEdges(self.get_stack_idx(*node)?));
                    self.instructions.push(Instruction::NoOp); // set after to calc jump
                    self.push_edge(*name);
                    self.compile_step(plan, &steps[1..])?;
                    self.pop_edge(*name);
                    self.instructions.push(Instruction::PopEdge);
                    self.instructions.push(Instruction::Jump(start + 1));
                    self.instructions[start + 1] = Instruction::NextEdge(self.instructions.len());
                }

                MatchStep::Filter(filter) => {
                    self.compile_filter(plan, filter)?;
                    let filter_end = self.instructions.len();
                    self.compile_step(plan, &steps[1..])?;
                    let end = self.instructions.len();
                    Self::adjust_jumps(
                        &mut self.instructions[start..filter_end],
                        JUMP_PLACEHOLDER,
                        end,
                    );
                }
            }
            Ok(())
        } else {
            self.instructions.push(Instruction::Yield);
            if self.returns.is_empty() {
                for value in &plan.returns {
                    self.returns.push(match value {
                        NamedEntity::Node(name) => Access::Node(self.get_stack_idx(*name)?),
                        NamedEntity::Edge(name) => Access::Edge(self.get_stack_idx(*name)?),
                    });
                }
            }
            Ok(())
        }
    }
}

impl QueryPlan {
    pub fn compile(self) -> Result<Program, Error> {
        let mut env = CompileEnv::new();
        env.compile_step(&self, &self.steps)?;
        env.instructions.push(Instruction::Halt);
        Ok(Program {
            instructions: env.instructions,
            accesses: env.accesses,
            returns: env.returns,
        })
    }
}
