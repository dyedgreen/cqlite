use crate::planner::{Filter, LoadProperty, MatchStep, QueryPlan, UpdateStep};
use crate::runtime::{Access, Instruction};
use crate::Error;
use std::collections::HashMap;

const JUMP_PLACEHOLDER: usize = usize::MAX;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Program {
    pub instructions: Vec<Instruction>,
    pub accesses: Vec<Access>,
    pub returns: Vec<Access>,
}

struct CompileEnv {
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
                Jump { jump }
                | LoadNextNode { jump }
                | LoadNextEdge { jump }
                | CheckIsOrigin { jump, .. }
                | CheckIsTarget { jump, .. }
                | CheckNodeLabel { jump, .. }
                | CheckEdgeLabel { jump, .. }
                | CheckNodeId { jump, .. }
                | CheckEdgeId { jump, .. }
                | CheckTrue { jump, .. }
                | CheckEq { jump, .. }
                | CheckLt { jump, .. }
                | CheckGt { jump, .. } => {
                    if *jump == from {
                        *jump = to;
                    }
                }
                NoOp
                | Yield
                | Halt
                | IterNodes
                | IterOriginEdges { .. }
                | IterTargetEdges { .. }
                | IterBothEdges { .. }
                | LoadOriginNode { .. }
                | LoadTargetNode { .. }
                | LoadOtherNode { .. }
                | PopNode
                | PopEdge
                | CreateNode { .. }
                | CreateEdge { .. }
                | SetNodeProperty { .. }
                | SetEdgeProperty { .. }
                | DeleteNode { .. }
                | DeleteEdge { .. } => (),
            }
        }
    }

    fn compile_access_raw(&self, load: &LoadProperty) -> Result<Access, Error> {
        Ok(match load {
            LoadProperty::Constant(val) => Access::Constant(val.clone()),
            LoadProperty::IdOfNode { node } => {
                let node = self.get_stack_idx(*node)?;
                Access::NodeId(node)
            }
            LoadProperty::IdOfEdge { edge } => {
                let edge = self.get_stack_idx(*edge)?;
                Access::EdgeId(edge)
            }
            LoadProperty::PropertyOfNode { node, key } => {
                let node = self.get_stack_idx(*node)?;
                Access::NodeProperty(node, key.to_string())
            }
            LoadProperty::PropertyOfEdge { edge, key } => {
                let edge = self.get_stack_idx(*edge)?;
                Access::EdgeProperty(edge, key.to_string())
            }
            LoadProperty::Parameter { name } => Access::Parameter(name.to_string()),
        })
    }

    fn compile_access(&mut self, load: &LoadProperty) -> Result<usize, Error> {
        let access = self.compile_access_raw(load)?;
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
                self.instructions[inner_jmp] = Instruction::Jump {
                    jump: self.instructions.len(),
                };
            }
            Filter::Not(inner) => {
                let start = self.instructions.len();
                self.compile_filter(plan, inner)?;
                let end = self.instructions.len();
                Self::adjust_jumps(&mut self.instructions[start..], JUMP_PLACEHOLDER, end + 1);
                self.instructions.push(Instruction::Jump {
                    jump: JUMP_PLACEHOLDER,
                });
            }

            Filter::IsOrigin { node, edge } => {
                let node = self.get_stack_idx(*node)?;
                let edge = self.get_stack_idx(*edge)?;
                self.instructions.push(Instruction::CheckIsOrigin {
                    jump: JUMP_PLACEHOLDER,
                    node,
                    edge,
                });
            }
            Filter::IsTarget { node, edge } => {
                let node = self.get_stack_idx(*node)?;
                let edge = self.get_stack_idx(*edge)?;
                self.instructions.push(Instruction::CheckIsTarget {
                    jump: JUMP_PLACEHOLDER,
                    node,
                    edge,
                });
            }

            Filter::NodeHasLabel { node, label } => {
                let node = self.get_stack_idx(*node)?;
                self.instructions.push(Instruction::CheckNodeLabel {
                    jump: JUMP_PLACEHOLDER,
                    node,
                    label: label.to_string(),
                });
            }
            Filter::EdgeHasLabel { edge, label } => {
                let edge = self.get_stack_idx(*edge)?;
                self.instructions.push(Instruction::CheckEdgeLabel {
                    jump: JUMP_PLACEHOLDER,
                    edge,
                    label: label.to_string(),
                });
            }

            Filter::NodeHasId { node, id } => {
                let node = self.get_stack_idx(*node)?;
                let id = self.compile_access(id)?;
                self.instructions.push(Instruction::CheckNodeId {
                    jump: JUMP_PLACEHOLDER,
                    node,
                    id,
                });
            }
            Filter::EdgeHasId { edge, id } => {
                let edge = self.get_stack_idx(*edge)?;
                let id = self.compile_access(id)?;
                self.instructions.push(Instruction::CheckEdgeId {
                    jump: JUMP_PLACEHOLDER,
                    edge,
                    id,
                });
            }

            Filter::IsTruthy(load) => {
                let value = self.compile_access(load)?;
                self.instructions.push(Instruction::CheckTrue {
                    jump: JUMP_PLACEHOLDER,
                    value,
                });
            }

            Filter::Eq(lhs, rhs) => {
                let lhs = self.compile_access(lhs)?;
                let rhs = self.compile_access(rhs)?;
                self.instructions.push(Instruction::CheckEq {
                    jump: JUMP_PLACEHOLDER,
                    lhs,
                    rhs,
                });
            }
            Filter::Lt(lhs, rhs) => {
                let lhs = self.compile_access(lhs)?;
                let rhs = self.compile_access(rhs)?;
                self.instructions.push(Instruction::CheckLt {
                    jump: JUMP_PLACEHOLDER,
                    lhs,
                    rhs,
                });
            }
            Filter::Gt(lhs, rhs) => {
                let lhs = self.compile_access(lhs)?;
                let rhs = self.compile_access(rhs)?;
                self.instructions.push(Instruction::CheckGt {
                    jump: JUMP_PLACEHOLDER,
                    lhs,
                    rhs,
                });
            }
        }
        Ok(())
    }

    fn compile_update(&mut self, plan: &QueryPlan, updates: &[UpdateStep]) -> Result<(), Error> {
        if let Some(update) = updates.get(0) {
            match update {
                UpdateStep::CreateNode {
                    name,
                    label,
                    properties,
                } => {
                    let create_node = Instruction::CreateNode {
                        label: label.to_string(),
                        properties: properties
                            .iter()
                            .map(|(key, load)| -> Result<_, Error> {
                                Ok((key.to_string(), self.compile_access(load)?))
                            })
                            .collect::<Result<_, Error>>()?,
                    };
                    self.instructions.push(create_node);
                    self.push_node(*name);
                    self.compile_update(plan, &updates[1..])?;
                    self.pop_node(*name);
                    self.instructions.push(Instruction::PopNode);
                    Ok(())
                }
                UpdateStep::CreateEdge {
                    name,
                    label,
                    origin,
                    target,
                    properties,
                } => {
                    let create_edge = Instruction::CreateEdge {
                        label: label.to_string(),
                        origin: self.get_stack_idx(*origin)?,
                        target: self.get_stack_idx(*target)?,
                        properties: properties
                            .iter()
                            .map(|(key, load)| -> Result<_, Error> {
                                Ok((key.to_string(), self.compile_access(load)?))
                            })
                            .collect::<Result<_, Error>>()?,
                    };
                    self.instructions.push(create_edge);
                    self.push_edge(*name);
                    self.compile_update(plan, &updates[1..])?;
                    self.pop_edge(*name);
                    self.instructions.push(Instruction::PopEdge);
                    Ok(())
                }
                UpdateStep::SetNodeProperty { node, key, value } => {
                    let node = self.get_stack_idx(*node)?;
                    let value = self.compile_access(value)?;
                    self.instructions.push(Instruction::SetNodeProperty {
                        node,
                        key: key.to_string(),
                        value,
                    });
                    self.compile_update(plan, &updates[1..])
                }
                UpdateStep::SetEdgeProperty { edge, key, value } => {
                    let edge = self.get_stack_idx(*edge)?;
                    let value = self.compile_access(value)?;
                    self.instructions.push(Instruction::SetEdgeProperty {
                        edge,
                        key: key.to_string(),
                        value,
                    });
                    self.compile_update(plan, &updates[1..])
                }
                UpdateStep::DeleteNode { node } => {
                    let node = self.get_stack_idx(*node)?;
                    self.instructions.push(Instruction::DeleteNode { node });
                    self.compile_update(plan, &updates[1..])
                }
                UpdateStep::DeleteEdge { edge } => {
                    let edge = self.get_stack_idx(*edge)?;
                    self.instructions.push(Instruction::DeleteEdge { edge });
                    self.compile_update(plan, &updates[1..])
                }
            }
        } else {
            self.instructions.push(Instruction::Yield);
            if self.returns.is_empty() {
                for load in &plan.returns {
                    self.returns.push(self.compile_access_raw(load)?);
                }
            }
            Ok(())
        }
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
                    self.instructions
                        .push(Instruction::Jump { jump: start + 1 });
                    self.instructions[start + 1] = Instruction::LoadNextNode {
                        jump: self.instructions.len(),
                    };
                }
                MatchStep::LoadOriginNode { name, edge } => {
                    self.instructions.push(Instruction::LoadOriginNode {
                        edge: self.get_stack_idx(*edge)?,
                    });
                    self.push_node(*name);
                    self.compile_step(plan, &steps[1..])?;
                    self.pop_node(*name);
                    self.instructions.push(Instruction::PopNode);
                }
                MatchStep::LoadTargetNode { name, edge } => {
                    self.instructions.push(Instruction::LoadTargetNode {
                        edge: self.get_stack_idx(*edge)?,
                    });
                    self.push_node(*name);
                    self.compile_step(plan, &steps[1..])?;
                    self.pop_node(*name);
                    self.instructions.push(Instruction::PopNode);
                }
                MatchStep::LoadOtherNode { name, node, edge } => {
                    self.instructions.push(Instruction::LoadOtherNode {
                        node: self.get_stack_idx(*node)?,
                        edge: self.get_stack_idx(*edge)?,
                    });
                    self.push_node(*name);
                    self.compile_step(plan, &steps[1..])?;
                    self.pop_node(*name);
                    self.instructions.push(Instruction::PopNode);
                }

                MatchStep::LoadOriginEdge { name, node } => {
                    self.instructions.push(Instruction::IterOriginEdges {
                        node: self.get_stack_idx(*node)?,
                    });
                    self.instructions.push(Instruction::NoOp); // set after to calc jump
                    self.push_edge(*name);
                    self.compile_step(plan, &steps[1..])?;
                    self.pop_edge(*name);
                    self.instructions.push(Instruction::PopEdge);
                    self.instructions
                        .push(Instruction::Jump { jump: start + 1 });
                    self.instructions[start + 1] = Instruction::LoadNextEdge {
                        jump: self.instructions.len(),
                    };
                }
                MatchStep::LoadTargetEdge { name, node } => {
                    self.instructions.push(Instruction::IterTargetEdges {
                        node: self.get_stack_idx(*node)?,
                    });
                    self.instructions.push(Instruction::NoOp); // set after to calc jump
                    self.push_edge(*name);
                    self.compile_step(plan, &steps[1..])?;
                    self.pop_edge(*name);
                    self.instructions.push(Instruction::PopEdge);
                    self.instructions
                        .push(Instruction::Jump { jump: start + 1 });
                    self.instructions[start + 1] = Instruction::LoadNextEdge {
                        jump: self.instructions.len(),
                    };
                }
                MatchStep::LoadEitherEdge { name, node } => {
                    self.instructions.push(Instruction::IterBothEdges {
                        node: self.get_stack_idx(*node)?,
                    });
                    self.instructions.push(Instruction::NoOp); // set after to calc jump
                    self.push_edge(*name);
                    self.compile_step(plan, &steps[1..])?;
                    self.pop_edge(*name);
                    self.instructions.push(Instruction::PopEdge);
                    self.instructions
                        .push(Instruction::Jump { jump: start + 1 });
                    self.instructions[start + 1] = Instruction::LoadNextEdge {
                        jump: self.instructions.len(),
                    };
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
            self.compile_update(plan, &plan.updates)
        }
    }
}

impl Program {
    /// Compile a `QueryPlan` into a `Program`.
    pub fn new(plan: &QueryPlan) -> Result<Program, Error> {
        let mut env = CompileEnv::new();
        env.compile_step(&plan, &plan.steps)?;
        env.instructions.push(Instruction::Halt);
        Ok(Program {
            instructions: env.instructions,
            accesses: env.accesses,
            returns: env.returns,
        })
    }
}
