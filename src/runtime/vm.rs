use crate::store::{Edge, EdgeIter, Node, NodeIter, StoreTxn};
use crate::{Error, Property};
use std::cmp::Ordering;
use std::collections::HashMap;

pub(crate) struct VirtualMachine<'env, 'txn, 'prog> {
    txn: &'txn StoreTxn<'env>,

    instructions: &'prog [Instruction],
    accesses: &'prog [Access],
    current_inst: usize,

    parameters: HashMap<String, Property>,

    pub(crate) node_stack: Vec<Node>,
    pub(crate) edge_stack: Vec<Edge>,
    node_iters: Vec<NodeIter<'txn>>,
    edge_iters: Vec<EdgeIter<'txn>>,
}

/// TODO: Consider to do a Cranelift JIT
/// instead? (let's see how slow this ends
/// up being ...)
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Instruction {
    NoOp,

    Jump(usize),
    Yield,
    Halt,

    IterNodes, // iter all nodes

    IterOriginEdges(usize), // iter edges originating from given node in stack
    IterTargetEdges(usize), // iter edges terminating at given node in stack
    IterBothEdges(usize),   // iter all edges for given node in stack

    NextNode(usize), // push next node to stack or pop iterator and jump
    NextEdge(usize), // push next edge to stack or pop iterator and jump

    LoadOriginNode(usize),       // push origin node of edge to stack
    LoadTargetNode(usize),       // push target node of edge to stack
    LoadOtherNode(usize, usize), // load the partner of a (node, edge) pair

    PopNode,
    PopEdge,

    CheckIsOrigin(usize, usize, usize), // given (jump, node, edge), jump if node is not edge origin
    CheckIsTarget(usize, usize, usize), // given (jump, node, edge), jump if node is not edge target

    CheckNodeLabel(usize, usize, String), // given (jump, node, label), jump is the label is different
    CheckEdgeLabel(usize, usize, String), // given (jump, edge, label), jump is the label is different

    CheckNodeId(usize, usize, usize), // given (jump, node, id), jump id access(id) != node.id
    CheckEdgeId(usize, usize, usize), // given (jump, node, id), jump id access(id) != node.id

    CheckTrue(usize, usize), // given (jump, property), jump if property is truthy
    CheckEq(usize, usize, usize), // given (jump, lhs, rhs), jump if not lhs = rhs
    CheckLt(usize, usize, usize), // given (jump, lhs, rhs), jump if not lhs < rhs
    CheckGt(usize, usize, usize), // given (jump, lhs, rhs), jump if not lhs > rhs
}

/// TODO: An instruction to access a value
/// this is meant to allow accessing nodes, edges, properties, and constants ...
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Access {
    Node(usize), // on the stack
    Edge(usize), // on the stack
    Constant(Property),
    NodeProperty(usize, String),
    EdgeProperty(usize, String),
    Parameter(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Status {
    Yield,
    Halt,
}

impl<'env, 'txn, 'prog> VirtualMachine<'env, 'txn, 'prog> {
    pub fn new(
        txn: &'txn StoreTxn<'env>,
        instructions: &'prog [Instruction],
        accesses: &'prog [Access],
        parameters: HashMap<String, Property>,
    ) -> Self {
        Self {
            txn,
            instructions,
            accesses,
            current_inst: 0,

            parameters,

            node_stack: Vec::new(),
            edge_stack: Vec::new(),
            node_iters: Vec::new(),
            edge_iters: Vec::new(),
        }
    }

    pub fn access_property(&self, access: usize) -> Result<&Property, Error> {
        match &self.accesses[access] {
            Access::Node(_) => Err(Error::Todo),
            Access::Edge(_) => Err(Error::Todo),
            Access::Constant(val) => Ok(val),
            Access::NodeProperty(node, key) => Ok(self.node_stack[*node].property(key)),
            Access::EdgeProperty(edge, key) => Ok(self.edge_stack[*edge].property(key)),
            Access::Parameter(name) => Ok(self.parameters.get(name).unwrap_or(&Property::Null)),
        }
    }

    /// Docs: TODO
    ///
    /// # Panics
    /// Indices in instructions are not checked and
    /// may panic. Instructions for consuming iterators
    /// do not check if iterators exist and may panic.
    pub fn run(&mut self) -> Result<Status, Error> {
        loop {
            match &self.instructions[self.current_inst] {
                Instruction::NoOp => self.current_inst += 1,

                Instruction::Jump(target) => self.current_inst = *target,
                Instruction::Yield => {
                    self.current_inst += 1;
                    return Ok(Status::Yield);
                }
                Instruction::Halt => return Ok(Status::Halt),

                Instruction::IterNodes => {
                    self.node_iters
                        .push(NodeIter::new(&self.txn.txn, &self.txn.nodes, None)?);
                    self.current_inst += 1;
                }

                Instruction::IterOriginEdges(node) => {
                    let node = &self.node_stack[*node];
                    self.edge_iters.push(EdgeIter::origins(&self.txn, node.id)?);
                    self.current_inst += 1;
                }
                Instruction::IterTargetEdges(node) => {
                    let node = &self.node_stack[*node];
                    self.edge_iters.push(EdgeIter::targets(&self.txn, node.id)?);
                    self.current_inst += 1;
                }
                Instruction::IterBothEdges(node) => {
                    let node = &self.node_stack[*node];
                    self.edge_iters.push(EdgeIter::both(&self.txn, node.id)?);
                    self.current_inst += 1;
                }

                Instruction::NextNode(jump) => {
                    let iter = self.node_iters.last_mut().unwrap();
                    if let Some(entry) = iter.next() {
                        self.node_stack.push(entry?.1);
                        self.current_inst += 1;
                    } else {
                        self.node_iters.pop();
                        self.current_inst = *jump;
                    }
                }
                Instruction::NextEdge(jump) => {
                    let iter = self.edge_iters.last_mut().unwrap();
                    if let Some(edge_id) = iter.next() {
                        self.edge_stack
                            .push(self.txn.load_edge(edge_id?)?.ok_or(Error::Todo)?);
                        self.current_inst += 1;
                    } else {
                        self.edge_iters.pop();
                        self.current_inst = *jump;
                    }
                }

                Instruction::LoadOriginNode(edge) => {
                    let edge = &self.edge_stack[*edge];
                    let node = self.txn.load_node(edge.origin)?.ok_or(Error::Todo)?;
                    self.node_stack.push(node);
                    self.current_inst += 1;
                }
                Instruction::LoadTargetNode(edge) => {
                    let edge = &self.edge_stack[*edge];
                    let node = self.txn.load_node(edge.target)?.ok_or(Error::Todo)?;
                    self.node_stack.push(node);
                    self.current_inst += 1;
                }
                Instruction::LoadOtherNode(node, edge) => {
                    let node = &self.node_stack[*node];
                    let edge = &self.edge_stack[*edge];
                    let other = if edge.target == node.id {
                        self.txn.load_node(edge.origin)?.ok_or(Error::Todo)?
                    } else {
                        self.txn.load_node(edge.target)?.ok_or(Error::Todo)?
                    };
                    self.node_stack.push(other);
                    self.current_inst += 1;
                }

                Instruction::PopNode => {
                    self.node_stack.pop();
                    self.current_inst += 1;
                }
                Instruction::PopEdge => {
                    self.edge_stack.pop();
                    self.current_inst += 1;
                }

                Instruction::CheckIsOrigin(jump, node, edge) => {
                    let node = &self.node_stack[*node];
                    let edge = &self.edge_stack[*edge];
                    if node.id == edge.origin {
                        self.current_inst += 1;
                    } else {
                        self.current_inst = *jump;
                    }
                }
                Instruction::CheckIsTarget(jump, node, edge) => {
                    let node = &self.node_stack[*node];
                    let edge = &self.edge_stack[*edge];
                    if node.id == edge.target {
                        self.current_inst += 1;
                    } else {
                        self.current_inst = *jump;
                    }
                }

                Instruction::CheckNodeLabel(jump, node, label) => {
                    let node = &self.node_stack[*node];
                    if node.label.as_str() == label.as_str() {
                        self.current_inst += 1;
                    } else {
                        self.current_inst = *jump;
                    }
                }
                Instruction::CheckEdgeLabel(jump, edge, label) => {
                    let edge = &self.edge_stack[*edge];
                    if edge.label.as_str() == label.as_str() {
                        self.current_inst += 1;
                    } else {
                        self.current_inst = *jump;
                    }
                }

                Instruction::CheckNodeId(jump, node, id) => {
                    let node = &self.node_stack[*node];
                    let id = self.access_property(*id)?.cast_to_id()?;
                    if node.id == id {
                        self.current_inst += 1;
                    } else {
                        self.current_inst = *jump;
                    }
                }
                Instruction::CheckEdgeId(jump, edge, id) => {
                    let edge = &self.node_stack[*edge];
                    let id = self.access_property(*id)?.cast_to_id()?;
                    if edge.id == id {
                        self.current_inst += 1;
                    } else {
                        self.current_inst = *jump;
                    }
                }

                Instruction::CheckTrue(jump, access) => {
                    let prop = self.access_property(*access)?;
                    if prop.is_truthy() {
                        self.current_inst += 1;
                    } else {
                        self.current_inst = *jump;
                    }
                }
                Instruction::CheckEq(jump, lhs, rhs) => {
                    let lhs = self.access_property(*lhs)?;
                    let rhs = self.access_property(*rhs)?;
                    if lhs.loosely_equals(rhs) {
                        self.current_inst += 1;
                    } else {
                        self.current_inst = *jump;
                    }
                }
                Instruction::CheckLt(jump, lhs, rhs) => {
                    let lhs = self.access_property(*lhs)?;
                    let rhs = self.access_property(*rhs)?;
                    if let Some(Ordering::Less) = lhs.loosely_compare(rhs) {
                        self.current_inst += 1;
                    } else {
                        self.current_inst = *jump;
                    }
                }
                Instruction::CheckGt(jump, lhs, rhs) => {
                    let lhs = self.access_property(*lhs)?;
                    let rhs = self.access_property(*rhs)?;
                    if let Some(Ordering::Greater) = lhs.loosely_compare(rhs) {
                        self.current_inst += 1;
                    } else {
                        self.current_inst = *jump;
                    }
                }
            }
        }
    }
}

impl<'env, 'txn, 'prog> std::fmt::Debug for VirtualMachine<'env, 'txn, 'prog> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Program")
            .field("current_inst", &self.current_inst)
            .field("instructions", &self.instructions)
            .field("node_stack", &self.node_stack)
            .field("edge_stack", &self.edge_stack)
            .field("node_iters", &self.node_iters.len())
            .field("edge_iters", &self.node_iters.len())
            .finish()
    }
}
