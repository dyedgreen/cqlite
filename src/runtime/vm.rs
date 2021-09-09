use crate::store::{Edge, EdgeIter, Node, StoreTxn, ValueIter};
use crate::Error;
use sanakirja::{Env, Txn};

pub(crate) struct VirtualMachine<'env, 'txn, 'inst> {
    txn: &'txn StoreTxn<'env>,

    instructions: &'inst [Instruction],
    current_inst: usize,

    pub(crate) node_stack: Vec<Node<'txn>>,
    pub(crate) edge_stack: Vec<Edge<'txn>>,
    node_iters: Vec<ValueIter<'txn, Txn<&'env Env>, Node<'txn>>>,
    edge_iters: Vec<EdgeIter>,
}

/// TODO: Consider to do a crane-lift JIT
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

    CheckNodeKind(usize, usize, String), // given (jump, node, kind), jump is the kind is different
    CheckEdgeKind(usize, usize, String), // given (jump, edge, kind), jump is the kind is different
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Status {
    Yield,
    Halt,
}

impl<'env, 'txn, 'inst> VirtualMachine<'env, 'txn, 'inst> {
    pub fn new(txn: &'txn StoreTxn<'env>, instructions: &'inst [Instruction]) -> Self {
        Self {
            txn,
            instructions,
            current_inst: 0,

            node_stack: Vec::new(),
            edge_stack: Vec::new(),
            node_iters: Vec::new(),
            edge_iters: Vec::new(),
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
                        .push(ValueIter::new(&self.txn.txn, &self.txn.nodes, None)?);
                    self.current_inst += 1;
                }

                Instruction::IterOriginEdges(node) => {
                    let node = &self.node_stack[*node];
                    self.edge_iters.push(EdgeIter::origins(node));
                    self.current_inst += 1;
                }
                Instruction::IterTargetEdges(node) => {
                    let node = &self.node_stack[*node];
                    self.edge_iters.push(EdgeIter::targets(node));
                    self.current_inst += 1;
                }
                Instruction::IterBothEdges(node) => {
                    let node = &self.node_stack[*node];
                    self.edge_iters.push(EdgeIter::both(node));
                    self.current_inst += 1;
                }

                Instruction::NextNode(jump) => {
                    let iter = self.node_iters.last_mut().unwrap();
                    if let Some(node) = iter.next() {
                        self.node_stack.push(node?);
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
                            .push(self.txn.get_edge(edge_id)?.ok_or(Error::Todo)?);
                        self.current_inst += 1;
                    } else {
                        self.edge_iters.pop();
                        self.current_inst = *jump;
                    }
                }

                Instruction::LoadOriginNode(edge) => {
                    let edge = &self.edge_stack[*edge];
                    let node = self.txn.get_node(edge.origin)?.ok_or(Error::Todo)?;
                    self.node_stack.push(node);
                    self.current_inst += 1;
                }
                Instruction::LoadTargetNode(edge) => {
                    let edge = &self.edge_stack[*edge];
                    let node = self.txn.get_node(edge.target)?.ok_or(Error::Todo)?;
                    self.node_stack.push(node);
                    self.current_inst += 1;
                }
                Instruction::LoadOtherNode(node, edge) => {
                    let node = &self.node_stack[*node];
                    let edge = &self.edge_stack[*edge];
                    let other = if edge.target == node.id {
                        self.txn.get_node(edge.origin)?.ok_or(Error::Todo)?
                    } else {
                        self.txn.get_node(edge.target)?.ok_or(Error::Todo)?
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

                Instruction::CheckNodeKind(jump, node, kind) => {
                    let node = &self.node_stack[*node];
                    if node.kind == kind {
                        self.current_inst += 1;
                    } else {
                        self.current_inst = *jump;
                    }
                }
                Instruction::CheckEdgeKind(jump, edge, kind) => {
                    let edge = &self.edge_stack[*edge];
                    if edge.kind == kind {
                        self.current_inst += 1;
                    } else {
                        self.current_inst = *jump;
                    }
                }
            }
        }
    }
}

impl<'env, 'txn, 'inst> std::fmt::Debug for VirtualMachine<'env, 'txn, 'inst> {
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
