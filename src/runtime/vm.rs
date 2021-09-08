use crate::store::{Edge, IndexIter, Node, StoreTxn, ValueIter};
use crate::Error;
use sanakirja::{Env, Txn};

pub(crate) struct VirtualMachine<'e, 't, 'i> {
    txn: &'t StoreTxn<'e>,
    instructions: &'i [Instruction],
    current_inst: usize,

    pub(crate) node_stack: Vec<Node<'t>>,
    pub(crate) edge_stack: Vec<Edge<'t>>,
    node_iters: Vec<ValueIter<'t, Txn<&'e Env>, Node<'t>>>,
    edge_iters: Vec<IndexIter<'t, Txn<&'e Env>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Instruction {
    /// Does nothing, can be used as a placeholder
    NoOp,

    /// Set the instruction pointer
    Jump(usize),
    /// Yields the interpreter, nodes and edges are ready for return
    Yield,
    /// Exists the program
    Halt,

    IterNodes,              // iter all nodes
    IterOriginEdges(usize), // iter edges originating from given node in stack
    IterTargetEdges(usize), // iter edges terminating at given node in stack

    NextNode(usize), // push next node to stack or pop iterator and jump
    NextEdge(usize), // push next edge to stack or pop iterator and jump

    LoadOriginNode(usize), // push origin node of edge to stack
    LoadTargetNode(usize), // push target node of edge to stack

    PopNode,
    PopEdge,
}
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum Status {
    Yield,
    Halt,
}

impl<'e, 't, 'i> VirtualMachine<'e, 't, 'i> {
    pub fn new(txn: &'t StoreTxn<'e>, instructions: &'i [Instruction]) -> Self {
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
            match self.instructions[self.current_inst] {
                Instruction::NoOp => self.current_inst += 1,

                Instruction::Jump(target) => self.current_inst = target,
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
                    let node_id = self.node_stack[node].id;
                    self.edge_iters.push(IndexIter::new(
                        &self.txn.txn,
                        &self.txn.origins,
                        node_id,
                    )?);
                    self.current_inst += 1;
                }
                Instruction::IterTargetEdges(node) => {
                    let node_id = self.node_stack[node].id;
                    self.edge_iters.push(IndexIter::new(
                        &self.txn.txn,
                        &self.txn.targets,
                        node_id,
                    )?);
                    self.current_inst += 1;
                }

                Instruction::NextNode(jump) => {
                    let iter = self.node_iters.last_mut().unwrap();
                    if let Some(node) = iter.next() {
                        self.node_stack.push(node?);
                        self.current_inst += 1;
                    } else {
                        self.node_iters.pop();
                        self.current_inst = jump;
                    }
                }
                Instruction::NextEdge(jump) => {
                    let iter = self.edge_iters.last_mut().unwrap();
                    if let Some(edge_id) = iter.next() {
                        self.edge_stack
                            .push(self.txn.get_edge(edge_id?)?.ok_or(Error::Todo)?);
                        self.current_inst += 1;
                    } else {
                        self.edge_iters.pop();
                        self.current_inst = jump;
                    }
                }

                Instruction::LoadOriginNode(edge) => {
                    let edge = &self.edge_stack[edge];
                    let node = self.txn.get_node(edge.origin)?.ok_or(Error::Todo)?;
                    self.node_stack.push(node);
                    self.current_inst += 1;
                }
                Instruction::LoadTargetNode(edge) => {
                    let edge = &self.edge_stack[edge];
                    let node = self.txn.get_node(edge.target)?.ok_or(Error::Todo)?;
                    self.node_stack.push(node);
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
            }
        }
    }
}

impl<'e, 't, 'i> std::fmt::Debug for VirtualMachine<'e, 't, 'i> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
