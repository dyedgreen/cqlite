use super::Program;
use crate::store::{Edge, EdgeIter, Node, NodeIter, PropOwned, PropRef, StoreTxn, Update};
use crate::Error;
use std::cmp::Ordering;
use std::collections::HashMap;

/// Runtime to execute a compiled query program. Note that the
/// transaction takes an immutable borrow, but expects to be the
/// only borrow; otherwise errors may occur when trying to enqueue
/// updates.
pub(crate) struct VirtualMachine<'env, 'txn, 'prog> {
    txn: &'txn StoreTxn<'env>,

    instructions: &'prog [Instruction],
    accesses: &'prog [Access],
    returns: &'prog [Access],
    parameters: HashMap<String, PropOwned>,
    current_inst: usize,

    node_stack: Vec<Node>,
    edge_stack: Vec<Edge>,
    node_iters: Vec<NodeIter<'txn>>,
    edge_iters: Vec<EdgeIter<'txn>>,
}

/// TODO: Consider to do a Cranelift JIT
/// instead? (let's see how slow this ends
/// up being ...)
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Instruction {
    /// Do nothing.
    NoOp,

    /// Absolute jump to instruction `jump`.
    Jump {
        jump: usize,
    },
    /// Yield a set of matches.
    Yield,
    /// Terminate the program.
    Halt,

    /// Create an iterator over all nodes.
    IterNodes,

    /// Iterate edges originating from `node`.
    IterOriginEdges {
        node: usize,
    },
    /// Iterator edges terminating at `node`.
    IterTargetEdges {
        node: usize,
    },
    /// Iterate all edges connected to `node`.
    IterBothEdges {
        node: usize,
    },

    /// Load the next node from the top iterator or pop
    /// the iterator and jump.
    LoadNextNode {
        jump: usize,
    },
    /// Load the next edge form the top iterator or pop
    /// the iterator and jump.
    LoadNextEdge {
        jump: usize,
    },

    /// Load the node with `id = access[id]` of jump.
    LoadExactNode {
        jump: usize,
        id: usize,
    },

    /// Load the node from which `edge` originates.
    LoadOriginNode {
        edge: usize,
    },
    /// Load the node at which `edge` terminates.
    LoadTargetNode {
        edge: usize,
    },
    /// Load the remaining node which belongs to the
    /// connection formed by `node` and `edge`.
    LoadOtherNode {
        node: usize,
        edge: usize,
    },

    PopNode,
    PopEdge,

    /// Perform a conditional jump if `node` is not
    /// the origin of `edge`.
    CheckIsOrigin {
        jump: usize,
        node: usize,
        edge: usize,
    },
    /// Perform a conditional jump if `node` is not
    /// the target of `edge`.
    CheckIsTarget {
        jump: usize,
        node: usize,
        edge: usize,
    },

    /// Perform a conditional jump if the label of
    /// `node` is different from `label`.
    CheckNodeLabel {
        jump: usize,
        node: usize,
        label: String,
    },
    /// Perform a conditional jump if the label of
    /// `edge` is different from `label`.
    CheckEdgeLabel {
        jump: usize,
        edge: usize,
        label: String,
    },

    /// Perform a conditional jump if the id of
    /// `node` is different from `access[id]`.
    CheckNodeId {
        jump: usize,
        node: usize,
        id: usize,
    },
    /// Perform a conditional jump if the id of
    /// `edge` is different from `access[id]`.
    CheckEdgeId {
        jump: usize,
        edge: usize,
        id: usize,
    },

    /// Perform a conditional jump if `access[value]`
    /// is no truthy.
    CheckTrue {
        jump: usize,
        value: usize,
    },
    /// Perform a conditional jump if `lhs` is not
    /// loosely equal to `rhs`.
    CheckEq {
        jump: usize,
        lhs: usize,
        rhs: usize,
    },
    /// Perform a conditional jump if not `lhs < rhs`.
    CheckLt {
        jump: usize,
        lhs: usize,
        rhs: usize,
    },
    /// Perform a conditional jump if not `lhs > rhs`.
    CheckGt {
        jump: usize,
        lhs: usize,
        rhs: usize,
    },

    /// Queue an update that creates a new node with
    /// the given label and the set of properties
    /// assembled using the `Vec` of accesses.
    ///
    /// The created node is also pushed to the node
    /// stack.
    CreateNode {
        label: String,
        properties: Vec<(String, usize)>,
    },
    /// Queue an update that creates a new edge with
    /// the given label, origin, target, and the set
    /// of properties assembled using the `Vec` of
    /// accesses.
    ///
    /// The created node is also pushed to the node
    /// stack.
    CreateEdge {
        label: String,
        origin: usize,
        target: usize,
        properties: Vec<(String, usize)>,
    },
    /// Queue an update that sets property `key` of
    /// `node` to `access[value]`.
    SetNodeProperty {
        node: usize,
        key: String,
        value: usize,
    },
    /// Queue an update that sets property `key` of
    /// `edge` to `access[value]`.
    SetEdgeProperty {
        edge: usize,
        key: String,
        value: usize,
    },
    /// Queue an update that deletes the given `node`.
    DeleteNode {
        node: usize,
    },
    /// Queue an update that deletes the given `edge`.
    DeleteEdge {
        edge: usize,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Access {
    Constant(PropOwned),
    NodeId(usize),
    EdgeId(usize),
    NodeLabel(usize),
    EdgeLabel(usize),
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
        txn: &'txn mut StoreTxn<'env>,
        program: &'prog Program,
        parameters: HashMap<String, PropOwned>,
    ) -> Self {
        Self {
            txn,
            instructions: &program.instructions,
            accesses: &program.accesses,
            returns: &program.returns,
            current_inst: 0,

            parameters,

            node_stack: Vec::new(),
            edge_stack: Vec::new(),
            node_iters: Vec::new(),
            edge_iters: Vec::new(),
        }
    }

    fn access_property(&self, access: usize) -> Result<PropRef, Error> {
        match &self.accesses[access] {
            Access::Constant(val) => Ok(val.to_ref()),
            Access::NodeId(node) => Ok(PropRef::Id(self.node_stack[*node].id())),
            Access::EdgeId(edge) => Ok(PropRef::Id(self.edge_stack[*edge].id())),
            Access::NodeLabel(node) => Ok(PropRef::Text(self.node_stack[*node].label())),
            Access::EdgeLabel(edge) => Ok(PropRef::Text(self.edge_stack[*edge].label())),
            Access::NodeProperty(node, key) => Ok(self.node_stack[*node].property(key).to_ref()),
            Access::EdgeProperty(edge, key) => Ok(self.edge_stack[*edge].property(key).to_ref()),
            Access::Parameter(name) => Ok(self
                .parameters
                .get(name)
                .map(PropOwned::to_ref)
                .unwrap_or(PropRef::Null)),
        }
    }

    pub fn access_return(&self, access: usize) -> Result<PropOwned, Error> {
        match self.returns.get(access).ok_or(Error::IndexOutOfBounds)? {
            Access::Constant(val) => Ok(val.clone()),
            Access::NodeId(node) => Ok(PropOwned::Id(self.node_stack[*node].id())),
            Access::EdgeId(edge) => Ok(PropOwned::Id(self.edge_stack[*edge].id())),
            Access::NodeLabel(node) => {
                Ok(PropOwned::Text(self.node_stack[*node].label().to_string()))
            }
            Access::EdgeLabel(edge) => {
                Ok(PropOwned::Text(self.edge_stack[*edge].label().to_string()))
            }
            Access::NodeProperty(node, key) => {
                let node = &self.node_stack[*node];
                Ok(self
                    .txn
                    .get_updated_property(node.id(), key)?
                    .unwrap_or_else(|| node.property(key).clone()))
            }
            Access::EdgeProperty(edge, key) => {
                let edge = &self.edge_stack[*edge];
                Ok(self
                    .txn
                    .get_updated_property(edge.id(), key)?
                    .unwrap_or_else(|| edge.property(key).clone()))
            }
            Access::Parameter(name) => Ok(self
                .parameters
                .get(name)
                .map(Clone::clone)
                .unwrap_or(PropOwned::Null)),
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

                Instruction::Jump { jump } => self.current_inst = *jump,
                Instruction::Yield => {
                    self.current_inst += 1;
                    return Ok(Status::Yield);
                }
                Instruction::Halt => return Ok(Status::Halt),

                Instruction::IterNodes => {
                    self.node_iters
                        .push(NodeIter::new(self.txn, &self.txn.nodes, None)?);
                    self.current_inst += 1;
                }

                Instruction::IterOriginEdges { node } => {
                    let node = &self.node_stack[*node];
                    self.edge_iters.push(EdgeIter::origins(self.txn, node.id)?);
                    self.current_inst += 1;
                }
                Instruction::IterTargetEdges { node } => {
                    let node = &self.node_stack[*node];
                    self.edge_iters.push(EdgeIter::targets(self.txn, node.id)?);
                    self.current_inst += 1;
                }
                Instruction::IterBothEdges { node } => {
                    let node = &self.node_stack[*node];
                    self.edge_iters.push(EdgeIter::both(self.txn, node.id)?);
                    self.current_inst += 1;
                }

                Instruction::LoadNextNode { jump } => {
                    let iter = self.node_iters.last_mut().unwrap();
                    if let Some(entry) = iter.next() {
                        self.node_stack.push(entry?.1);
                        self.current_inst += 1;
                    } else {
                        self.node_iters.pop();
                        self.current_inst = *jump;
                    }
                }
                Instruction::LoadNextEdge { jump } => {
                    let iter = self.edge_iters.last_mut().unwrap();
                    if let Some(edge_id) = iter.next() {
                        self.edge_stack
                            .push(self.txn.load_edge(edge_id?)?.ok_or(Error::MissingEdge)?);
                        self.current_inst += 1;
                    } else {
                        self.edge_iters.pop();
                        self.current_inst = *jump;
                    }
                }

                Instruction::LoadExactNode { jump, id } => {
                    let id = self.access_property(*id)?.cast_to_id().ok();
                    if let Some(node) = id
                        .map(|id| self.txn.load_node(id).transpose())
                        .flatten()
                        .transpose()?
                    {
                        self.node_stack.push(node);
                        self.current_inst += 1;
                    } else {
                        self.current_inst = *jump;
                    }
                }

                Instruction::LoadOriginNode { edge } => {
                    let edge = &self.edge_stack[*edge];
                    let node = self.txn.load_node(edge.origin)?.ok_or(Error::MissingNode)?;
                    self.node_stack.push(node);
                    self.current_inst += 1;
                }
                Instruction::LoadTargetNode { edge } => {
                    let edge = &self.edge_stack[*edge];
                    let node = self.txn.load_node(edge.target)?.ok_or(Error::MissingNode)?;
                    self.node_stack.push(node);
                    self.current_inst += 1;
                }
                Instruction::LoadOtherNode { node, edge } => {
                    let node = &self.node_stack[*node];
                    let edge = &self.edge_stack[*edge];
                    let other = if edge.target == node.id {
                        self.txn.load_node(edge.origin)?.ok_or(Error::MissingNode)?
                    } else {
                        self.txn.load_node(edge.target)?.ok_or(Error::MissingNode)?
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

                Instruction::CheckIsOrigin { jump, node, edge } => {
                    let node = &self.node_stack[*node];
                    let edge = &self.edge_stack[*edge];
                    if node.id == edge.origin {
                        self.current_inst += 1;
                    } else {
                        self.current_inst = *jump;
                    }
                }
                Instruction::CheckIsTarget { jump, node, edge } => {
                    let node = &self.node_stack[*node];
                    let edge = &self.edge_stack[*edge];
                    if node.id == edge.target {
                        self.current_inst += 1;
                    } else {
                        self.current_inst = *jump;
                    }
                }

                Instruction::CheckNodeLabel { jump, node, label } => {
                    let node = &self.node_stack[*node];
                    if node.label() == label.as_str() {
                        self.current_inst += 1;
                    } else {
                        self.current_inst = *jump;
                    }
                }
                Instruction::CheckEdgeLabel { jump, edge, label } => {
                    let edge = &self.edge_stack[*edge];
                    if edge.label() == label.as_str() {
                        self.current_inst += 1;
                    } else {
                        self.current_inst = *jump;
                    }
                }

                Instruction::CheckNodeId { jump, node, id } => {
                    let node = &self.node_stack[*node];
                    match self.access_property(*id)?.cast_to_id() {
                        Ok(id) if id == node.id => self.current_inst += 1,
                        Ok(_) | Err(_) => self.current_inst = *jump,
                    }
                }
                Instruction::CheckEdgeId { jump, edge, id } => {
                    let edge = &self.edge_stack[*edge];
                    match self.access_property(*id)?.cast_to_id() {
                        Ok(id) if id == edge.id => self.current_inst += 1,
                        Ok(_) | Err(_) => self.current_inst = *jump,
                    }
                }

                Instruction::CheckTrue { jump, value } => {
                    let value = self.access_property(*value)?;
                    if value.is_truthy() {
                        self.current_inst += 1;
                    } else {
                        self.current_inst = *jump;
                    }
                }
                Instruction::CheckEq { jump, lhs, rhs } => {
                    let lhs = self.access_property(*lhs)?;
                    let rhs = self.access_property(*rhs)?;
                    if lhs.loosely_equals(&rhs) {
                        self.current_inst += 1;
                    } else {
                        self.current_inst = *jump;
                    }
                }
                Instruction::CheckLt { jump, lhs, rhs } => {
                    let lhs = self.access_property(*lhs)?;
                    let rhs = self.access_property(*rhs)?;
                    if let Some(Ordering::Less) = lhs.loosely_compare(&rhs) {
                        self.current_inst += 1;
                    } else {
                        self.current_inst = *jump;
                    }
                }
                Instruction::CheckGt { jump, lhs, rhs } => {
                    let lhs = self.access_property(*lhs)?;
                    let rhs = self.access_property(*rhs)?;
                    if let Some(Ordering::Greater) = lhs.loosely_compare(&rhs) {
                        self.current_inst += 1;
                    } else {
                        self.current_inst = *jump;
                    }
                }

                Instruction::CreateNode { label, properties } => {
                    let node = Node {
                        id: self.txn.id_seq(),
                        label: label.clone(),
                        properties: properties
                            .iter()
                            .map(|(key, access)| -> Result<_, Error> {
                                Ok((key.clone(), self.access_property(*access)?.to_owned()))
                            })
                            .filter(|prop| !matches!(prop, Ok((_, PropOwned::Null))))
                            .collect::<Result<_, Error>>()?,
                    };
                    self.txn.queue_update(Update::CreateNode(node.clone()))?;
                    self.node_stack.push(node);
                    self.current_inst += 1;
                }
                Instruction::CreateEdge {
                    label,
                    origin,
                    target,
                    properties,
                } => {
                    let origin = self.node_stack[*origin].id();
                    let target = self.node_stack[*target].id();
                    let edge = Edge {
                        id: self.txn.id_seq(),
                        label: label.clone(),
                        origin,
                        target,
                        properties: properties
                            .iter()
                            .map(|(key, access)| -> Result<_, Error> {
                                Ok((key.clone(), self.access_property(*access)?.to_owned()))
                            })
                            .filter(|prop| !matches!(prop, Ok((_, PropOwned::Null))))
                            .collect::<Result<_, Error>>()?,
                    };
                    self.txn.queue_update(Update::CreateEdge(edge.clone()))?;
                    self.edge_stack.push(edge);
                    self.current_inst += 1;
                }
                Instruction::SetNodeProperty { node, key, value } => {
                    let node = &self.node_stack[*node];
                    let value = self.access_property(*value)?.to_owned();
                    self.txn.queue_update(Update::SetNodeProperty(
                        node.id,
                        key.to_string(),
                        value,
                    ))?;
                    self.current_inst += 1;
                }
                Instruction::SetEdgeProperty { edge, key, value } => {
                    let edge = &self.node_stack[*edge];
                    let value = self.access_property(*value)?.to_owned();
                    self.txn.queue_update(Update::SetEdgeProperty(
                        edge.id,
                        key.to_string(),
                        value,
                    ))?;
                    self.current_inst += 1;
                }
                Instruction::DeleteNode { node } => {
                    let node = &self.node_stack[*node];
                    self.txn.queue_update(Update::DeleteNode(node.id))?;
                    self.current_inst += 1;
                }
                Instruction::DeleteEdge { edge } => {
                    let edge = &self.edge_stack[*edge];
                    self.txn.queue_update(Update::DeleteEdge(edge.id))?;
                    self.current_inst += 1;
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
