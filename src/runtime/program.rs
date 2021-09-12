use crate::runtime::Instruction;
use crate::store::Property;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Program {
    pub instructions: Vec<Instruction>,
    pub accesses: Vec<ValueAccess>, // TODO: more clear naming (?)
    pub returns: Vec<ValueAccess>,
}

/// TODO: An instruction to access a value
/// TODO: Find a better name...
/// this is meant to allow accessing nodes, edges, properties, and constants ...
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ValueAccess {
    Constant(Property),

    Node(usize), // node on the stack
    Edge(usize), // edge on the stack

    NodeProperty(usize, String),
    EdgeProperty(usize, String),
}

impl Program {
    pub fn new(
        instructions: Vec<Instruction>,
        accesses: Vec<ValueAccess>,
        returns: Vec<ValueAccess>,
    ) -> Self {
        Self {
            instructions,
            accesses,
            returns,
        }
    }
}
