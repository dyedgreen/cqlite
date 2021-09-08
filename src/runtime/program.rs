use crate::runtime::Instruction;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Program {
    pub instructions: Vec<Instruction>,
    pub returns: Vec<StackValue>,
}

/// TODO: Maybe later this will also encompass bound
/// values/ literals ...
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum StackValue {
    Node(usize),
    Edge(usize),
}

impl Program {
    pub fn new(instructions: Vec<Instruction>, returns: Vec<StackValue>) -> Self {
        Self {
            instructions,
            returns,
        }
    }
}
