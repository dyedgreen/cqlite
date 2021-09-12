use crate::runtime::{Access, Instruction};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Program {
    pub instructions: Vec<Instruction>,
    pub accesses: Vec<Access>, // TODO: more clear naming (?)
    pub returns: Vec<Access>,
}

impl Program {
    pub fn new(
        instructions: Vec<Instruction>,
        accesses: Vec<Access>,
        returns: Vec<Access>,
    ) -> Self {
        Self {
            instructions,
            accesses,
            returns,
        }
    }
}
