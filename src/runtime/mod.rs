mod program;
mod vm;

pub(crate) use program::Program;
pub(crate) use vm::{Access, Instruction, Status, VirtualMachine};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{MatchStep, QueryPlan};
    use vm::*;

    #[test]
    fn compile_a_to_b() {
        let plan = QueryPlan {
            steps: vec![
                MatchStep::LoadAnyNode { name: 0 },
                MatchStep::LoadOriginEdge { name: 1, node: 0 },
                MatchStep::LoadTargetNode { name: 2, edge: 1 },
            ],
            updates: vec![],
            returns: vec![],
        };

        let code = {
            use Instruction::*;
            vec![
                IterNodes,
                LoadNextNode { jump: 11 },
                IterOriginEdges { node: 0 },
                LoadNextEdge { jump: 9 },
                LoadTargetNode { edge: 0 },
                Yield,
                PopNode,
                PopEdge,
                Jump { jump: 3 },
                PopNode,
                Jump { jump: 1 },
                Halt,
            ]
        };

        assert_eq!(code, Program::new(&plan).unwrap().instructions);
    }
}
