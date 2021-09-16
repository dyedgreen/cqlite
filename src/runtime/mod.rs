mod program;
mod vm;

pub(crate) use program::Program;
pub(crate) use vm::{Access, Instruction, Status, VirtualMachine};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{MatchStep, QueryPlan};
    use crate::store::Store;
    use std::collections::HashMap;
    use vm::*;

    #[test]
    fn test_basic_match_script() {
        let store = Store::open_anon().unwrap();
        let mut txn = store.mut_txn().unwrap();
        let a = txn.create_node("PERSON_A", None).unwrap().id;
        let b = txn.create_node("PERSON_B", None).unwrap().id;
        txn.create_edge("KNOWS", a, b, None).unwrap();
        txn.create_edge("KNOWS", b, a, None).unwrap();
        txn.commit().unwrap();

        let instructions = {
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
        let prog = Program {
            instructions,
            accesses: vec![],
            returns: vec![],
        };

        let mut txn = store.txn().unwrap();
        let mut vm = VirtualMachine::new(&mut txn, &prog, HashMap::new());

        assert_eq!(Ok(Status::Yield), vm.run());
        assert_eq!(2, vm.node_stack.len());
        assert_eq!("PERSON_A", vm.node_stack[0].label());
        assert_eq!("PERSON_B", vm.node_stack[1].label());
        assert_eq!(1, vm.edge_stack.len());
        assert_eq!("KNOWS", vm.edge_stack[0].label());

        assert_eq!(Ok(Status::Yield), vm.run());
        assert_eq!(2, vm.node_stack.len());
        assert_eq!("PERSON_B", vm.node_stack[0].label());
        assert_eq!("PERSON_A", vm.node_stack[1].label());
        assert_eq!(1, vm.edge_stack.len());
        assert_eq!("KNOWS", vm.edge_stack[0].label());

        assert_eq!(Ok(Status::Halt), vm.run());
    }

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
