mod program;
mod vm;

pub(crate) use program::Program;
pub(crate) use vm::{Access, Instruction, Status, VirtualMachine};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;
    use vm::*;

    #[test]
    fn test_basic_match_script() {
        let store = Store::open_anon().unwrap();
        let mut txn = store.mut_txn().unwrap();
        let a = txn.create_node("PERSON_A").unwrap().id;
        let b = txn.create_node("PERSON_B").unwrap().id;
        txn.create_edge("KNOWS", a, b).unwrap();
        txn.create_edge("KNOWS", b, a).unwrap();
        txn.commit().unwrap();

        let code = {
            use Instruction::*;
            vec![
                IterNodes,
                NextNode(11),
                IterOriginEdges(0),
                NextEdge(9),
                LoadTargetNode(0),
                Yield,
                PopNode,
                PopEdge,
                Jump(3),
                PopNode,
                Jump(1),
                Halt,
            ]
        };

        let txn = store.txn().unwrap();
        let mut vm = VirtualMachine::new(&txn, &code, &[]);

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
}
