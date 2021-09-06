#![allow(dead_code)] // TODO: Delete !

use std::path::Path;
use store::Store;

pub(crate) mod error;
pub(crate) mod parser;
pub(crate) mod planner;
pub(crate) mod runtime;
pub(crate) mod store;

pub use error::Error;

pub struct Graph {
    store: Store,
}

impl Graph {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, sanakirja::Error> {
        let store = Store::open(path)?;
        Ok(Self { store })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_a_to_b() {
        let cipher = "MATCH (a) -> (b) RETURN a, b";
        let query = parser::parse(cipher).unwrap();
        let plan = planner::QueryPlan::new(&query).unwrap();
        let code = plan.compile().unwrap();

        let store = Store::open_anon().unwrap();
        let mut txn = store.mut_txn().unwrap();
        let a = txn.create_node("PERSON_A").unwrap().id;
        let b = txn.create_node("PERSON_B").unwrap().id;
        txn.create_edge("KNOWS", a, b).unwrap();
        txn.create_edge("KNOWS", b, a).unwrap();
        txn.commit().unwrap();

        let txn = store.txn().unwrap();
        let mut program = runtime::VirtualMachine::new(&txn, &code);

        assert_eq!(Ok(runtime::Status::Yield), program.run());
        assert_eq!(2, program.node_stack.len());
        assert_eq!("PERSON_A", program.node_stack[0].kind);
        assert_eq!("PERSON_B", program.node_stack[1].kind);
        assert_eq!(1, program.edge_stack.len());
        assert_eq!("KNOWS", program.edge_stack[0].kind);

        assert_eq!(Ok(runtime::Status::Yield), program.run());
        assert_eq!(2, program.node_stack.len());
        assert_eq!("PERSON_B", program.node_stack[0].kind);
        assert_eq!("PERSON_A", program.node_stack[1].kind);
        assert_eq!(1, program.edge_stack.len());
        assert_eq!("KNOWS", program.edge_stack[0].kind);

        assert_eq!(Ok(runtime::Status::Halt), program.run());
    }
}
