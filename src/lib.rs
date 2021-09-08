// TODO: Delete !
#![allow(dead_code)]

use planner::QueryPlan;
use runtime::{Program, StackValue, Status, VirtualMachine};
use std::path::Path;
use store::{Edge, Node, Store, StoreTxn};

pub(crate) mod error;
pub(crate) mod parser;
pub(crate) mod planner;
pub(crate) mod runtime;
pub(crate) mod store;

pub use error::Error;

pub struct Graph {
    store: Store,
}

pub struct Statement<'graph> {
    store: &'graph Store,
    program: Program,
}

pub struct Matches<'stmt, 'txn> {
    stmt: &'stmt Statement<'stmt>,
    vm: VirtualMachine<'txn, 'txn, 'txn>,
}

pub struct Match<'stmt, 'txn> {
    stmt: &'stmt Statement<'stmt>,
    vm: &'txn VirtualMachine<'txn, 'txn, 'txn>,
}

impl Graph {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let store = Store::open(path)?;
        Ok(Self { store })
    }

    pub fn open_anon() -> Result<Self, Error> {
        let store = Store::open_anon()?;
        Ok(Self { store })
    }

    pub fn prepare(&self, query: &str) -> Result<Statement, Error> {
        let ast = parser::parse(query)?;
        let plan = QueryPlan::new(&ast)?;
        // TODO
        // plan.optimize();
        let program = plan.compile()?;
        Ok(Statement {
            store: &self.store,
            program,
        })
    }

    pub fn txn(&self) -> Result<StoreTxn, Error> {
        self.store.txn()
    }
}

impl<'graph> Statement<'graph> {
    pub fn query<'stmt, 'txn>(
        &'txn self,
        txn: &'txn StoreTxn<'stmt>,
    ) -> Result<Matches<'txn, 'txn>, Error> {
        Ok(Matches {
            stmt: self,
            vm: VirtualMachine::new(txn, self.program.instructions.as_slice()),
        })
    }

    pub fn execute(&self) -> Result<(), Error> {
        let txn = self.store.txn()?;
        self.query(&txn)?;
        Ok(())
    }
}

impl<'stmt, 'txn> Matches<'stmt, 'txn> {
    #[inline]
    pub fn next<'m>(&'m mut self) -> Result<Option<Match<'m, 'm>>, Error> {
        match self.vm.run()? {
            Status::Yield => Ok(Some(Match {
                stmt: self.stmt,
                vm: &self.vm,
            })),
            Status::Halt => Ok(None),
        }
    }
}

impl<'stmt, 'txn> Match<'stmt, 'txn> {
    pub fn node(&self, idx: usize) -> Result<&Node<'txn>, Error> {
        match self.stmt.program.returns.get(idx) {
            Some(StackValue::Node(idx)) => Ok(&self.vm.node_stack[*idx]),
            Some(StackValue::Edge(_)) => Err(Error::Todo),
            None => Err(Error::Todo),
        }
    }

    pub fn edge(&self, idx: usize) -> Result<&Edge<'txn>, Error> {
        println!("{:?}", self.stmt.program);
        match self.stmt.program.returns.get(idx) {
            Some(StackValue::Node(_)) => Err(Error::Todo),
            Some(StackValue::Edge(idx)) => Ok(&self.vm.edge_stack[*idx]),
            None => Err(Error::Todo),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_a_to_b() {
        let graph = Graph::open_anon().unwrap();

        // TODO
        let mut txn = graph.store.mut_txn().unwrap();
        let a = txn.create_node("PERSON_A").unwrap().id;
        let b = txn.create_node("PERSON_B").unwrap().id;
        txn.create_edge("KNOWS", a, b).unwrap();
        txn.create_edge("KNOWS", b, a).unwrap();
        txn.commit().unwrap();

        let stmt = graph
            .prepare("MATCH (a) -[e]-> (b) RETURN a, b, e")
            .unwrap();
        let txn = graph.txn().unwrap();
        let mut matches = stmt.query(&txn).unwrap();

        let result = matches.next().unwrap().unwrap();
        assert_eq!("PERSON_A", result.node(0).unwrap().kind);
        assert_eq!("PERSON_B", result.node(1).unwrap().kind);
        assert_eq!("KNOWS", result.edge(2).unwrap().kind);

        let result = matches.next().unwrap().unwrap();
        assert_eq!("PERSON_B", result.node(0).unwrap().kind);
        assert_eq!("PERSON_A", result.node(1).unwrap().kind);
        assert_eq!("KNOWS", result.edge(2).unwrap().kind);

        assert!(matches.next().unwrap().is_none());
    }
}
