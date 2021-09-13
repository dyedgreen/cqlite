// TODO: Delete !
#![allow(dead_code)]

use planner::QueryPlan;
use runtime::{Access, Program, Status, VirtualMachine};
use std::path::Path;
use store::{Store, StoreTxn};

pub(crate) mod error;
pub(crate) mod parser;
pub(crate) mod planner;
pub(crate) mod runtime;
pub(crate) mod store;

pub use error::Error;
pub use store::{Edge, Node, Property};

/// TODO: A handle to the database
pub struct Graph {
    store: Store,
}

/// TODO: Handle read/ write transactions in VM ...
/// either with generics or with an enum (?)
pub struct Txn<'graph>(StoreTxn<'graph>);

/// TODO: A prepared statement
pub struct Statement<'graph> {
    graph: &'graph Graph,
    program: Program,
}

/// TODO: A running query, the same statement
/// can be run concurrently ...
pub struct Query<'stmt, 'txn> {
    stmt: &'stmt Statement<'stmt>,
    vm: VirtualMachine<'stmt, 'txn, 'stmt>,
}

/// TODO: A guard to access a set of matched
/// nodes and edges
pub struct Match<'query> {
    query: &'query Query<'query, 'query>,
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
            graph: self,
            program,
        })
    }

    pub fn txn(&self) -> Result<Txn, Error> {
        Ok(Txn(self.store.txn()?))
    }

    pub fn mut_txn(&self) -> Result<Txn, Error> {
        Ok(Txn(self.store.mut_txn()?))
    }

    pub fn delete_me_build_test_graph(&self) -> Result<(), Error> {
        let mut txn = self.store.mut_txn()?;

        let a = txn.create_node("PERSON", None)?.id;
        let b = txn.create_node("PERSON", None)?.id;

        txn.create_edge("KNOWS", a, b, None)?;
        txn.create_edge("HEARD_OF", b, a, None)?;
        txn.commit()?;

        Ok(())
    }
}

impl<'graph> Statement<'graph> {
    pub fn query<'stmt, 'txn>(
        &'stmt self,
        txn: &'txn Txn<'stmt>,
    ) -> Result<Query<'stmt, 'txn>, Error> {
        Ok(Query {
            stmt: self,
            vm: VirtualMachine::new(
                &txn.0,
                &self.program.instructions[..],
                &self.program.accesses[..],
            ),
        })
    }

    pub fn execute(&self) -> Result<(), Error> {
        let txn = self.graph.txn()?;
        self.query(&txn)?;
        Ok(())
    }
}

impl<'stmt, 'txn> Query<'stmt, 'txn> {
    #[inline]
    pub fn step(&mut self) -> Result<Option<Match>, Error> {
        match self.vm.run()? {
            Status::Yield => Ok(Some(Match { query: self })),
            Status::Halt => Ok(None),
        }
    }
}

impl<'query> Match<'query> {
    pub fn node(&self, idx: usize) -> Result<&Node, Error> {
        match self.query.stmt.program.returns.get(idx) {
            Some(Access::Constant(_)) => Err(Error::Todo),
            Some(Access::Node(idx)) => Ok(&self.query.vm.node_stack[*idx]),
            Some(Access::Edge(_)) => Err(Error::Todo),
            Some(Access::NodeProperty(_, _)) => Err(Error::Todo),
            Some(Access::EdgeProperty(_, _)) => Err(Error::Todo),
            None => Err(Error::Todo),
        }
    }

    pub fn edge(&self, idx: usize) -> Result<&Edge, Error> {
        match self.query.stmt.program.returns.get(idx) {
            Some(Access::Constant(_)) => Err(Error::Todo),
            Some(Access::Node(_)) => Err(Error::Todo),
            Some(Access::Edge(idx)) => Ok(&self.query.vm.edge_stack[*idx]),
            Some(Access::NodeProperty(_, _)) => Err(Error::Todo),
            Some(Access::EdgeProperty(_, _)) => Err(Error::Todo),
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
        let a = txn.create_node("PERSON_A", None).unwrap().id;
        let b = txn.create_node("PERSON_B", None).unwrap().id;
        txn.create_edge("KNOWS", a, b, None).unwrap();
        txn.create_edge("KNOWS", b, a, None).unwrap();
        txn.commit().unwrap();

        let stmt = graph
            .prepare("MATCH (a) -[e]-> (b) RETURN a, b, e")
            .unwrap();
        let txn = graph.txn().unwrap();
        let mut matches = stmt.query(&txn).unwrap();

        let result = matches.step().unwrap().unwrap();
        assert_eq!("PERSON_A", result.node(0).unwrap().label());
        assert_eq!("PERSON_B", result.node(1).unwrap().label());
        assert_eq!("KNOWS", result.edge(2).unwrap().label());

        let result = matches.step().unwrap().unwrap();
        assert_eq!("PERSON_B", result.node(0).unwrap().label());
        assert_eq!("PERSON_A", result.node(1).unwrap().label());
        assert_eq!("KNOWS", result.edge(2).unwrap().label());

        assert!(matches.step().unwrap().is_none());
    }

    #[test]
    fn run_a_edge_b() {
        let graph = Graph::open_anon().unwrap();

        // TODO
        let mut txn = graph.store.mut_txn().unwrap();
        let a = txn.create_node("PERSON_A", None).unwrap().id;
        let b = txn.create_node("PERSON_B", None).unwrap().id;
        txn.create_edge("KNOWS", a, b, None).unwrap();
        txn.commit().unwrap();

        let stmt = graph.prepare("MATCH (a) -[e]- (b) RETURN a, b, e").unwrap();
        let txn = graph.txn().unwrap();
        let mut matches = stmt.query(&txn).unwrap();

        let result = matches.step().unwrap().unwrap();
        assert_eq!("PERSON_A", result.node(0).unwrap().label());
        assert_eq!("PERSON_B", result.node(1).unwrap().label());
        assert_eq!("KNOWS", result.edge(2).unwrap().label());

        let result = matches.step().unwrap().unwrap();
        assert_eq!("PERSON_B", result.node(0).unwrap().label());
        assert_eq!("PERSON_A", result.node(1).unwrap().label());
        assert_eq!("KNOWS", result.edge(2).unwrap().label());

        assert!(matches.step().unwrap().is_none());
    }

    #[test]
    fn run_a_to_a() {
        let graph = Graph::open_anon().unwrap();

        // TODO
        let mut txn = graph.store.mut_txn().unwrap();
        let a = txn.create_node("PERSON_A", None).unwrap().id;
        let b = txn.create_node("PERSON_B", None).unwrap().id;
        txn.create_edge("KNOWS", a, a, None).unwrap();
        txn.create_edge("KNOWS", b, b, None).unwrap();
        txn.commit().unwrap();

        let stmt = graph.prepare("MATCH (a) -[e]-> (a) RETURN a, e").unwrap();
        let txn = graph.txn().unwrap();
        let mut matches = stmt.query(&txn).unwrap();

        let result = matches.step().unwrap().unwrap();
        assert_eq!("PERSON_A", result.node(0).unwrap().label());
        assert_eq!("KNOWS", result.edge(1).unwrap().label());

        let result = matches.step().unwrap().unwrap();
        assert_eq!("PERSON_B", result.node(0).unwrap().label());
        assert_eq!("KNOWS", result.edge(1).unwrap().label());

        assert!(matches.step().unwrap().is_none());
    }

    #[test]
    fn run_a_edge_a() {
        let graph = Graph::open_anon().unwrap();

        // TODO
        let mut txn = graph.store.mut_txn().unwrap();
        let a = txn.create_node("PERSON_A", None).unwrap().id;
        let b = txn.create_node("PERSON_B", None).unwrap().id;
        txn.create_edge("KNOWS", a, a, None).unwrap();
        txn.create_edge("KNOWS", b, b, None).unwrap();
        txn.commit().unwrap();

        let stmt = graph.prepare("MATCH (a) -[e]- (a) RETURN a, e").unwrap();
        let txn = graph.txn().unwrap();

        let mut matches = stmt.query(&txn).unwrap();

        let result = matches.step().unwrap().unwrap();
        assert_eq!("PERSON_A", result.node(0).unwrap().label());
        assert_eq!("KNOWS", result.edge(1).unwrap().label());

        let result = matches.step().unwrap().unwrap();
        assert_eq!("PERSON_A", result.node(0).unwrap().label());
        assert_eq!("KNOWS", result.edge(1).unwrap().label());

        let result = matches.step().unwrap().unwrap();
        assert_eq!("PERSON_B", result.node(0).unwrap().label());
        assert_eq!("KNOWS", result.edge(1).unwrap().label());

        let result = matches.step().unwrap().unwrap();
        assert_eq!("PERSON_B", result.node(0).unwrap().label());
        assert_eq!("KNOWS", result.edge(1).unwrap().label());

        assert!(matches.step().unwrap().is_none());
    }

    #[test]
    fn run_a_knows_b() {
        let graph = Graph::open_anon().unwrap();

        // TODO
        let mut txn = graph.store.mut_txn().unwrap();
        let a = txn.create_node("PERSON_A", None).unwrap().id;
        let b = txn.create_node("PERSON_B", None).unwrap().id;
        txn.create_edge("KNOWS", a, b, None).unwrap();
        txn.create_edge("HEARD_OF", b, a, None).unwrap();
        txn.commit().unwrap();

        let stmt = graph
            .prepare("MATCH (a) -[e:KNOWS]-> (b) RETURN a, b, e")
            .unwrap();
        let txn = graph.txn().unwrap();
        let mut matches = stmt.query(&txn).unwrap();

        let result = matches.step().unwrap().unwrap();
        assert_eq!("PERSON_A", result.node(0).unwrap().label());
        assert_eq!("PERSON_B", result.node(1).unwrap().label());
        assert_eq!("KNOWS", result.edge(2).unwrap().label());

        assert!(matches.step().unwrap().is_none());
    }

    #[test]
    fn run_a_edge_b_with_where_property() {
        let graph = Graph::open_anon().unwrap();

        // TODO
        let mut txn = graph.store.mut_txn().unwrap();
        let mut a = txn.create_node("PERSON", None).unwrap();
        a.properties.insert("test".into(), Property::Integer(42));
        txn.update_node(&a).unwrap();
        let b = txn.create_node("PERSON", None).unwrap();
        txn.create_edge("KNOWS", a.id(), b.id(), None).unwrap();
        txn.commit().unwrap();

        let stmt = graph
            .prepare(
                "
                MATCH (a:PERSON) -[:KNOWS]- (b:PERSON)
                WHERE a.test = 42
                RETURN a, b
                ",
            )
            .unwrap();
        let txn = graph.txn().unwrap();
        let mut matches = stmt.query(&txn).unwrap();

        let result = matches.step().unwrap().unwrap();
        assert_eq!(a.id(), result.node(0).unwrap().id());
        assert_eq!(b.id(), result.node(1).unwrap().id());

        assert!(matches.step().unwrap().is_none());
    }

    #[test]
    fn run_a_edge_b_with_where_id() {
        let graph = Graph::open_anon().unwrap();

        // TODO
        let mut txn = graph.store.mut_txn().unwrap();
        let a = txn.create_node("PERSON", None).unwrap();
        let b = txn.create_node("PERSON", None).unwrap();
        txn.create_edge("KNOWS", a.id(), b.id(), None).unwrap();
        txn.commit().unwrap();

        let stmt = graph
            .prepare(
                "
                MATCH (a:PERSON)
                WHERE 1 = ID ( a )
                RETURN a
                ",
            )
            .unwrap();
        let txn = graph.txn().unwrap();
        let mut matches = stmt.query(&txn).unwrap();

        let result = matches.step().unwrap().unwrap();
        assert_eq!(b.id(), result.node(0).unwrap().id());

        assert!(matches.step().unwrap().is_none());
    }
}
