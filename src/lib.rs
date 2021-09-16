// TODO: Delete !
#![allow(dead_code)]

use planner::QueryPlan;
use runtime::{Program, Status, VirtualMachine};
use std::{collections::HashMap, path::Path};
use store::{Store, StoreTxn};

pub(crate) mod error;
pub(crate) mod parser;
pub(crate) mod planner;
pub(crate) mod runtime;
pub(crate) mod store;

pub use error::Error;
pub use store::{Property, PropertyRef};

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
        Ok(Statement {
            graph: self,
            program: Program::new(&plan)?,
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

impl<'graph> Txn<'graph> {
    pub fn commit(self) -> Result<(), Error> {
        self.0.commit()
    }
}

impl<'graph> Statement<'graph> {
    /// TODO: Have a parameter trait
    /// TODO: Read matches
    pub fn query<'stmt, 'txn>(
        &'stmt self,
        txn: &'txn mut Txn<'stmt>,
        parameters: Option<HashMap<String, Property>>,
    ) -> Result<Query<'stmt, 'txn>, Error> {
        txn.0.flush()?;
        Ok(Query {
            stmt: self,
            vm: VirtualMachine::new(
                &txn.0,
                &self.program,
                parameters.unwrap_or_else(HashMap::new),
            ),
        })
    }

    /// TODO: Have a parameter trait
    /// TODO: Write to the database
    pub fn execute<'stmt, 'txn>(
        &'stmt self,
        txn: &'txn mut Txn<'stmt>,
        parameters: Option<HashMap<String, Property>>,
    ) -> Result<(), Error> {
        let mut query = self.query(txn, parameters)?;
        while let Some(_) = query.step()? {}
        Ok(())
    }
}

impl<'stmt, 'txn> Query<'stmt, 'txn> {
    #[inline]
    pub fn step(&mut self) -> Result<Option<Match>, Error> {
        // TODO: If a query has no return clause, skip yields ...
        match self.vm.run()? {
            Status::Yield => Ok(Some(Match { query: self })),
            Status::Halt => Ok(None),
        }
    }
}

impl<'query> Match<'query> {
    /// TODO: Should we not return a property ref but accept a 'FromProperty'?
    pub fn get(&self, idx: usize) -> Result<PropertyRef, Error> {
        self.query.vm.access_return(idx)
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
        let a = txn.create_node("PERSON", None).unwrap().id;
        let b = txn.create_node("PERSON", None).unwrap().id;
        let eab = txn.create_edge("KNOWS", a, b, None).unwrap().id;
        let eba = txn.create_edge("KNOWS", b, a, None).unwrap().id;
        txn.commit().unwrap();

        let stmt = graph
            .prepare("MATCH (a) -[e]-> (b) RETURN ID(a), ID(b), ID(e)")
            .unwrap();
        let mut txn = graph.txn().unwrap();
        let mut matches = stmt.query(&mut txn, None).unwrap();

        let result = matches.step().unwrap().unwrap();
        assert_eq!(PropertyRef::Id(a), result.get(0).unwrap());
        assert_eq!(PropertyRef::Id(b), result.get(1).unwrap());
        assert_eq!(PropertyRef::Id(eab), result.get(2).unwrap());

        let result = matches.step().unwrap().unwrap();
        assert_eq!(PropertyRef::Id(b), result.get(0).unwrap());
        assert_eq!(PropertyRef::Id(a), result.get(1).unwrap());
        assert_eq!(PropertyRef::Id(eba), result.get(2).unwrap());

        assert!(matches.step().unwrap().is_none());
    }

    #[test]
    fn run_a_edge_b() {
        let graph = Graph::open_anon().unwrap();

        // TODO
        let mut txn = graph.store.mut_txn().unwrap();
        let a = txn.create_node("PERSON", None).unwrap().id;
        let b = txn.create_node("PERSON", None).unwrap().id;
        let e = txn.create_edge("KNOWS", a, b, None).unwrap().id;
        txn.commit().unwrap();

        let stmt = graph
            .prepare("MATCH (a) -[e]- (b) RETURN ID(a), ID(b), ID(e)")
            .unwrap();
        let mut txn = graph.txn().unwrap();
        let mut matches = stmt.query(&mut txn, None).unwrap();

        let result = matches.step().unwrap().unwrap();
        assert_eq!(PropertyRef::Id(a), result.get(0).unwrap());
        assert_eq!(PropertyRef::Id(b), result.get(1).unwrap());
        assert_eq!(PropertyRef::Id(e), result.get(2).unwrap());

        let result = matches.step().unwrap().unwrap();
        assert_eq!(PropertyRef::Id(b), result.get(0).unwrap());
        assert_eq!(PropertyRef::Id(a), result.get(1).unwrap());
        assert_eq!(PropertyRef::Id(e), result.get(2).unwrap());

        assert!(matches.step().unwrap().is_none());
    }

    #[test]
    fn run_a_to_a() {
        let graph = Graph::open_anon().unwrap();

        // TODO
        let mut txn = graph.store.mut_txn().unwrap();
        let a = txn.create_node("PERSON", None).unwrap().id;
        let b = txn.create_node("PERSON", None).unwrap().id;
        let e1 = txn.create_edge("KNOWS", a, a, None).unwrap().id;
        let e2 = txn.create_edge("KNOWS", b, b, None).unwrap().id;
        txn.commit().unwrap();

        let stmt = graph
            .prepare("MATCH (a) -[e]-> (a) RETURN ID(a), ID(e)")
            .unwrap();
        let mut txn = graph.txn().unwrap();
        let mut matches = stmt.query(&mut txn, None).unwrap();

        let result = matches.step().unwrap().unwrap();
        assert_eq!(PropertyRef::Id(a), result.get(0).unwrap());
        assert_eq!(PropertyRef::Id(e1), result.get(1).unwrap());

        let result = matches.step().unwrap().unwrap();
        assert_eq!(PropertyRef::Id(b), result.get(0).unwrap());
        assert_eq!(PropertyRef::Id(e2), result.get(1).unwrap());
    }

    #[test]
    fn run_a_edge_a() {
        let graph = Graph::open_anon().unwrap();

        // TODO
        let mut txn = graph.store.mut_txn().unwrap();
        let a = txn.create_node("PERSON_A", None).unwrap().id;
        let b = txn.create_node("PERSON_B", None).unwrap().id;
        let e1 = txn.create_edge("KNOWS", a, a, None).unwrap().id;
        let e2 = txn.create_edge("KNOWS", b, b, None).unwrap().id;
        txn.commit().unwrap();

        let stmt = graph
            .prepare("MATCH (a) -[e]- (a) RETURN ID(a), ID(e)")
            .unwrap();
        let mut txn = graph.txn().unwrap();

        let mut matches = stmt.query(&mut txn, None).unwrap();

        let result = matches.step().unwrap().unwrap();
        assert_eq!(PropertyRef::Id(a), result.get(0).unwrap());
        assert_eq!(PropertyRef::Id(e1), result.get(1).unwrap());
        let result = matches.step().unwrap().unwrap();
        assert_eq!(PropertyRef::Id(a), result.get(0).unwrap());
        assert_eq!(PropertyRef::Id(e1), result.get(1).unwrap());

        let result = matches.step().unwrap().unwrap();
        assert_eq!(PropertyRef::Id(b), result.get(0).unwrap());
        assert_eq!(PropertyRef::Id(e2), result.get(1).unwrap());
        let result = matches.step().unwrap().unwrap();
        assert_eq!(PropertyRef::Id(b), result.get(0).unwrap());
        assert_eq!(PropertyRef::Id(e2), result.get(1).unwrap());

        assert!(matches.step().unwrap().is_none());
    }

    #[test]
    fn run_a_knows_b() {
        let graph = Graph::open_anon().unwrap();

        // TODO
        let mut txn = graph.store.mut_txn().unwrap();
        let a = txn.create_node("PERSON_A", None).unwrap().id;
        let b = txn.create_node("PERSON_B", None).unwrap().id;
        let e = txn.create_edge("KNOWS", a, b, None).unwrap().id;
        txn.create_edge("HEARD_OF", b, a, None).unwrap();
        txn.commit().unwrap();

        let stmt = graph
            .prepare("MATCH (a) -[e:KNOWS]-> (b) RETURN ID(a), ID(b), ID(e)")
            .unwrap();
        let mut txn = graph.txn().unwrap();
        let mut matches = stmt.query(&mut txn, None).unwrap();

        let result = matches.step().unwrap().unwrap();
        assert_eq!(PropertyRef::Id(a), result.get(0).unwrap());
        assert_eq!(PropertyRef::Id(b), result.get(1).unwrap());
        assert_eq!(PropertyRef::Id(e), result.get(2).unwrap());

        assert!(matches.step().unwrap().is_none());
    }

    #[test]
    fn run_a_edge_b_with_where_property() {
        let graph = Graph::open_anon().unwrap();

        // TODO
        let mut txn = graph.store.mut_txn().unwrap();
        let a = txn.create_node("PERSON", None).unwrap();
        txn.update_node(a.id(), "test", Property::Integer(42))
            .unwrap();
        let b = txn.create_node("PERSON", None).unwrap();
        txn.create_edge("KNOWS", a.id(), b.id(), None).unwrap();
        txn.commit().unwrap();

        let stmt = graph
            .prepare(
                "
                            MATCH (a:PERSON) -[:KNOWS]- (b:PERSON)
                            WHERE a.test = 42
                            RETURN ID(a), ID(b)
                            ",
            )
            .unwrap();
        let mut txn = graph.txn().unwrap();
        let mut matches = stmt.query(&mut txn, None).unwrap();

        let result = matches.step().unwrap().unwrap();
        assert_eq!(PropertyRef::Id(a.id()), result.get(0).unwrap());
        assert_eq!(PropertyRef::Id(b.id()), result.get(1).unwrap());

        assert!(matches.step().unwrap().is_none());
    }

    #[test]
    fn run_a_edge_b_with_property_map() {
        let graph = Graph::open_anon().unwrap();

        // TODO
        let mut txn = graph.store.mut_txn().unwrap();
        let a = txn.create_node("PERSON", None).unwrap();
        txn.update_node(a.id(), "test", Property::Text("hello world!".to_string()))
            .unwrap();
        let b = txn.create_node("PERSON", None).unwrap();
        txn.create_edge("KNOWS", a.id(), b.id(), None).unwrap();
        txn.commit().unwrap();

        let stmt = graph
            .prepare(
                "
                MATCH (a:PERSON { test: 'hello world!' }) -[:KNOWS]- (b:PERSON)
                RETURN ID(a), ID(b)
                ",
            )
            .unwrap();
        let mut txn = graph.txn().unwrap();
        let mut matches = stmt.query(&mut txn, None).unwrap();

        let result = matches.step().unwrap().unwrap();
        assert_eq!(PropertyRef::Id(a.id()), result.get(0).unwrap());
        assert_eq!(PropertyRef::Id(b.id()), result.get(1).unwrap());

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
                RETURN ID(a)
                ",
            )
            .unwrap();
        let mut txn = graph.txn().unwrap();
        let mut matches = stmt.query(&mut txn, None).unwrap();

        let result = matches.step().unwrap().unwrap();
        assert_eq!(PropertyRef::Id(1), result.get(0).unwrap());

        assert!(matches.step().unwrap().is_none());
    }

    #[test]
    fn run_a_where_with_parameters() {
        let graph = Graph::open_anon().unwrap();

        // TODO
        let mut txn = graph.store.mut_txn().unwrap();
        let a = txn
            .create_node(
                "PERSON",
                Some(
                    vec![
                        ("name".into(), Property::Text("Peter Parker".into())),
                        ("age".into(), Property::Real(21.0)),
                    ]
                    .into_iter()
                    .collect(),
                ),
            )
            .unwrap();
        let b = txn.create_node("PERSON", None).unwrap();
        txn.create_edge("KNOWS", a.id(), b.id(), None).unwrap();
        txn.commit().unwrap();

        let stmt = graph
            .prepare(
                "
                MATCH (a:PERSON)
                WHERE a.age >= $min_age
                RETURN ID(a)
                ",
            )
            .unwrap();
        let mut txn = graph.txn().unwrap();
        let mut matches = stmt
            .query(
                &mut txn,
                Some(
                    vec![("min_age".into(), Property::Integer(18))]
                        .into_iter()
                        .collect(),
                ),
            )
            .unwrap();

        let result = matches.step().unwrap().unwrap();
        assert_eq!(PropertyRef::Id(a.id()), result.get(0).unwrap());
        assert!(matches.step().unwrap().is_none());
    }

    #[test]
    fn run_set() {
        let graph = Graph::open_anon().unwrap();

        // TODO
        let mut txn = graph.store.mut_txn().unwrap();
        txn.create_node("PERSON", None).unwrap();
        txn.commit().unwrap();

        let stmt = graph.prepare("MATCH (a:PERSON) SET a.answer = 42").unwrap();
        let mut txn = graph.mut_txn().unwrap();
        stmt.execute(&mut txn, None).unwrap();
        txn.commit().unwrap();

        let stmt = graph
            .prepare("MATCH (a:PERSON) WHERE ID(a) = 0 RETURN a.answer")
            .unwrap();
        let mut txn = graph.txn().unwrap();
        let mut query = stmt.query(&mut txn, None).unwrap();
        let results = query.step().unwrap().unwrap();
        assert_eq!(PropertyRef::Integer(42), results.get(0).unwrap());
    }

    #[test]
    fn run_delete() {
        let graph = Graph::open_anon().unwrap();

        // TODO
        let mut txn = graph.store.mut_txn().unwrap();
        txn.create_node("PERSON", None).unwrap();
        txn.commit().unwrap();

        let stmt = graph.prepare("MATCH (a:PERSON) RETURN ID(a)").unwrap();
        let mut txn = graph.txn().unwrap();
        let mut query = stmt.query(&mut txn, None).unwrap();
        assert_eq!(
            PropertyRef::Id(0),
            query.step().unwrap().unwrap().get(0).unwrap(),
        );
        assert!(query.step().unwrap().is_none());

        let del_stmt = graph.prepare("MATCH (a:PERSON) DELETE a").unwrap();
        let mut txn = graph.mut_txn().unwrap();
        del_stmt.execute(&mut txn, None).unwrap();
        txn.commit().unwrap();

        let mut txn = graph.txn().unwrap();
        let mut query = stmt.query(&mut txn, None).unwrap();
        assert!(query.step().unwrap().is_none());
    }
}
