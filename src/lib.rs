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
pub use store::Property;

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

    pub fn prepare<'graph>(&'graph self, query: &str) -> Result<Statement<'graph>, Error> {
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
}

impl<'graph> Txn<'graph> {
    pub fn commit(self) -> Result<(), Error> {
        self.0.commit()
    }
}

impl<'graph> Statement<'graph> {
    /// TODO: Have a parameter trait
    pub fn query<'stmt, 'txn>(
        &'stmt self,
        txn: &'txn mut Txn<'graph>,
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
    pub fn execute<'stmt, 'txn>(
        &'stmt self,
        txn: &'txn mut Txn<'stmt>,
        parameters: Option<HashMap<String, Property>>,
    ) -> Result<(), Error> {
        let mut query = self.query(txn, parameters)?;
        while let Some(_) = query.step()? {}
        txn.0.flush()?;
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
    pub fn get(&self, idx: usize) -> Result<Property, Error> {
        self.query.vm.access_return(idx)
    }
}
