//! GQLite / Cypher Lite (TODO: Name!!!)
//!
//! # Example
//! ```
//! # fn test() -> Result<(), gqlite::Error> {
//! use gqlite::Graph;
//!
//! let graph = Graph::open_anon()?;
//!
//! let mut txn = graph.mut_txn()?;
//! let edge: u64 = graph.prepare(
//!     "
//!     CREATE (a:PERSON { name: 'Peter Parker' })
//!     CREATE (b:PERSON { name: 'Clark Kent' })
//!     CREATE (a) -[e:KNOWS]-> (b)
//!     RETURN ID(e)
//!     "
//!   )?
//!   .query_map(&mut txn, (), |m| m.get(0))?
//!   .next()
//!   .unwrap()?;
//! txn.commit()?;
//!
//! let name: String = graph.prepare(
//!     "
//!     MATCH (p:PERSON) <-[e:KNOWS]- (:PERSON)
//!     WHERE ID(e) = $edge
//!     RETURN p.name
//!     "
//!   )?
//!   .query_map(&mut graph.txn()?, ("edge", edge), |m| m.get(0))?
//!   .next()
//!   .unwrap()?;
//! assert_eq!("Clark Kent", name);
//! # Ok(())
//! # }
//! # test().unwrap();
//! ```

use planner::QueryPlan;
use runtime::{Program, Status, VirtualMachine};
use std::{convert::TryInto, path::Path};
use store::{Store, StoreTxn};

pub(crate) mod error;
pub(crate) mod params;
pub(crate) mod parser;
pub(crate) mod planner;
pub(crate) mod runtime;
pub(crate) mod store;

pub use error::Error;
pub use params::Params;
pub use store::Property;

/// TODO: A handle to the database
pub struct Graph {
    store: Store,
}

/// TODO: A read or read/ write transaction
/// within the database
pub struct Txn<'graph>(StoreTxn<'graph>);

/// TODO: A prepared statement
pub struct Statement<'graph> {
    _graph: &'graph Graph,
    program: Program,
}

/// TODO: A running query, the same statement
/// can be run concurrently ...
pub struct Query<'stmt, 'txn> {
    stmt: &'stmt Statement<'stmt>,
    vm: VirtualMachine<'stmt, 'txn, 'stmt>,
}

/// TODO: A RAII guard to access a set of matched
/// nodes and edges
pub struct Match<'query> {
    query: &'query Query<'query, 'query>,
}

/// TODO: A query iterator
pub struct MappedQuery<'stmt, 'txn, F> {
    query: Query<'stmt, 'txn>,
    map: F,
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
        let plan = QueryPlan::new(&ast)?.optimize()?;
        Ok(Statement {
            _graph: self,
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
    pub fn query<'stmt, 'txn, P>(
        &'stmt self,
        txn: &'txn mut Txn<'graph>,
        params: P,
    ) -> Result<Query<'stmt, 'txn>, Error>
    where
        P: Params,
    {
        txn.0.flush()?;
        Ok(Query {
            stmt: self,
            vm: VirtualMachine::new(&mut txn.0, &self.program, params.build()),
        })
    }

    pub fn query_map<'stmt, 'txn, T, P, F>(
        &'stmt self,
        txn: &'txn mut Txn<'graph>,
        params: P,
        map: F,
    ) -> Result<MappedQuery<'stmt, 'txn, F>, Error>
    where
        P: Params,
        F: FnMut(Match<'_>) -> Result<T, Error>,
    {
        Ok(MappedQuery {
            query: self.query(txn, params)?,
            map,
        })
    }

    pub fn execute<'stmt, 'txn, P>(
        &'stmt self,
        txn: &'txn mut Txn<'graph>,
        params: P,
    ) -> Result<(), Error>
    where
        P: Params,
    {
        let mut query = self.query(txn, params)?;
        while query.step()?.is_some() {}
        txn.0.flush()?;
        Ok(())
    }
}

impl<'stmt, 'txn> Query<'stmt, 'txn> {
    #[inline]
    pub fn step(&mut self) -> Result<Option<Match>, Error> {
        if self.stmt.program.returns.is_empty() {
            loop {
                match self.vm.run()? {
                    Status::Yield => continue,
                    Status::Halt => break Ok(None),
                }
            }
        } else {
            match self.vm.run()? {
                Status::Yield => Ok(Some(Match { query: self })),
                Status::Halt => Ok(None),
            }
        }
    }
}

impl<'query> Match<'query> {
    pub fn get<P, E>(&self, idx: usize) -> Result<P, Error>
    where
        Property: TryInto<P, Error = E>,
        Error: From<E>,
    {
        Ok(self.query.vm.access_return(idx)?.try_into()?)
    }
}

impl<'stmt, 'txn, T, F> Iterator for MappedQuery<'stmt, 'txn, F>
where
    F: FnMut(Match<'_>) -> Result<T, Error>,
{
    type Item = Result<T, Error>;

    fn next(&mut self) -> Option<Result<T, Error>> {
        let query = &mut self.query;
        let map = &mut self.map;
        query.step().transpose().map(|res| res.and_then(map))
    }
}
