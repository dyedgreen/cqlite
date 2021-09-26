//! GQLite provides an embedded graph database.
//!
//! A `Graph` can store a number of nodes, as well as edges
//! forming relationships between those nodes. Each node or
//! edge has a set of zero or more key value pairs called properties.
//!
//! The graph supports ACID queries using a simplified subset of the
//! [`CYPHER`](https://opencypher.org) graph query language, which can
//! be run inside read-only or read-write transactions.
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
//!         "
//!         CREATE (a:PERSON { name: 'Peter Parker' })
//!         CREATE (b:PERSON { name: 'Clark Kent' })
//!         CREATE (a) -[e:KNOWS]-> (b)
//!         RETURN ID(e)
//!         "
//!     )?
//!     .query_map(&mut txn, (), |m| m.get(0))?
//!     .next()
//!     .unwrap()?;
//! txn.commit()?;
//!
//! let name: String = graph.prepare(
//!         "
//!         MATCH (p:PERSON) <-[e:KNOWS]- (:PERSON)
//!         WHERE ID(e) = $edge
//!         RETURN p.name
//!         "
//!     )?
//!     .query_map(&mut graph.txn()?, ("edge", edge), |m| m.get(0))?
//!     .next()
//!     .unwrap()?;
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

/// A graph is a collection of nodes and edges.
///
/// Graphs may be held in-memory or persisted to a single
/// file and support ACID queries over the graph.
pub struct Graph {
    store: Store,
}

/// An ongoing transaction.
///
/// Any modifications to the graph that occurred during this transaction
/// are discarded unless the transaction is committed.
///
/// Once a transaction has started, it will not observe any later
/// modifications to the graph which occur inside other transactions.
pub struct Txn<'graph>(StoreTxn<'graph>);

/// A prepared statement.
pub struct Statement<'graph> {
    _graph: &'graph Graph,
    program: Program,
}

/// RAII guard which represents an ongoing query.
pub struct Query<'stmt, 'txn> {
    stmt: &'stmt Statement<'stmt>,
    vm: VirtualMachine<'stmt, 'txn, 'stmt>,
}

/// RAII guard which represents a set of nodes which
/// matches a query.
pub struct Match<'query> {
    query: &'query Query<'query, 'query>,
}

/// Iterator which yields all matches of the contained
/// query after mapping them with a user provided
/// function.
///
/// A `MappedQuery` can be obtained by calling
/// [`query_map`][Statement::query_map].
pub struct MappedQuery<'stmt, 'txn, F> {
    query: Query<'stmt, 'txn>,
    map: F,
}

impl Graph {
    /// Opens the file at the given path. If the file does not exist,
    /// it will be created. A newly created graph will start out empty.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn test() -> Result<(), gqlite::Error> {
    /// use gqlite::Graph;
    ///
    /// let graph = Graph::open("example.graph")?;
    /// # Ok(())
    /// # }
    /// # test().unwrap();
    /// ```
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let store = Store::open(path)?;
        Ok(Self { store })
    }

    /// Open an anonymous graph which is held in-memory.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn test() -> Result<(), gqlite::Error> {
    /// use gqlite::Graph;
    ///
    /// let graph = Graph::open_anon()?;
    /// # Ok(())
    /// # }
    /// # test().unwrap();
    /// ```
    pub fn open_anon() -> Result<Self, Error> {
        let store = Store::open_anon()?;
        Ok(Self { store })
    }

    /// Prepare a statement given a query `&str`. Queries support
    /// a subset of the [`CYPHER`](https://opencypher.org) graph
    /// query language.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn test() -> Result<(), gqlite::Error> {
    /// use gqlite::Graph;
    ///
    /// let graph = Graph::open_anon()?;
    /// let stmt = graph.prepare(
    ///     "
    ///     MATCH (a:PERSON { name: 'Test', age: 42 })
    ///     RETURN ID(a)
    ///     "
    /// )?;
    /// let stmt = graph.prepare(
    ///     "
    ///     MATCH (a:PERSON) -[:KNOWS]-> (b:PERSON)
    ///     RETURN a.name, b.name
    ///     "
    /// )?;
    /// let stmt = graph.prepare(
    ///     "
    ///     MATCH (a:PERSON)
    ///     MATCH (b:PERSON)
    ///     CREATE (a) -[:KNOWS { since: 'today' }]-> (b)
    ///     "
    /// )?;
    /// # Ok(())
    /// # }
    /// # test().unwrap();
    /// ```
    pub fn prepare<'graph>(&'graph self, query: &str) -> Result<Statement<'graph>, Error> {
        let ast = parser::parse(query)?;
        let plan = QueryPlan::new(&ast)?.optimize()?;
        Ok(Statement {
            _graph: self,
            program: Program::new(&plan)?,
        })
    }

    /// Start a new read-only transaction. There may be many simultaneous
    /// read-only transactions.
    pub fn txn(&self) -> Result<Txn, Error> {
        Ok(Txn(self.store.txn()?))
    }

    /// Start a new write transaction. Queries executed within this transaction
    /// may modify the graph. Multiple write transactions exclude each other.
    pub fn mut_txn(&self) -> Result<Txn, Error> {
        Ok(Txn(self.store.mut_txn()?))
    }
}

impl<'graph> Txn<'graph> {
    /// Commit any changes made to the graph using this transaction. This fails
    /// if the transaction was not created using [`mut_txn`][Graph::mut_txn].
    ///
    /// If a write transaction is dropped without calling `commit`, any modifications
    /// to the graph will be discarded.
    pub fn commit(self) -> Result<(), Error> {
        self.0.commit()
    }
}

impl<'graph> Statement<'graph> {
    /// Execute this statement. The returned `Query` RAII
    /// guard can be used to step through the produced matches.
    ///
    /// Queries may include parameters of the form `$identifier`.
    /// Parameters can be provided using the `params`
    /// argument, providing a value which implementing [`Params`][Params].
    /// If a value for a given parameter is not provided, it
    /// defaults to `NULL`.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn test() -> Result<(), gqlite::Error> {
    /// use gqlite::Graph;
    ///
    /// let graph = Graph::open_anon()?;
    /// let stmt = graph.prepare(
    ///     "
    ///     CREATE (a:PERSON { message: $msg, age: 42 })
    ///     RETURN ID(a), a.message, a.age
    ///     "
    /// )?;
    ///
    /// let mut txn = graph.mut_txn()?;
    /// let mut query = stmt.query(&mut txn, ("msg", "Hello World!"))?;
    ///
    /// let m = query.step()?.unwrap();
    /// assert_eq!(m.get::<u64, _>(0)?, 0);
    /// assert_eq!(m.get::<String, _>(1)?, "Hello World!");
    /// assert_eq!(m.get::<i64, _>(2)?, 42);
    ///
    /// assert!(query.step()?.is_none());
    ///
    /// txn.commit()?;
    /// # Ok(())
    /// # }
    /// # test().unwrap();
    /// ```
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

    /// Execute this statement and return an iterator which
    /// maps each match using a user-provided function.
    ///
    /// This is almost always more convenient than using
    /// [`query`][Statement::query] directly.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn test() -> Result<(), gqlite::Error> {
    /// use gqlite::Graph;
    ///
    /// let graph = Graph::open_anon()?;
    /// let stmt = graph.prepare(
    ///     "
    ///     CREATE (a:PERSON { message: $msg, age: 42 })
    ///     RETURN ID(a), a.message, a.age
    ///     "
    /// )?;
    ///
    /// let mut txn = graph.mut_txn()?;
    /// let nodes = stmt
    ///     .query_map(&mut txn, ("msg", "Hello World!"), |m| {
    ///         Ok((m.get(0)?, m.get(1)?, m.get(2)?))
    ///     })?
    ///     .collect::<Result<Vec<(u64, String, i64)>, _>>()?;
    ///
    /// assert_eq!(nodes, [(0, "Hello World!".into(), 42)]);
    ///
    /// txn.commit()?;
    /// # Ok(())
    /// # }
    /// # test().unwrap();
    /// ```
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

    /// Run the query to completion, ignoring any
    /// values which may be returned.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn test() -> Result<(), gqlite::Error> {
    /// use gqlite::Graph;
    ///
    /// let graph = Graph::open_anon()?;
    /// let mut txn = graph.mut_txn()?;
    /// graph
    ///     .prepare("CREATE (a:PERSON { message: $msg, age: 42 })")?
    ///     .execute(&mut txn, ("msg", "Hello World!"))?;
    /// txn.commit()?;
    /// # Ok(())
    /// # }
    /// # test().unwrap();
    /// ```
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
