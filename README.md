# Cypher Lite / GQLite

[![crates.io](https://img.shields.io/crates/v/gqlite.svg)](https://crates.io/crates/gqlite)
[![Released API docs](https://docs.rs/gqlite/badge.svg)](https://docs.rs/gqlite)
[![CI](https://github.com/dyedgreen/gqlite/actions/workflows/ci.yml/badge.svg)](https://github.com/dyedgreen/gqlite/actions/workflows/ci.yml)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

An embedded graph database implemented in Rust. This is currently a pre-release. It has not been
extensively tested with 'real-world work-loads', and the file-format and API are not yet stabilized.

The longer term goal is to create an in-process graph database with a stable on-disk format and
support for a wide range of programming languages, providing a native Rust API, as well as a C FFI interface.

```rust
use gqlite::Graph;

let graph = Graph::open_anon()?;

let mut txn = graph.mut_txn()?;
let edge: u64 = graph.prepare(
        "
        CREATE (a:PERSON { name: 'Peter Parker' })
        CREATE (b:PERSON { name: 'Clark Kent' })
        CREATE (a) -[e:KNOWS]-> (b)
        RETURN ID(e)
        "
    )?
    .query_map(&mut txn, (), |m| m.get(0))?
    .next()
    .unwrap()?;
txn.commit()?;

let name: String = graph.prepare(
        "
        MATCH (p:PERSON) <-[e:KNOWS]- (:PERSON)
        WHERE ID(e) = $edge
        RETURN p.name
        "
    )?
    .query_map(&mut graph.txn()?, ("edge", edge), |m| m.get(0))?
    .next()
    .unwrap()?;
assert_eq!("Clark Kent", name);
```


## Architecture Overview


### Parser :: `src/parser`

PEG grammar and parser for a subset of the `CYPHER` graph query language

### Query Planner :: `src/planner`

Transforms a parsed query ast into a logical query plan. Performs some
optimizations on the query plan.

### Byte-Code Interpreter :: `src/runtime`

Defines a simple 'byte' code (`Instructions`) and can execute those against a given
database, as well as generate instructions for a given query plan.

### Storage Backend :: `src/store`

Uses a disc-backed `btree` to provide basic storage, iteration and lockup for nodes and
edges.
