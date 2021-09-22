# Cypher Lite / GQLite

[![CI](https://github.com/dyedgreen/gqlite/actions/workflows/ci.yml/badge.svg)](https://github.com/dyedgreen/gqlite/actions/workflows/ci.yml)

An embedded graph database implemented with Rust. Currently WIP/ DRAFT ...

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

Transforms a parsed query ast into a logical query plan. (In the future) does some
optimizations on the query plan.

### Byte-Code Interpreter :: `src/runtime`

Defines a simple 'byte' code (`Instructions`) and can execute those against a given
database, as well as generate instructions for a given query plan.

### Storage Backend :: `src/store`

Uses a disc-backed `btree` to provide basic storage, iteration and lockup for nodes and
edges.
