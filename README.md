# Cypher Lite / GQLite

An embedded graph database implemented with Rust. Currently WIP/ DRAFT ...

```rust
use gqlite::Graph;

let graph = Graph::open("database.graph")?;

let create_stmt = graph.prepare(
  "
  CREATE (a:PERSON { name: 'Peter Parker' })
  CREATE (b:PERSON { name: 'Clark Kent' })
  CREATE (a) -[:KNOWS]-> (b)
  RETURN ID(a), ID(b)
  "
)?;
let mut txn = graph.mut_txn()?;
let query = create_stmt.query(&mut txn, None)?;
let vals = query.step()?;
let id_a: u64 = vals.get(0)?;
let id_b: u64 = vals.get(1)?;
txn.commit()?;

println!("ID(a) = {}, ID(b) = {}", id_a, id_b);

let stmt = grapg.prepare(
  "
  MATCH (p:PERSON) <-[:KNOWS]- (:PERSON)
  RETURN p.name
  "
)?;
let mut txn = graph.txn()?;
let query = stmt.query(&mut txn, None)?;
let vals = query.step()?;
assert_eq!("Clark Kent".to_string(), vals.get(0)?);
```


## Architecture Overview


### Parser :: `src/parser`

PEG grammar and parser for a subset of the `CYPHER` graph query language

### Query Planner :: `src/planner`

Transforms a parsed query ast into a logical query plan. (In the future) does some
optimizations on the query plan.

Finally, this turns the query plan into a sequence of `Instructions` to run in a
simple byte-code interpreter.

### Byte-Code Interpreter :: `src/runtime`

Defines a simple 'byte' code (`Instructions`) and can execute those against a given
database.

### Storage Backend :: `src/store`

Uses a disc-backed `btree` to provide basic storage, iteration and lockup for nodes and
edges.
