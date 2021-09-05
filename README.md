# Cypher Lite / GQLite

An embedded graph database implemented with Rust. Currently WIP/ DRAFT ...


## Architecture Overview


### Parser :: `src/parser`

PEG grammar and parser for a subset of the `CYPHER` graph query language

### Query Planner :: `src/planner`

Transforms a parsed query ast into a logical query plan. (In the future) does some
optimizations on the query plan.

Finally, this turns the query plan into a sequence of `Instructions` to run in a
simple byte-code interpreter.

### Byte-Code Interpreter :: `src/runtime`

Defines a simple byte code (`Instructions`) and can execute those against a given
database.

### Storage Backend :: `src/store`

Uses a disc-backed `btree` to provide basic storage, iteration and lockup for nodes and
edges.
