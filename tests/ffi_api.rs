#![cfg(feature = "ffi")]

use inline_c::assert_c;

#[test]
fn it_works() {
    (assert_c! {
        #include <stdio.h>
        #include <assert.h>
        #include "cqlite.h"

        int main() {
            CQLiteGraph *graph;
            assert(cqlite_open_anon(&graph) == CQLITE_OK);
            assert(cqlite_close(graph) == CQLITE_OK);

            return 0;
        }
    })
    .success();
}

#[test]
fn create_node_read_node() {
    (assert_c! {
        #include <stdio.h>
        #include <assert.h>
        #include "cqlite.h"

        int main() {
            CQLiteGraph *graph;
            assert(cqlite_open_anon(&graph) == CQLITE_OK);

            CQLiteStatement *stmt;
            CQLiteTxn *txn;

            // create nodes and edges
            assert(cqlite_prepare(
                graph,
                "CREATE (a:PERSON) CREATE (b:PERSON) CREATE (a) -[:KNOWS]-> (b)",
                &stmt
            ) == CQLITE_OK);
            assert(cqlite_mut_txn(graph, &txn) == CQLITE_OK);
            assert(cqlite_start(stmt, txn) == CQLITE_OK);
            assert(cqlite_step(stmt) == CQLITE_DONE);
            assert(cqlite_commit(txn) == CQLITE_OK);
            assert(cqlite_finalize(stmt) == CQLITE_OK);

            // match nodes and edges
            assert(cqlite_prepare(
                graph,
                "MATCH (a) -> (b) RETURN ID(a), ID(b)",
                &stmt
            ) == CQLITE_OK);
            assert(cqlite_txn(graph, &txn) == CQLITE_OK);
            assert(cqlite_start(stmt, txn) == CQLITE_OK);

            assert(cqlite_step(stmt) == CQLITE_MATCH);
            assert(cqlite_return_count(stmt) == 2);
            assert(cqlite_return_type(stmt, 0) == CQLITE_ID);
            assert(cqlite_return_type(stmt, 1) == CQLITE_ID);
            assert(cqlite_return_id(stmt, 0) == 0);
            assert(cqlite_return_id(stmt, 1) == 1);

            assert(cqlite_step(stmt) == CQLITE_DONE);
            assert(cqlite_drop(txn) == CQLITE_OK);
            assert(cqlite_finalize(stmt) == CQLITE_OK);

            assert(cqlite_close(graph) == CQLITE_OK);

            return 0;
        }
    })
    .success();
}
