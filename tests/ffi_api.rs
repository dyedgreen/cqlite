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
