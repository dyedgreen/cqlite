#ifndef CQLITE_H
#define CQLITE_H

#include <stdint.h>
#include <stdbool.h>

enum CQLiteStatus {
  CQLITE_OK,
  CQLITE_MATCH,
  CQLITE_DONE,
  CQLITE_IO,
  CQLITE_CORRUPTION,
  CQLITE_POISON,
  CQLITE_INTERNAL,
  CQLITE_READ_ONLY_WRITE,
  CQLITE_SYNTAX,
  CQLITE_IDENTIFIER_IS_NOT_NODE,
  CQLITE_IDENTIFIER_IS_NOT_EDGE,
  CQLITE_IDENTIGIER_EXISTS,
  CQLITE_UNKNOWN_IDENTIFIER,
  CQLITE_TYPE_MISMATCH,
  CQLITE_INDEX_OUT_OF_BOUNDS,
  CQLITE_MISSING_NODE,
  CQLITE_MISSING_EDGE,
  CQLITE_DELETE_CONNECTED,
  CQLITE_INVALID_STRING,
  CQLITE_OPEN_TRANSACTION,
  CQLITE_OPEN_STATEMENT,
  CQLITE_MISUSE,
};
typedef uint8_t CQLiteStatus;

enum CQLiteType {
  CQLITE_ID,
  CQLITE_INTEGER,
  CQLITE_REAL,
  CQLITE_BOOLEAN,
  CQLITE_TEXT,
  CQLITE_BLOB,
  CQLITE_NULL,
};
typedef uint8_t CQLiteType;

typedef struct CQLiteGraph CQLiteGraph;

typedef struct CQLiteStatement CQLiteStatement;

typedef struct CQLiteTxn CQLiteTxn;

CQLiteStatus cqlite_open(const char *path, struct CQLiteGraph **graph);

CQLiteStatus cqlite_open_anon(struct CQLiteGraph **graph);

CQLiteStatus cqlite_close(struct CQLiteGraph *graph);

CQLiteStatus cqlite_txn(const struct CQLiteGraph *graph, struct CQLiteTxn **txn);

CQLiteStatus cqlite_mut_txn(const struct CQLiteGraph *graph, struct CQLiteTxn **txn);

CQLiteStatus cqlite_drop(struct CQLiteTxn *txn);

CQLiteStatus cqlite_commit(struct CQLiteTxn *txn);

CQLiteStatus cqlite_prepare(const struct CQLiteGraph *graph,
                            const char *query,
                            struct CQLiteStatement **stmt);

CQLiteStatus cqlite_start(struct CQLiteStatement *stmt, struct CQLiteTxn *txn);

CQLiteStatus cqlite_step(struct CQLiteStatement *stmt);

CQLiteStatus cqlite_finalize(struct CQLiteStatement *stmt);

CQLiteStatus cqlite_bind_id(struct CQLiteStatement *stmt, const char *name, uint64_t value);

CQLiteStatus cqlite_bind_integer(struct CQLiteStatement *stmt, const char *name, int64_t value);

CQLiteStatus cqlite_bind_real(struct CQLiteStatement *stmt, const char *name, double value);

CQLiteStatus cqlite_bind_boolean(struct CQLiteStatement *stmt, const char *name, bool value);

CQLiteStatus cqlite_bind_text(struct CQLiteStatement *stmt, const char *name, const char *value);

CQLiteStatus cqlite_bind_blob(struct CQLiteStatement *stmt,
                              const char *name,
                              const void *value,
                              uintptr_t length);

CQLiteStatus cqlite_bind_null(struct CQLiteStatement *stmt, const char *name);

CQLiteType cqlite_return_type(struct CQLiteStatement *stmt, uintptr_t idx);

uint64_t cqlite_return_id(struct CQLiteStatement *stmt, uintptr_t idx);

int64_t cqlite_return_integer(struct CQLiteStatement *stmt, uintptr_t idx);

double cqlite_return_real(struct CQLiteStatement *stmt, uintptr_t idx);

bool cqlite_return_boolean(struct CQLiteStatement *stmt, uintptr_t idx);

const char *cqlite_return_text(struct CQLiteStatement *stmt, uintptr_t idx);

const void *cqlite_return_blob(struct CQLiteStatement *stmt, uintptr_t idx);

uintptr_t cqlite_return_bytes(struct CQLiteStatement *stmt, uintptr_t idx);

#endif /* CQLITE_H */
