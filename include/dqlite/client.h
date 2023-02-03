#ifndef DQLITE_CLIENT_H
#define DQLITE_CLIENT_H

#include <stdint.h>

typedef struct dqlite dqlite;
typedef struct dqlite_stmt dqlite_stmt;

int dqlite_open(
	const char *dir, const char *name, dqlite **db,
	const char *const *addrs, unsigned n_addrs, unsigned me
);
int dqlite_prepare(dqlite *db, const char *sql, int sql_len, dqlite_stmt **stmt, const char **tail);

int dqlite_bind_blob64(
	dqlite_stmt *stmt, int index,
	const void *blob, uint64_t blob_len, void (*dealloc)(void *)
);
int dqlite_bind_double(dqlite_stmt *stmt, int index, double val);
int dqlite_bind_int64(dqlite_stmt *stmt, int index, int64_t val);
int dqlite_bind_null(dqlite_stmt *stmt, int index);
int dqlite_bind_text64(
	dqlite_stmt *stmt, int index,
	const char *text, uint64_t text_len, void (*dealloc)(void *),
	unsigned char encoding
);

int dqlite_step(dqlite_stmt *stmt);

int dqlite_reset(dqlite_stmt *stmt);

int dqlite_finalize(dqlite_stmt *stmt);

int dqlite_exec(
	dqlite *db, const char *sql,
	int (*cb)(void *, int, char **, char **), void *cb_data, char **errmsg
);

void dqlite_close(dqlite *db);

#endif
