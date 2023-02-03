#include "include/dqlite/client.h"

#include <sqlite3.h>

#include <assert.h>
#include <stdlib.h>

#define N_ADDRS 1
static const char *const addrs[N_ADDRS] = {"127.0.0.1:8001"};

int main(void)
{
	dqlite *db;
	dqlite_stmt *stmt;
	int rv;

	rv = dqlite_open("./stuff", "whatever", &db, addrs, N_ADDRS, 0);
	if (rv != 0) {
		abort();
	}
	rv = dqlite_exec(db, "CREATE TABLE foo (n INT)", NULL, NULL, NULL);
	if (rv != 0) {
		abort();
	}
	rv = dqlite_prepare(db, "SELECT * FROM foo", -1, &stmt, NULL);
	if (rv != 0) {
		abort();
	}
	rv = dqlite_exec(db, "INSERT INTO foo (n) VALUES (1)", NULL, NULL, NULL);
	if (rv != 0) {
		abort();
	}
	rv = dqlite_step(stmt);
	assert(rv == SQLITE_ROW);
	rv = dqlite_step(stmt);
	assert(rv == SQLITE_DONE);
	return 0;
}
