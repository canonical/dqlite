#include <assert.h>
#include <stddef.h>

#include <sqlite3.h>

#include "stmt.h"
#include "dqlite.h"
#include "lifecycle.h"
#include "registry.h"

void dqlite__stmt_init(struct dqlite__stmt *stmt)
{
	assert(stmt != NULL);

	dqlite__lifecycle_init(DQLITE__LIFECYCLE_STMT);
}

void dqlite__stmt_close(struct dqlite__stmt *stmt)
{
	assert(stmt != NULL);

	if (stmt->stmt != NULL) {
		/* Ignore the return code, since it will be non-zero in case the
		 * most rececent evaluation of the statement failed. */
		sqlite3_finalize(stmt->stmt); }

	dqlite__lifecycle_close(DQLITE__LIFECYCLE_STMT);
}

int dqlite__stmt_exec(
	struct dqlite__stmt *stmt,
	uint64_t *last_insert_id,
	uint64_t *rows_affected)
{
	int rc;

	assert(stmt != NULL);
	assert(stmt->stmt != NULL);

	rc = sqlite3_step(stmt->stmt);
	if (rc != SQLITE_DONE)
		return rc;

	*last_insert_id = sqlite3_last_insert_rowid(stmt->db);
	*rows_affected = sqlite3_changes(stmt->db);

	return 0;
}

DQLITE__REGISTRY_METHODS(dqlite__stmt_registry, dqlite__stmt);
