#include <stddef.h>

#include <sqlite3.h>

#include "./lib/assert.h"

#include "tx.h"

void tx__init(struct tx *tx, unsigned long long id, sqlite3 *conn)
{
	tx->id = id;
	tx->conn = conn;
	tx->is_zombie = false;
	tx->state = TX__PENDING;
}

bool tx__is_leader(struct tx *tx)
{
	sqlite3_wal_replication *replication;
	int enabled;
	int rc;
	assert(tx->conn != NULL);
	rc = sqlite3_wal_replication_enabled(tx->conn, "main", &enabled,
					     &replication);
	assert(rc == SQLITE_OK);
	assert(enabled == 1);
	return replication != NULL;
};
