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

void tx__close(struct tx *tx)
{
	(void)tx;
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

int tx__frames(struct tx *tx,
	       bool is_begin,
	       int page_size,
	       int n_frames,
	       unsigned *page_numbers,
	       void *pages,
	       unsigned truncate,
	       bool is_commit)
{
	int rc;

	if (is_begin) {
		assert(tx->state == TX__PENDING);
	} else {
		assert(tx->state == TX__WRITING);
	}

	if (tx__is_leader(tx)) {
		/* In leader mode, don't actually invoke SQLite replication API,
		 * since that will be done by SQLite internally.*/
		goto out;
	}

	rc = sqlite3_wal_replication_frames(tx->conn, "main", is_begin,
					    page_size, n_frames, page_numbers,
					    pages, truncate, is_commit);
	if (rc != SQLITE_OK) {
		return rc;
	}

out:
	if (is_commit) {
		tx->state = TX__WRITTEN;
	} else {
		tx->state = TX__WRITING;
	}

	return 0;
}
