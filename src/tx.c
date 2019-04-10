#include <stddef.h>

#include <sqlite3.h>

#include "lib/assert.h"

#include "tx.h"

void tx__init(struct tx *tx, unsigned long long id, sqlite3 *conn)
{
	tx->id = id;
	tx->conn = conn;
	tx->is_zombie = false;
	tx->state = TX__PENDING;
	tx->dry_run = tx__is_leader(tx);
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

	if (tx->dry_run) {
		/* In leader or surrogate mode, don't actually invoke SQLite
		 * replication API, since that will be done by SQLite
		 * internally.*/
		goto out;
	}

	if (is_begin) {
		assert(tx->state == TX__PENDING);
	} else {
		assert(tx->state == TX__WRITING);
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

int tx__undo(struct tx *tx) {
	int rc;

	if (tx->dry_run) {
		/* In leader or surrogate mode, don't actually invoke SQLite
		 * replication API, since that will be done by SQLite
		 * internally.*/
		goto out;
	}

	assert(tx->state == TX__PENDING || tx->state == TX__WRITING);

	rc = sqlite3_wal_replication_undo(tx->conn, "main");
	if (rc != 0) {
		return rc;
	}

out:
	tx->state = TX__UNDONE;

	return 0;
}

void tx__zombie(struct tx *tx) {
	assert(tx__is_leader(tx));
	assert(!tx->is_zombie);
	tx->is_zombie = true;
}

void tx__surrogate(struct tx *tx, sqlite3 *conn) {
	assert(tx__is_leader(tx));
	assert(tx->dry_run);
	assert(tx->is_zombie);
	assert(tx->state == TX__WRITING);

	tx->conn = conn;
	tx->is_zombie = false;
}
