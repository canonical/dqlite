#include <stddef.h>

#include <sqlite3.h>

#include "lib/assert.h"

#include <stdio.h>

#include "tx.h"

void tx__init(struct tx *tx, unsigned long long id, sqlite3 *conn)
{
	tx->id = id;
	tx->conn = conn;
	tx->is_zombie = false;
	tx->state = TX__PENDING;
	tx->dry_run = tx__is_leader(tx);
	DEBUG_TX(tx, "ok");
}

void tx__close(struct tx *tx)
{
	DEBUG_TX(tx, "close");
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
	DEBUG_TX(tx, (replication != NULL ? "leader" : "follower"));
	return replication != NULL;
}

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
		DEBUG_TX(tx, "pending");
		assert(tx->state == TX__PENDING);
	} else {
		DEBUG_TX(tx, "writing");
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
		DEBUG_TX(tx, "is_commit");
		tx->state = TX__WRITTEN;
	} else {
		DEBUG_TX(tx, "is_writing");
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

	DEBUG_TX(tx, "pre-undo");
	rc = sqlite3_wal_replication_undo(tx->conn, "main");
	if (rc != 0) {
		DEBUG_TX(tx, "undo failed");
		return rc;
	}

out:
	tx->state = TX__UNDONE;
	DEBUG_TX(tx, "undone");

	return 0;
}

void tx__zombie(struct tx *tx) {
	assert(tx__is_leader(tx));
	assert(!tx->is_zombie);
	tx->is_zombie = true;
	DEBUG_TX(tx, "zombie transaction");
}

void tx__surrogate(struct tx *tx, sqlite3 *conn) {
	assert(tx__is_leader(tx));
	assert(tx->dry_run);
	assert(tx->is_zombie);
	assert(tx->state == TX__WRITING);

	tx->conn = conn;
	tx->is_zombie = false;
	DEBUG_TX(tx, "surragate");
}

#ifdef DEBUG_VERBOSE

#include <time.h>

void ts (void)
{
    struct timespec spec;
    clock_gettime(CLOCK_REALTIME, &spec);

    struct tm *ptm = localtime((const time_t *) & spec.tv_sec);

    int hour   = ptm->tm_hour;;
    int minute = ptm->tm_min;
    int second = ptm->tm_sec;
    int micros = spec.tv_nsec / 1000;
    int day    = ptm->tm_mday;
    int month  = ptm->tm_mon + 1;
    int year   = ptm->tm_year + 1900;

    printf("%4d/%02d/%02d %02d:%02d:%02d.%06d %s ", year, month, day, hour, minute, second, micros, tzname[0]);
}

#endif // DEBUG_VERBOSE
