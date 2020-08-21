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
	(void)tx;
	return true;
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
