#include <semaphore.h>
#include <stdio.h>

#include <sqlite3.h>
#include <uv.h>

#include "../include/dqlite.h"

#include "./lib/assert.h"

#include "conn.h"
#include "error.h"
#include "lifecycle.h"
#include "queue.h"

int dqlite__queue_item_init(struct dqlite__queue_item *i,
                            struct conn *      conn) {
	int err;

	assert(i != NULL);

	dqlite__lifecycle_init(DQLITE__LIFECYCLE_QUEUE_ITEM);

	dqlite__error_init(&i->error);

	i->conn = conn;

	/* We'll leave this semaphore once the queued connection gets
	 * processed */
	err = sem_init(&i->pending, 0, 0);
	if (err != 0) {
		dqlite__error_close(&i->error);
		dqlite__lifecycle_close(DQLITE__LIFECYCLE_QUEUE_ITEM);
		return err;
	}

	return 0;
}

void dqlite__queue_item_close(struct dqlite__queue_item *i) {
	int err;

	assert(i != NULL);

	/* The sem_destroy call should only fail if the given semaphore is
	 * invalid, which must not be our case. */
	err = sem_destroy(&i->pending);
	assert(err == 0);

	dqlite__error_close(&i->error);

	dqlite__lifecycle_close(DQLITE__LIFECYCLE_QUEUE_ITEM);
}

static void dqlite__queue_item_process(struct dqlite__queue_item *i) {
	int err;

	assert(i != NULL);

	err = conn__start(i->conn);
	if (err != 0) {
		dqlite__error_wrapf(
		    &i->error, &i->conn->error, "failed to init connection");
	}

	err = sem_post(&i->pending);
	assert(err == 0); /* No reason for which posting should fail */
}

void dqlite__queue_item_wait(struct dqlite__queue_item *i) {
	assert(i != NULL);
	assert(i->conn != NULL);

	/* Wait for the pending mutex to be released by the main dqlite loop */
	sem_wait(&i->pending);
}

void dqlite__queue_init(struct dqlite__queue *q) {
	assert(q != NULL);

	dqlite__lifecycle_init(DQLITE__LIFECYCLE_QUEUE);

	dqlite__error_init(&q->error);

	q->incoming = NULL;
	q->length   = 0;
}

void dqlite__queue_close(struct dqlite__queue *q) {
	assert(q != NULL);

	/* Assert that the queue is empty */
	assert(q->incoming == NULL);
	assert(q->length == 0);

	dqlite__error_close(&q->error);

	dqlite__lifecycle_close(DQLITE__LIFECYCLE_QUEUE);
}

int dqlite__queue_push(struct dqlite__queue *q, struct dqlite__queue_item *item) {
	int                         length;
	struct dqlite__queue_item **incoming;

	assert(q != NULL);
	assert(item != NULL);

	/* Increase the size of the incoming queue by 1 */
	length   = q->length + 1;
	incoming = (struct dqlite__queue_item **)sqlite3_realloc(
	    q->incoming, sizeof(*incoming) * (length));

	if (incoming == NULL) {
		dqlite__error_oom(&q->error, "failed to grow incoming queue");
		return DQLITE_NOMEM;
	}

	incoming[q->length] = item;

	q->incoming = incoming;
	q->length   = length;

	return 0;
}

struct dqlite__queue_item *dqlite__queue_pop(struct dqlite__queue *q) {
	struct dqlite__queue_item *item;

	if (!q->length) {
		return NULL;
	}

	q->length--;
	item = q->incoming[q->length];

	if (q->length == 0) {
		/* If the queue drops to zero items, free the incoming array */
		sqlite3_free(q->incoming);
		q->incoming = NULL;
	}

	return item;
}

void dqlite__queue_process(struct dqlite__queue *q) {
	struct dqlite__queue_item *item;

	assert(q != NULL);

	while ((item = dqlite__queue_pop(q)) != NULL) {
		dqlite__queue_item_process(item);
	}
}
