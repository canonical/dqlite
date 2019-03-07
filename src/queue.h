#ifndef DQLITE_QUEUE_H
#define DQLITE_QUEUE_H

#include <semaphore.h>

#include "error.h"

/* A an item in the incoming connection queue */
struct dqlite__queue_item {
	/* read-only */
	dqlite__error        error; /* Last error occurred, if any */
	struct conn *conn;  /* Incoming connection */

	/* private */
	sem_t pending; /* Block until the connection gets processed */
};

int dqlite__queue_item_init(struct dqlite__queue_item *i, struct conn *conn);

/* Must be called only once the queue is empty */
void dqlite__queue_item_close(struct dqlite__queue_item *i);

/* Wait for the given enqueued connection to be accepted or refused */
void dqlite__queue_item_wait(struct dqlite__queue_item *i);

/* Queue of incoming connections */
struct dqlite__queue {
	/* read-only */
	dqlite__error error; /* Last error occurred, if any */

	/* private */
	struct dqlite__queue_item **incoming; /* Array incoming connections */
	unsigned                    length;   /* Number of incoming connections */
};

void dqlite__queue_init(struct dqlite__queue *q);

void dqlite__queue_close(struct dqlite__queue *q);

int dqlite__queue_push(struct dqlite__queue *q, struct dqlite__queue_item *item);

struct dqlite__queue_item *dqlite__queue_pop(struct dqlite__queue *q);

void dqlite__queue_process(struct dqlite__queue *q);

#endif /* DQLITE_QUEUE_H */
