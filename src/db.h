/**
 * State of a single database.
 */

#ifndef DB_H_
#define DB_H_

#include "lib/queue.h"

#include "config.h"
#include "tx.h"

struct db
{
	struct config *config; /* Dqlite configuration */
	char *filename;        /* Database filename */
	bool opening;          /* Whether an Open request is in progress */
	sqlite3 *follower;     /* Follower connection */
	queue leaders;         /* Open leader connections */
	struct tx *tx;         /* Current ongoing transaction, if any */
	unsigned tx_id;        /* Current ongoing transaction ID, if any */
	queue queue;           /* Prev/next database, used by the registry */
};

/**
 * Initialize a database object.
 *
 * The given @filename will be copied.
 */
void db__init(struct db *db, struct config *config, const char *filename);

/**
 * Release all memory associated with a database object.
 *
 * If the follower connection was opened, it will be closed.
 */
void db__close(struct db *db);

/**
 * Open the follower connection associated with this database.
 */
int db__open_follower(struct db *db);

/**
 * Create an initialize the matadata of a new write transaction against this
 * database.
 *
 * The given conn @conn can be either a leader or a follower transaction. There
 * must be no ongoing write transaction for this database.
 */
int db__create_tx(struct db *db, unsigned long long id, sqlite3 *conn);

/**
 * Delete the transaction metadata object associated with this database.
 */
void db__delete_tx(struct db *db);

#endif /* DB_H_*/
