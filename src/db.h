/**
 *
 * State of a single database.
 *
 */

#ifndef DB_H_
#define DB_H_

#include "./lib/queue.h"

#include "options.h"
#include "tx.h"

struct db
{
	struct options *options; /* Dqlite options */
	char *filename;		 /* Filename identifying the database */
	sqlite3 *follower;       /* Follower connection, used for replication */
	queue leaders;		 /* All open leader connections */
	struct tx *tx;		 /* Current ongoing transaction, if any */
	queue queue;		 /* Prev/next database, used by the registry */
};

/**
 * Initialize a database object.
 *
 * The given @filename will be copied.
 */
void db__init(struct db *db, struct options *options, const char *filename);

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
