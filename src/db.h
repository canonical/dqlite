/**
 * State of a single database.
 */

#ifndef DB_H_
#define DB_H_

#include "lib/queue.h"

#include "config.h"

struct db
{
	struct config *config; /* Dqlite configuration */
	char *filename;        /* Database filename */
	char *path;            /* Used for on-disk db */
	sqlite3 *follower;     /* Follower connection */
	queue leaders;         /* Open leader connections */
	unsigned tx_id;        /* Current ongoing transaction ID, if any */
	queue queue;           /* Prev/next database, used by the registry */
	int read_lock;         /* Lock used by snapshots & checkpoints */
};

/**
 * Initialize a database object.
 *
 * The given @filename will be copied.
 * Return 0 on success.
 */
int db__init(struct db *db, struct config *config, const char *filename);

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

#endif /* DB_H_*/
