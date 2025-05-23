/**
 * State of a single database.
 */

#ifndef DB_H_
#define DB_H_

#include <stdint.h>
#include "lib/queue.h"

#include "config.h"

struct db
{
	struct config *config;        /* Dqlite configuration */
	struct sqlite3_vfs *vfs;      /* Underlying VFS */
	char *filename;               /* Database filename */
	char *path;                   /* Used for on-disk db */
	uint32_t cookie;              /* Used to bind to the pool's thread */
	int leaders;                  /* Open leader connections */
	struct leader *active_leader; /* Current leader writing to the database */
	queue pending_queue;          /* Queue of pending execs, used by leader */
	queue queue;                  /* Prev/next database, used by the registry */
	int read_lock;                /* Lock used by snapshots & checkpoints */
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
 * Open a connection to the database.
 */
int db__open(struct db *db, sqlite3 **conn);


#endif /* DB_H_*/
