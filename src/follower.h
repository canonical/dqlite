#ifndef FOLLOWER_H_
#define FOLLOWER_H_

#include <sqlite3.h>

#include "./lib/queue.h"

struct follower
{
	sqlite3 *conn;
	queue queue;
};

void follower__init(struct follower *f, sqlite3 *conn);

/**
 * Return the filename of the database of the given follower connection.
 */
const char *follower__filename(struct follower *f);

#endif /* FOLLOWER_H_*/
