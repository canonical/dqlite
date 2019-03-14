#ifndef FOLLOWER_H_
#define FOLLOWER_H_

#include <sqlite3.h>

#include "./lib/queue.h"

#include "db.h"

struct follower
{
	sqlite3 *conn;
};

int follower__init(struct follower *f, const char *vfs, const char *filename);
void follower__close(struct follower *f);

#endif /* FOLLOWER_H_*/
