#ifndef LEADER_H_
#define LEADER_H_

#include <sqlite3.h>

#include "./lib/queue.h"

#include "db.h"

struct leader
{
	sqlite3 *conn;
	struct db *db;
	queue queue;
};

int leader__init(struct leader *l, struct db *db);

void leader__close(struct leader *l);

#endif /* LEADER_H_*/
