#ifndef LEADER_H_
#define LEADER_H_

#include <sqlite3.h>

#include "./lib/queue.h"

struct leader
{
	sqlite3 *conn;
	queue queue;
};

void leader__init(struct leader *l, sqlite3 *conn);

#endif /* LEADER_H_*/
