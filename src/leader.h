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

struct exec;
typedef void (*exec_cb)(struct exec *req, int status);

struct exec
{
	void *data;
	struct leader *leader;
	sqlite3_stmt *stmt;
	queue queue;
};

int leader__init(struct leader *l, struct db *db);

void leader__close(struct leader *l);

int leader__exec(struct leader *l,
		 struct exec *req,
		 sqlite3_stmt *stmt,
		 exec_cb cb);

#endif /* LEADER_H_*/
