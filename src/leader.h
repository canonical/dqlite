#ifndef LEADER_H_
#define LEADER_H_

#include <stdbool.h>

#include <libco.h>
#include <sqlite3.h>

#include "./lib/queue.h"

#include "db.h"

struct exec;
struct leader
{
	struct db *db;
	cothread_t main;
	cothread_t loop;
	sqlite3 *conn;
	struct exec *exec; /* Exec request currently in progress, if any */
	queue queue;
};

typedef void (*exec_cb)(struct exec *req, int status);

struct exec
{
	void *data;
	struct leader *leader;
	sqlite3_stmt *stmt;
	bool done;
	int status;
	queue queue;
	exec_cb cb;
};

int leader__init(struct leader *l, struct db *db);

void leader__close(struct leader *l);

int leader__exec(struct leader *l,
		 struct exec *req,
		 sqlite3_stmt *stmt,
		 exec_cb cb);

#endif /* LEADER_H_*/
