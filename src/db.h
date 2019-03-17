#ifndef DB_H_
#define DB_H_

#include "./lib/queue.h"

#include "options.h"
#include "tx.h"

struct leader;

struct db
{
	struct options *options;
	char *filename;
	sqlite3 *follower;
	struct tx *tx;
	queue leaders;
	queue queue;
};

void db__init(struct db *db, struct options *options, const char *filename);
void db__close(struct db *db);

int db__open_follower(struct db *db);

int db__create_tx(struct db *db, unsigned long long id, sqlite3 *conn);

void db__delete_tx(struct db *db);

#endif /* DB_H_*/
