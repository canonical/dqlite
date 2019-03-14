#ifndef DB_H_
#define DB_H_

#include "./lib/queue.h"

#include "options.h"

struct leader;
struct follower;

struct db
{
	struct options *options;
	const char *filename;
	struct follower *follower;
	queue leaders;
	queue queue;
};

void db__init(struct db *db, struct options *options, const char *filename);
void db__close(struct db *db);

int db__open_follower(struct db *db);

#endif /* DB_H_*/
