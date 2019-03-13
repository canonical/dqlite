#ifndef DB_H_
#define DB_H_

#include "./lib/queue.h"

struct db
{
	const char *filename;
	queue queue;
};

void db__init(struct db *db, const char *filename);

#endif /* DB_H_*/
