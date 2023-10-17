#ifndef DQLITE_REVAMP_H_
#define DQLITE_REVAMP_H_

#include "registry.h"

#include <semaphore.h>

struct db_context
{
	sem_t sem;
	struct registry registry;
};

int dbContextInit(struct db_context *ctx, struct config *config);
void dbContextClose(struct db_context *ctx);

#endif
