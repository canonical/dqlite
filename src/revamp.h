#ifndef DQLITE_REVAMP_H
#define DQLITE_REVAMP_H

#include <semaphore.h>

struct db_context
{
	sem_t sem;
};

#endif
