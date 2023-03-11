#include <raft.h>
#include "sqlite3.h"

#include "./lib/assert.h"
#include "./tuple.h"

#include "stmt.h"

/* The maximum number of columns we expect (for bindings or rows) is 255, which
 * can fit in one byte. */
#define STMT__MAX_COLUMNS (1 << 8) - 1

void stmt__init(struct stmt *s)
{
	s->stmt = NULL;
}

void stmt__close(struct stmt *s)
{
	sqlite3_finalize(s->stmt);
}

const char *stmt__hash(struct stmt *stmt)
{
	(void)stmt;
	return NULL;
}

REGISTRY_METHODS(stmt__registry, stmt);
