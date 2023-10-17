#include "revamp.h"

int dbContextInit(struct db_context *ctx, struct config *config)
{
	int rv;
	rv = sem_init(&ctx->sem, 0, 0);
	if (rv != 0) {
		return DQLITE_ERROR;
	}
	registry__init(&ctx->registry, config);
	return 0;
}

void dbContextClose(struct db_context *ctx)
{
	registry__close(&ctx->registry);
	sem_destroy(&ctx->sem);
}
