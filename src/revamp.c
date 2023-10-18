#include "revamp.h"

int dbContextInit(struct db_context *ctx, struct config *config)
{
	int rv;
	rv = pthread_mutex_init(&ctx->mutex, NULL);
	assert(rv == 0);
	rv = pthread_cond_init(&ctx->cond, NULL);
	assert(rv == 0);
	registry__init(&ctx->registry, config);
	ctx->shutdown = false;
	return 0;
}

int postExecSqlReq(struct db_context *ctx,
		struct exec_sql_req req,
		void *data,
		void (*cb)(struct exec_sql_req, int, void *))
{
	(void)ctx;
	(void)req;
	(void)data;
	(void)cb;
	return 0;
}

void dbContextClose(struct db_context *ctx)
{
	registry__close(&ctx->registry);
	pthread_cond_destroy(&ctx->cond);
	pthread_mutex_destroy(&ctx->mutex);
}

void *dbTask(void *arg)
{
	struct db_context *ctx = arg;
	int rv;

	rv = pthread_mutex_lock(&ctx->mutex);
	assert(rv == 0);
	for (;;) {
		rv = pthread_cond_wait(&ctx->cond, &ctx->mutex);
		assert(rv == 0);
		if (ctx->shutdown) {
			break;
		}
	}
	return NULL;
}
