#ifndef DQLITE_REVAMP_H_
#define DQLITE_REVAMP_H_

#include "lib/queue.h"
#include "registry.h"
#include "tuple.h"

#include <pthread.h>
#include <stdbool.h>

struct db_context
{
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	struct registry registry;
	queue exec_sql_reqs;
	bool shutdown;
};

struct exec_sql_req
{
	char *db_name;
	char *sql;
	struct value *params;
	queue queue;
};

int dbContextInit(struct db_context *ctx, struct config *config);

int postExecSqlReq(struct db_context *ctx,
		struct exec_sql_req req,
		void *data,
		void (*cb)(struct exec_sql_req, int, void *));

void dbContextClose(struct db_context *ctx);

void *dbTask(void *arg);

#endif
