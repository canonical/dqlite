#ifndef DQLITE_REVAMP_H_
#define DQLITE_REVAMP_H_

#include "lib/queue.h"
#include "tuple.h"

#include <pthread.h>
#include <stdbool.h>

struct db_context
{
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	queue exec_sql_reqs;
	queue dbs;
	bool shutdown;
};

struct exec_sql_req
{
	struct uv_async_s base;
	char *db_name;
	char *sql;
	struct value *params;
	queue queue;
	int status;
};

int dbContextInit(struct db_context *ctx);

int postExecSqlReq(struct db_context *ctx,
		struct exec_sql_req *req);

void dbContextClose(struct db_context *ctx);

void *dbTask(void *arg);

#endif
