#include "revamp.h"

#include "bind.h"
#include "lib/queue.h"

struct database
{
	sqlite3 *conn;
	queue queue;
};

int dbContextInit(struct db_context *ctx)
{
	int rv;
	rv = pthread_mutex_init(&ctx->mutex, NULL);
	assert(rv == 0);
	rv = pthread_cond_init(&ctx->cond, NULL);
	assert(rv == 0);
	QUEUE__INIT(&ctx->exec_sql_reqs);
	QUEUE__INIT(&ctx->dbs);
	ctx->shutdown = false;
	return 0;
}

int postExecSqlReq(struct db_context *ctx,
		struct exec_sql_req *req)
{
	pthread_mutex_lock(&ctx->mutex);
	QUEUE__PUSH(&ctx->exec_sql_reqs, &req->queue);
	pthread_cond_signal(&ctx->cond);
	pthread_mutex_unlock(&ctx->mutex);
	return 0;
}

void dbContextClose(struct db_context *ctx)
{
	pthread_cond_destroy(&ctx->cond);
	pthread_mutex_destroy(&ctx->mutex);
}

static sqlite3 *getDatabase(queue *dbs, const char *name)
{
	queue *q;
	struct database *d;
	int rv;

	QUEUE__FOREACH(q, dbs) {
		d = QUEUE__DATA(q, struct database, queue);
		if (strcmp(sqlite3_db_name(d->conn, 0), name) == 0) {
			return d->conn;
		}
	}

	d = sqlite3_malloc(sizeof *d);
	if (d == NULL) {
		return NULL;
	}
	rv = sqlite3_open(name, &d->conn);
	if (rv != 0) {
		return NULL;
	}
	QUEUE__PUSH(dbs, &d->queue);
	return d->conn;
}

static void processRequests(queue *exec_sql_reqs, queue *dbs)
{
	queue *q;
	struct exec_sql_req *req;
	sqlite3 *db;
	sqlite3_stmt *stmt;
	int rv;

	QUEUE__FOREACH(q, exec_sql_reqs) {
		req = QUEUE__DATA(q, struct exec_sql_req, queue);
		db = getDatabase(dbs, req->db_name);
		rv = sqlite3_prepare_v2(db, req->sql, -1, &stmt, NULL);
		if (rv != 0) {
		}
		rv = bindParams(stmt, req->params);
		if (rv != 0) {
		}
		do {
			rv = sqlite3_step(stmt);
		} while (rv == SQLITE_ROW);
		sqlite3_finalize(stmt);
		req->status = rv;
		uv_async_send(&req->base);
	}
}

void *dbTask(void *arg)
{
	struct db_context *ctx = arg;
	queue *exec_sql_reqs;
	int rv;

	rv = pthread_mutex_lock(&ctx->mutex);
	assert(rv == 0);
	for (;;) {
		pthread_cond_wait(&ctx->cond, &ctx->mutex);

		for (;;) {
			if (ctx->shutdown) {
				goto shutdown;
			}

			exec_sql_reqs = QUEUE__HEAD(&ctx->exec_sql_reqs);
			if (exec_sql_reqs == &ctx->exec_sql_reqs) {
				break;
			}
			QUEUE__REMOVE(&ctx->exec_sql_reqs);
			QUEUE__INIT(&ctx->exec_sql_reqs);
			pthread_mutex_unlock(&ctx->mutex);

			processRequests(exec_sql_reqs, &ctx->dbs);
			pthread_mutex_lock(&ctx->mutex);
		}
	}

shutdown:
	rv = pthread_mutex_unlock(&ctx->mutex);
	assert(rv == 0);
	return NULL;
}
