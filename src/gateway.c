#include "gateway.h"

#include "bind.h"
#include "leader.h"
#include "lib/threadpool.h"
#include "protocol.h"
#include "query.h"
#include "raft.h"
#include "request.h"
#include "response.h"
#include "tracing.h"
#include "translate.h"
#include "tuple.h"
#include "vfs.h"

#if defined(__has_attribute) && __has_attribute (musttail)
# define TAIL __attribute__ ((musttail))
#else
# define TAIL
#endif

/**
 * Silly in-memory sqlite3 connection used to check if statements contain only non-sql
 * code like comments or spaces.
 */
static sqlite3 *check_connn;

__attribute__((constructor)) static void init(void) {
	int rc = sqlite3_open_v2(":memory:", &check_connn, SQLITE_OPEN_READONLY | SQLITE_OPEN_MEMORY, NULL);
	assert(rc == 0);
	assert(check_connn != NULL);
}

__attribute__((destructor)) static void fini(void) {
	sqlite3_close(check_connn);
}

void gateway__init(struct gateway *g,
		   struct config *config,
		   struct registry *registry,
		   struct raft *raft)
{
	tracef("gateway init");
	memset(g, 0, sizeof *g);
	g->config = config;
	g->registry = registry;
	g->raft = raft;
	g->leader = NULL;
	g->req = NULL;
	stmt__registry_init(&g->stmts);
	g->protocol = DQLITE_PROTOCOL_VERSION;
	g->client_id = 0;
}

/* FIXME: This function becomes unsound when using the new thread pool, since
 * the request callbacks will race with operations running in the pool. */
void gateway__leader_close(struct gateway *g, int reason)
{
	if (g == NULL || g->leader == NULL) {
		tracef("gateway:%p or gateway->leader are NULL", g);
		return;
	}

	(void)reason;
	// raft_timer_stop(g->raft, &g->exec.timer);
	// TODO: finish inflight exec
	// TODO: apply with RAFT_SHUTDOWN is buggy
	// TODO: this must only be "exec_abort" or even be 
	//       in leader_close as it has internal detail to exec
	// if (g->req != NULL) {
	// 	if (g->leader->inflight != NULL) {
	// 		tracef("finish inflight apply request");
	// 		struct raft_apply *req = &g->leader->inflight->req;
	// 		req->cb(req, reason, NULL);
	// 		assert(g->req == NULL);
	// 	} else if (g->barrier.cb != NULL) {
	// 		tracef("finish inflight barrier");
	// 		/* This is not a typo, g->barrier.req.cb is a wrapper
	// 		 * around g->barrier.cb and will set g->barrier.cb to
	// 		 * NULL when called. */
	// 		struct raft_barrier *b = &g->barrier.req;
	// 		b->cb(b, reason);
	// 		assert(g->barrier.cb == NULL);
	// 	} else if (g->leader->exec != NULL &&
	// 		   g->leader->exec->barrier.cb != NULL) {
	// 		tracef("finish inflight exec barrier");
	// 		struct raft_barrier *b = &g->leader->exec->barrier.req;
	// 		b->cb(b, reason);
	// 		assert(g->leader->exec == NULL);
	// 	} else if (g->req->type == DQLITE_REQUEST_QUERY_SQL) {
	// 		/* Finalize the statement that was in the process of
	// 		 * yielding rows. We only need to handle QUERY_SQL
	// 		 * because for QUERY and EXEC the statement is finalized
	// 		 * by the call to stmt__registry_close, below (and for
	// 		 * EXEC_SQL the lifetimes of the statements are managed
	// 		 * by leader__exec and the associated callback).
	// 		 *
	// 		 * It's okay if g->req->stmt is NULL since
	// 		 * sqlite3_finalize(NULL) is documented to be a no-op.
	// 		 */
	// 		sqlite3_finalize(g->req->stmt);
	// 		g->req = NULL;
	// 	} else if (g->req->type == DQLITE_REQUEST_QUERY) {
	// 		/* In case the statement is a prepared one, it
	// 		 * will be finalized by the stmt__registry_close
	// 		 * call below. Nevertheless, we must signal that
	// 		 * the request is not in place anymore so that any
	// 		 * callback which is already in the queue will not
	// 		 * attempt to execute a finalized statement.
	// 		 */
	// 		g->req = NULL;
	// 	}
	// }
	stmt__registry_close(&g->stmts);
	leader__close(g->leader);
	sqlite3_free(g->leader);
	g->leader = NULL;
}

void gateway__close(struct gateway *g)
{
	// TODO fix this race condition with exec_work_cb
	tracef("gateway close");
	if (g->leader == NULL) {
		stmt__registry_close(&g->stmts);
		return;
	}

	gateway__leader_close(g, RAFT_SHUTDOWN);
}

/* Declare a request struct and a response struct of the appropriate types and
 * decode the request. This is used in the common case where only one schema
 * version is extant. */
#define START_V0(REQ, RES, ...)                                       \
	struct request_##REQ request = { 0 };                         \
	struct response_##RES response = { 0 };                       \
	{                                                             \
		int rv_;                                              \
		if (req->schema != 0) {                               \
			tracef("bad schema version %d", req->schema); \
			failure(req, DQLITE_PARSE,                    \
				"unrecognized schema version");       \
			return 0;                                     \
		}                                                     \
		rv_ = request_##REQ##__decode(cursor, &request);      \
		if (rv_ != 0) {                                       \
			return rv_;                                   \
		}                                                     \
	}

#define CHECK_LEADER(REQ)                                            \
	if (raft_state(g->raft) != RAFT_LEADER) {                    \
		failure(REQ, SQLITE_IOERR_NOT_LEADER, "not leader"); \
		return 0;                                            \
	}

#define SUCCESS(LOWER, UPPER, RESP, SCHEMA)                                    \
	{                                                                      \
		size_t _n = response_##LOWER##__sizeof(&RESP);                 \
		char *_cursor;                                                 \
		assert(_n % 8 == 0);                                           \
		_cursor = buffer__advance(req->buffer, _n);                    \
		/* Since responses are small and the buffer it's at least 4096 \
		 * bytes, this can't fail. */                                  \
		assert(_cursor != NULL);                                       \
		response_##LOWER##__encode(&RESP, &_cursor);                   \
		req->cb(req, 0, DQLITE_RESPONSE_##UPPER, SCHEMA);              \
	}

/* Encode the given success response and invoke the request callback,
 * using schema version 0. */
#define SUCCESS_V0(LOWER, UPPER) SUCCESS(LOWER, UPPER, response, 0)

/* Lookup the database with the given ID.
 *
 * TODO: support more than one database per connection? */
#define LOOKUP_DB(ID)                                                \
	if (ID != 0 || g->leader == NULL) {                          \
		failure(req, SQLITE_NOTFOUND, "no database opened"); \
		return 0;                                            \
	}

/* Lookup the statement with the given ID. */
#define LOOKUP_STMT(ID)                                    \
	stmt = stmt__registry_get(&g->stmts, ID);          \
	if (stmt == NULL) {                                \
		failure(req, SQLITE_NOTFOUND,              \
			"no statement with the given id"); \
		return 0;                                  \
	}

#define FAIL_IF_CHECKPOINTING                                                  \
	{                                                                      \
		struct sqlite3_file *_file;                                    \
		int _rv;                                                       \
		_rv = sqlite3_file_control(g->leader->conn, "main",            \
					   SQLITE_FCNTL_FILE_POINTER, &_file); \
		assert(_rv == SQLITE_OK); /* Should never fail */              \
                                                                               \
		_rv = _file->pMethods->xShmLock(                               \
		    _file, 1 /* checkpoint lock */, 1,                         \
		    SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE);                   \
		if (_rv != 0) {                                                \
			assert(_rv == SQLITE_BUSY);                            \
			failure(req, SQLITE_BUSY, "checkpoint in progress");   \
			return 0;                                              \
		}                                                              \
		_file->pMethods->xShmLock(                                     \
		    _file, 1 /* checkpoint lock */, 1,                         \
		    SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE);                 \
	}

/* Encode fa failure response and invoke the request callback */
static void failure(struct handle *req, int code, const char *message)
{
	size_t n;
	char *cursor;
	struct response_failure failure = {
		.code = (uint64_t)code,
		.message = message,
	};
	n = response_failure__sizeof(&failure);
	assert(n % 8 == 0);
	cursor = buffer__advance(req->buffer, n);
	/* The buffer has at least 4096 bytes, and error messages are shorter
	 * than that. So this can't fail. */
	assert(cursor != NULL);
	response_failure__encode(&failure, &cursor);
	req->cb(req, code, DQLITE_RESPONSE_FAILURE, 0);
}

static int handle_leader_legacy(struct gateway *g, struct handle *req)
{
	tracef("handle leader legacy");
	struct cursor *cursor = &req->cursor;
	START_V0(leader, server_legacy);
	raft_id id;
	raft_leader(g->raft, &id, &response.address);
	if (response.address == NULL) {
		response.address = "";
	}
	SUCCESS_V0(server_legacy, SERVER_LEGACY);
	return 0;
}

static int handle_leader(struct gateway *g, struct handle *req)
{
	tracef("handle leader");
	struct cursor *cursor = &req->cursor;
	raft_id id = 0;
	const char *address = NULL;
	unsigned i;
	if (g->protocol == DQLITE_PROTOCOL_VERSION_LEGACY) {
		return handle_leader_legacy(g, req);
	}
	START_V0(leader, server);

	/* Only voters might now who the leader is. */
	for (i = 0; i < g->raft->configuration.n; i++) {
		struct raft_server *server = &g->raft->configuration.servers[i];
		if (server->id == g->raft->id && server->role == RAFT_VOTER) {
			tracef("handle leader - dispatch to %llu", server->id);
			raft_leader(g->raft, &id, &address);
			break;
		}
	}

	response.id = id;
	response.address = address;
	if (response.address == NULL) {
		response.address = "";
	}
	SUCCESS_V0(server, SERVER);
	return 0;
}

static int handle_client(struct gateway *g, struct handle *req)
{
	tracef("handle client");
	struct cursor *cursor = &req->cursor;
	START_V0(client, welcome);
	g->client_id = request.id;
	response.heartbeat_timeout = g->config->heartbeat_timeout;
	SUCCESS_V0(welcome, WELCOME);
	return 0;
}

static int handle_open(struct gateway *g, struct handle *req)
{
	tracef("handle open");
	struct cursor *cursor = &req->cursor;
	struct db *db;
	int rc;
	START_V0(open, db);
	if (g->leader != NULL) {
		tracef("already open");
		failure(req, SQLITE_BUSY,
			"a database for this connection is already open");
		return 0;
	}
	rc = registry__db_get(g->registry, request.filename, &db);
	if (rc != 0) {
		tracef("registry db get failed %d", rc);
		return rc;
	}
	g->leader = sqlite3_malloc(sizeof *g->leader);
	if (g->leader == NULL) {
		tracef("malloc failed");
		return DQLITE_NOMEM;
	}
	rc = leader__init(g->leader, db, g->raft);
	if (rc != 0) {
		tracef("leader init failed %d", rc);
		sqlite3_free(g->leader);
		g->leader = NULL;
		return rc;
	}
	response.id = 0;
	SUCCESS_V0(db, DB);
	return 0;
}

static void handle_prepare_cb(struct exec *exec)
{
	PRE(exec != NULL);
	struct gateway *g = exec->data;
	PRE(g != NULL && g->req != NULL);
	struct handle *req = g->req;
	int status = exec->status;
	sqlite3_stmt *stmt = exec->stmt;
	const char *sql = exec->sql;
	const char *tail = exec->tail;
	raft_free(exec);

	if (status != 0) {
		failure(req, status, sqlite3_errmsg(g->leader->conn));
	} else if (stmt == NULL) {
		/* FIXME Should we use a code other than 0 here? */
		failure(req, 0, "empty statement");
	} else {
		struct response_stmt response_v0 = { 0 };
		struct response_stmt_with_offset response_v1 = { 0 };
		struct stmt *registry_stmt;
		int rc;

		if (req->schema == DQLITE_PREPARE_STMT_SCHEMA_V0) {
			sqlite3_stmt *tail_stmt;
			rc = sqlite3_prepare_v2(check_connn, tail, -1, &tail_stmt, NULL);
			if (rc != 0 || tail_stmt != NULL) {
				sqlite3_finalize(tail_stmt);
				sqlite3_finalize(stmt);
				failure(req, SQLITE_ERROR, "nonempty statement tail");
				return;
			}
		}

		rc = stmt__registry_add(&g->stmts, &registry_stmt);
		if (rc != 0) {
			sqlite3_finalize(stmt);
			failure(req, SQLITE_NOMEM, "stmt registry add failed");
			return;
		}
		registry_stmt->stmt = stmt;

		switch (req->schema) {
			case DQLITE_PREPARE_STMT_SCHEMA_V0:
				response_v0.db_id = (uint32_t)req->db_id;
				response_v0.id = (uint32_t)registry_stmt->id;
				response_v0.params =
					(uint64_t)sqlite3_bind_parameter_count(registry_stmt->stmt);
				SUCCESS(stmt, STMT, response_v0,
					DQLITE_PREPARE_STMT_SCHEMA_V0);
				break;
			case DQLITE_PREPARE_STMT_SCHEMA_V1:
				response_v1.db_id = (uint32_t)req->db_id;
				response_v1.id = (uint32_t)registry_stmt->id;
				response_v1.params =
					(uint64_t)sqlite3_bind_parameter_count(registry_stmt->stmt);
				response_v1.offset = (uint64_t)(tail - sql);
				SUCCESS(stmt_with_offset, STMT_WITH_OFFSET, response_v1,
					DQLITE_PREPARE_STMT_SCHEMA_V1);
				break;
			default:
				assert(0);
		}
	}
}

static int handle_prepare(struct gateway *g, struct handle *req)
{
	tracef("handle prepare");
	struct cursor *cursor = &req->cursor;
	struct request_prepare request = { 0 };
	int rc;

	if (req->schema != DQLITE_PREPARE_STMT_SCHEMA_V0 &&
	    req->schema != DQLITE_PREPARE_STMT_SCHEMA_V1) {
		failure(req, SQLITE_ERROR, "unrecognized schema version");
		return 0;
	}
	rc = request_prepare__decode(cursor, &request);
	if (rc != 0) {
		return rc;
	}

	CHECK_LEADER(req);
	LOOKUP_DB(request.db_id);

	/* This cast is safe as long as the TODO in LOOKUP_DB is not
	 * implemented. */
	req->db_id = (size_t)request.db_id;

	struct exec *exec = raft_malloc(sizeof *exec);
	if (exec == NULL) {
		return DQLITE_NOMEM;
	}
	*exec = (struct exec) {
		.data = g,
		.sql = request.sql,
	};
	g->req = req;
	leader_exec(g->leader, exec, NULL, handle_prepare_cb);
	return 0;
}

static int handle_finalize(struct gateway *g, struct handle *req)
{
	tracef("handle finalize");
	struct cursor *cursor = &req->cursor;
	struct stmt *stmt;
	int rv;
	START_V0(finalize, empty);
	LOOKUP_DB(request.db_id);
	LOOKUP_STMT(request.stmt_id);
	rv = stmt__registry_del(&g->stmts, stmt);
	if (rv != 0) {
		tracef("handle finalize registry del failed %d", rv);
		failure(req, rv, "finalize statement");
		return 0;
	}
	SUCCESS_V0(empty, EMPTY);
	return 0;
}

/* Fill a result response with the last inserted ID and number of rows
 * affected. */
static void fill_result(struct gateway *g, struct response_result *response)
{
	assert(g->leader != NULL);
	response->last_insert_id =
	    (uint64_t)sqlite3_last_insert_rowid(g->leader->conn);
	/* FIXME eventually we should consider using sqlite3_changes64 here */
	response->rows_affected = (uint64_t)sqlite3_changes(g->leader->conn);
}

static const char *error_message(sqlite3 *db, int rc)
{
	switch (rc) {
		case SQLITE_IOERR_LEADERSHIP_LOST:
			return "disk I/O error";
		case SQLITE_IOERR_WRITE:
			return "disk I/O error";
		case SQLITE_ABORT:
			return "abort";
		case SQLITE_ROW:
			return "rows yielded when none expected for EXEC "
			       "request";
	}

	if (sqlite3_errcode(db) == SQLITE_OK) {
		return sqlite3_errstr(rc);
	}
	return sqlite3_errmsg(db);
}

static void handle_exec_work_cb(struct exec *exec)
{
	PRE(exec->stmt != NULL);
	struct gateway *g = exec->data;
	struct handle *req = g->req;
	struct cursor *cursor = &req->cursor;

	int rv = 0;
	// FIXME: bind_params should be incremental and tuple_decoder should
	// be part of the request.
	if (req->exec_count == 0) {
		rv = bind__params(exec->stmt, cursor, 
			req->schema == DQLITE_REQUEST_PARAMS_SCHEMA_V0 
				? TUPLE__PARAMS 
				: TUPLE__PARAMS32);
	}
	req->exec_count++;
	if (rv != 0) {
		tracef("handle exec bind failed %d", rv);
		leader_exec_result(exec, rv); // "bind parameters" how to get this error message?
	} else {
		rv = sqlite3_step(exec->stmt);
		sqlite3_reset(exec->stmt);
		if (rv == SQLITE_DONE) {
			leader_exec_result(exec, 0);
		} else if (rv == SQLITE_ROW) {
			/* The exec request cannot handle a query which returns rows. */
			leader_exec_result(exec, SQLITE_ERROR);
		} else {
			leader_exec_result(exec, rv);
		}
	}

	TAIL return leader_exec_resume(exec);
}

static void handle_exec_done_cb(struct exec *exec)
{
	struct gateway *g = exec->data;
	PRE(g->leader != NULL && g->req != NULL);
	struct handle *req = g->req;
	struct response_result response;

	g->req = NULL;

	if (exec->status == 0) {
		fill_result(g, &response);
		SUCCESS_V0(result, RESULT);
	} else {
		failure(req, exec->status, error_message(g->leader->conn, exec->status));
	}
	raft_free(exec);
}

static int handle_exec(struct gateway *g, struct handle *req)
{
	tracef("handle exec schema:%" PRIu8, req->schema);
	struct cursor *cursor = &req->cursor;
	struct stmt *stmt;
	struct request_exec request = { 0 };
	int rv;

	if (req->schema != DQLITE_REQUEST_PARAMS_SCHEMA_V0 &&
			req->schema != DQLITE_REQUEST_PARAMS_SCHEMA_V1) {
		tracef("bad schema version %d", req->schema);
		failure(req, DQLITE_PARSE,
			"unrecognized schema version");
		return 0;
	}

	/* The v0 and v1 schemas only differ in the layout of the tuple,
	 * so we can use the same decode function for both. */
	rv = request_exec__decode(cursor, &request);
	if (rv != 0) {
		return rv;
	}

	CHECK_LEADER(req);
	LOOKUP_DB(request.db_id);
	LOOKUP_STMT(request.stmt_id);
	FAIL_IF_CHECKPOINTING;
	struct exec *exec = raft_malloc(sizeof *exec);
	if (exec == NULL) {
		return DQLITE_NOMEM;
	}
	*exec = (struct exec){
		.data = g,
		.stmt = stmt->stmt,
	};
	g->req = req;
	leader_exec(g->leader, exec, handle_exec_work_cb, handle_exec_done_cb);
	return 0;
}

static void handle_exec_sql_done_cb(struct exec *exec) {
	struct gateway *g   = exec->data;
	PRE(g->leader != NULL && g->req != NULL);
	struct handle  *req = g->req;
	struct response_result response = {};

	sqlite3_finalize(exec->stmt);

	if (exec->status != 0) {
		g->req = NULL;
		return failure(g->req, exec->status, error_message(g->leader->conn, exec->status));
	}

	if (exec->tail != NULL) {
		exec->stmt = NULL;
		exec->sql = exec->tail;
		exec->tail = NULL;

		// FIXME leader_exec *might* become tail call if I ask users to explicitly
		// set callbacks. Is that worth it? I need to check which calls stay on the stack 
		// is this loop as it is the only problem so far.
		return leader_exec(g->leader, exec, handle_exec_work_cb, handle_exec_sql_done_cb);
	}

	g->req = NULL;
	if (req->exec_count > 0) {
		fill_result(g, &response);
	}
	SUCCESS_V0(result, RESULT);
}

static int handle_exec_sql(struct gateway *g, struct handle *req)
{
	tracef("handle exec sql schema:%" PRIu8, req->schema);
	struct cursor *cursor = &req->cursor;
	struct request_exec_sql request = { 0 };
	int rc;

	if (req->schema != DQLITE_REQUEST_PARAMS_SCHEMA_V0 &&
			req->schema != DQLITE_REQUEST_PARAMS_SCHEMA_V1) {
		tracef("bad schema version %d", req->schema);
		failure(req, DQLITE_PARSE, "unrecognized schema version");
		return 0;
	}
	/* The only difference in layout between the v0 and v1 requests is in
	 * the tuple, which isn't parsed until bind__params later on. */
	rc = request_exec_sql__decode(cursor, &request);
	if (rc != 0) {
		return rc;
	}

	CHECK_LEADER(req);
	LOOKUP_DB(request.db_id);
	FAIL_IF_CHECKPOINTING;
	PRE(g->req == NULL);
	req->exec_count = 0;
	g->req = req;
	struct exec *exec = raft_malloc(sizeof *exec);
	if (exec == NULL) {
		return DQLITE_NOMEM;
	}
	*exec = (struct exec) {
		.data   = g,
		.sql    = request.sql,
	};

	leader_exec(g->leader, exec, handle_exec_work_cb, handle_exec_sql_done_cb);
	return 0;
}

static void handle_query_work_cb(struct exec *exec)
{
	struct gateway *g = exec->data;
	PRE(g->req != NULL && g->leader != NULL);
	struct handle *req = g->req;

	int rc;
	if (req->exec_count == 0) {
		struct cursor *cursor = &req->cursor;
		rc = bind__params(exec->stmt, cursor, 
			req->schema == DQLITE_REQUEST_PARAMS_SCHEMA_V0 
				? TUPLE__PARAMS 
				: TUPLE__PARAMS32);
		if (rc != 0) {
			tracef("handle exec bind failed %d", rc);
			leader_exec_result(exec, rc); /* "bind parameters" how to get this error message? */

			TAIL return leader_exec_resume(exec);
		}
	}
	req->exec_count++;

	rc = query__batch(exec->stmt, req->buffer);
	if (rc == SQLITE_ROW) {
		/* If the statement is still running, do not resume the
		 * exec state machine, but send a response instead. The
		 * execution will resume as soon as the data has been
		 * sent. See gateway__resume. */
		struct response_rows response = {
			.eof = DQLITE_RESPONSE_ROWS_PART,
		};
		SUCCESS_V0(rows, ROWS);
		return;
	}
	
	if (rc == SQLITE_DONE) {
		leader_exec_result(exec, 0);
	} else {
		sqlite3_reset(exec->stmt);
		leader_exec_result(exec, rc);
	}

	TAIL return leader_exec_resume(exec);
}


static void handle_query_done_cb(struct exec *exec) {
	struct gateway *g   = exec->data;
	PRE(g->leader != NULL && g->req != NULL);
	struct handle *req = g->req;
	g->req = NULL;
	if (exec->status == 0) {
		struct response_rows response = {
			.eof = DQLITE_RESPONSE_ROWS_DONE,
		};
		SUCCESS_V0(rows, ROWS);
	} else {
		failure(req, exec->status, error_message(g->leader->conn, exec->status));
	}
	raft_free(exec);
}

static int handle_query(struct gateway *g, struct handle *req)
{
	tracef("handle query schema:%" PRIu8, req->schema);
	struct cursor *cursor = &req->cursor;
	struct stmt *stmt;
	struct request_query request = { 0 };
	int rv;

	if (req->schema != DQLITE_REQUEST_PARAMS_SCHEMA_V0 &&
			req->schema != DQLITE_REQUEST_PARAMS_SCHEMA_V1) {
		tracef("bad schema version %d", req->schema);
		failure(req, DQLITE_PARSE, "unrecognized schema version");
		return 0;
	}
	/* The only difference in layout between the v0 and v1 requests is in
	 * the tuple, which isn't parsed until bind__params later on. */
	rv = request_query__decode(cursor, &request);
	if (rv != 0) {
		return rv;
	}

	CHECK_LEADER(req);
	LOOKUP_DB(request.db_id);
	LOOKUP_STMT(request.stmt_id);
	FAIL_IF_CHECKPOINTING;
	g->req = req;

	struct exec *exec = raft_malloc(sizeof *exec);
	if (exec == NULL) {
		return DQLITE_ERROR;
	}
	*exec = (struct exec) {
		.data   = g,
		.stmt   = stmt->stmt,
	};

	leader_exec(g->leader,exec, handle_query_work_cb, handle_query_done_cb);
	return 0;
}

static void handle_query_sql_done_cb(struct exec *exec)
{
	struct gateway *g   = exec->data;
	PRE(g->leader != NULL && g->req != NULL);
	struct handle *req = g->req;
	g->req = NULL;

	sqlite3_finalize(exec->stmt);
	if (exec->status == 0) {
		if (req->exec_count == 0) {
			failure(req, 0, "empty statement");
		} else {
			struct response_rows response = {
				.eof = DQLITE_RESPONSE_ROWS_DONE,
			};
			SUCCESS_V0(rows, ROWS);
		}
	} else {
		failure(req, exec->status, error_message(g->leader->conn, exec->status));
	}
	raft_free(exec);
}

static int handle_query_sql(struct gateway *g, struct handle *req)
{
	tracef("handle query sql schema:%" PRIu8, req->schema);
	struct cursor *cursor = &req->cursor;
	struct request_query_sql request = { 0 };
	int rv;

	/* Fail early if the schema version isn't recognized. */
	if (req->schema != 0 && req->schema != 1) {
		tracef("bad schema version %d", req->schema);
		failure(req, DQLITE_PARSE, "unrecognized schema version");
		return 0;
	}
	/* Schema version only affect the tuple format, which is parsed later */
	rv = request_query_sql__decode(cursor, &request);
	if (rv != 0) {
		return rv;
	}

	CHECK_LEADER(req);
	LOOKUP_DB(request.db_id);
	FAIL_IF_CHECKPOINTING;
	g->req = req;

	struct exec *exec = raft_malloc(sizeof *exec);
	if (exec == NULL) {
		return DQLITE_ERROR;
	}
	*exec = (struct exec) {
		.data   = g,
		.sql    = request.sql,
	};

	leader_exec(g->leader, exec, handle_query_work_cb, handle_query_sql_done_cb);
	return 0;
}

/*
 * An interrupt can only be handled when a query is already yielding rows.
 */
static int handle_interrupt(struct gateway *g, struct handle *req)
{
	tracef("handle interrupt");
	g->req = NULL;
	struct cursor *cursor = &req->cursor;
	START_V0(interrupt, empty);
	sqlite3_finalize(req->stmt);
	req->stmt = NULL;
	SUCCESS_V0(empty, EMPTY);
	return 0;
}

struct change {
	struct gateway *gateway;
	struct raft_change req;
};

static void raftChangeCb(struct raft_change *change, int status)
{
	tracef("raft change cb status:%d", status);
	struct change *r = change->data;
	struct gateway *g = r->gateway;
	struct handle *req = g->req;
	struct response_empty response = { 0 };
	g->req = NULL;
	sqlite3_free(r);
	if (status != 0) {
		failure(req, translateRaftErrCode(status),
			raft_strerror(status));
	} else {
		SUCCESS_V0(empty, EMPTY);
	}
}

static int handle_add(struct gateway *g, struct handle *req)
{
	tracef("handle add");
	struct cursor *cursor = &req->cursor;
	struct change *r;
	int rv;
	START_V0(add, empty);
	(void)response;

	CHECK_LEADER(req);

	r = sqlite3_malloc(sizeof *r);
	if (r == NULL) {
		return DQLITE_NOMEM;
	}
	r->gateway = g;
	r->req.data = r;
	g->req = req;

	rv = raft_add(g->raft, &r->req, request.id, request.address,
		      raftChangeCb);
	if (rv != 0) {
		tracef("raft add failed %d", rv);
		g->req = NULL;
		sqlite3_free(r);
		failure(req, translateRaftErrCode(rv), raft_strerror(rv));
		return 0;
	}

	return 0;
}

static int handle_promote_or_assign(struct gateway *g, struct handle *req)
{
	tracef("handle assign");
	struct cursor *cursor = &req->cursor;
	struct change *r;
	uint64_t role = DQLITE_VOTER;
	int rv;
	START_V0(promote_or_assign, empty);
	(void)response;

	CHECK_LEADER(req);

	/* Detect if this is an assign role request, instead of the former
	 * promote request. */
	if (cursor->cap > 0) {
		rv = uint64__decode(cursor, &role);
		if (rv != 0) {
			tracef("handle assign promote rv %d", rv);
			return rv;
		}
	}

	r = sqlite3_malloc(sizeof *r);
	if (r == NULL) {
		tracef("malloc failed");
		return DQLITE_NOMEM;
	}
	r->gateway = g;
	r->req.data = r;
	g->req = req;

	rv = raft_assign(g->raft, &r->req, request.id,
			 translateDqliteRole((int)role), raftChangeCb);
	if (rv != 0) {
		tracef("raft_assign failed %d", rv);
		g->req = NULL;
		sqlite3_free(r);
		failure(req, translateRaftErrCode(rv), raft_strerror(rv));
		return 0;
	}

	return 0;
}

static int handle_remove(struct gateway *g, struct handle *req)
{
	tracef("handle remove");
	struct cursor *cursor = &req->cursor;
	struct change *r;
	int rv;
	START_V0(remove, empty);
	(void)response;

	CHECK_LEADER(req);

	r = sqlite3_malloc(sizeof *r);
	if (r == NULL) {
		tracef("malloc failed");
		return DQLITE_NOMEM;
	}
	r->gateway = g;
	r->req.data = r;
	g->req = req;

	rv = raft_remove(g->raft, &r->req, request.id, raftChangeCb);
	if (rv != 0) {
		tracef("raft_remote failed %d", rv);
		g->req = NULL;
		sqlite3_free(r);
		failure(req, translateRaftErrCode(rv), raft_strerror(rv));
		return 0;
	}

	return 0;
}

static int dumpFile(const char *filename,
		    uint8_t *data,
		    size_t n,
		    struct buffer *buffer)
{
	char *cur;
	uint64_t len = n;

	cur = buffer__advance(buffer, text__sizeof(&filename));
	if (cur == NULL) {
		goto oom;
	}
	text__encode(&filename, &cur);
	cur = buffer__advance(buffer, uint64__sizeof(&len));
	if (cur == NULL) {
		goto oom;
	}
	uint64__encode(&len, &cur);

	if (n == 0) {
		return 0;
	}

	assert(n % 8 == 0);
	assert(data != NULL);

	cur = buffer__advance(buffer, n);
	if (cur == NULL) {
		goto oom;
	}
	memcpy(cur, data, n);

	return 0;

oom:
	return DQLITE_NOMEM;
}

static int handle_dump(struct gateway *g, struct handle *req)
{
	tracef("handle dump");
	struct cursor *cursor = &req->cursor;
	bool err = true;
	sqlite3_vfs *vfs;
	char *cur;
	char filename[1024] = { 0 };
	void *data;
	size_t n;
	uint8_t *page;
	uint32_t database_size = 0;
	uint8_t *database;
	uint8_t *wal;
	size_t n_database;
	size_t n_wal;
	int rv;
	START_V0(dump, files);

	response.n = 2;
	cur = buffer__advance(req->buffer, response_files__sizeof(&response));
	assert(cur != NULL);
	response_files__encode(&response, &cur);

	vfs = sqlite3_vfs_find(g->config->name);
	rv = VfsSnapshot(vfs, request.filename, &data, &n);
	if (rv != 0) {
		tracef("dump failed");
		failure(req, rv, "failed to dump database");
		return 0;
	}

	if (data != NULL) {
		/* Extract the database size from the first page. */
		page = data;
		database_size += (uint32_t)(page[28] << 24);
		database_size += (uint32_t)(page[29] << 16);
		database_size += (uint32_t)(page[30] << 8);
		database_size += (uint32_t)(page[31]);

		n_database = database_size * g->config->page_size;
		n_wal = n - n_database;

		database = data;
		wal = database + n_database;
	} else {
		assert(n == 0);

		n_database = 0;
		n_wal = 0;

		database = NULL;
		wal = NULL;
	}

	rv = dumpFile(request.filename, database, n_database, req->buffer);
	if (rv != 0) {
		tracef("dump failed");
		failure(req, rv, "failed to dump database");
		goto out_free_data;
	}

	/* filename is zero inited and initially we allow only writing 1024 - 4
	 * - 1 bytes to it, so after strncpy filename will be zero-terminated
	 * and will not have overflowed. strcat adds the 4 byte suffix and
	 * also zero terminates the resulting string. */
	const char *wal_suffix = "-wal";
	strncpy(filename, request.filename,
		sizeof(filename) - strlen(wal_suffix) - 1);
	strcat(filename, wal_suffix);
	rv = dumpFile(filename, wal, n_wal, req->buffer);
	if (rv != 0) {
		tracef("wal dump failed");
		failure(req, rv, "failed to dump wal file");
		goto out_free_data;
	}

	err = false;

out_free_data:
	if (data != NULL) {
		raft_free(data);
	}

	if (!err) {
		req->cb(req, 0, DQLITE_RESPONSE_FILES, 0);
	}

	return 0;
}

static int encodeServer(struct gateway *g,
			unsigned i,
			struct buffer *buffer,
			int format)
{
	char *cur;
	uint64_t id;
	uint64_t role;
	text_t address;

	assert(format == DQLITE_REQUEST_CLUSTER_FORMAT_V0 ||
	       format == DQLITE_REQUEST_CLUSTER_FORMAT_V1);

	id = g->raft->configuration.servers[i].id;
	address = g->raft->configuration.servers[i].address;
	role =
	    (uint64_t)translateRaftRole(g->raft->configuration.servers[i].role);

	cur = buffer__advance(buffer, uint64__sizeof(&id));
	if (cur == NULL) {
		return DQLITE_NOMEM;
	}
	uint64__encode(&id, &cur);

	cur = buffer__advance(buffer, text__sizeof(&address));
	if (cur == NULL) {
		return DQLITE_NOMEM;
	}
	text__encode(&address, &cur);

	if (format == DQLITE_REQUEST_CLUSTER_FORMAT_V0) {
		return 0;
	}

	cur = buffer__advance(buffer, uint64__sizeof(&role));
	if (cur == NULL) {
		return DQLITE_NOMEM;
	}
	uint64__encode(&role, &cur);

	return 0;
}

static int handle_cluster(struct gateway *g, struct handle *req)
{
	tracef("handle cluster");
	struct cursor *cursor = &req->cursor;
	unsigned i;
	char *cur;
	int rv;
	START_V0(cluster, servers);

	if (request.format != DQLITE_REQUEST_CLUSTER_FORMAT_V0 &&
	    request.format != DQLITE_REQUEST_CLUSTER_FORMAT_V1) {
		tracef("bad cluster format");
		failure(req, DQLITE_PARSE, "unrecognized cluster format");
		return 0;
	}

	response.n = g->raft->configuration.n;
	cur = buffer__advance(req->buffer, response_servers__sizeof(&response));
	assert(cur != NULL);
	response_servers__encode(&response, &cur);

	for (i = 0; i < response.n; i++) {
		rv = encodeServer(g, i, req->buffer, (int)request.format);
		if (rv != 0) {
			tracef("encode failed");
			failure(req, rv, "failed to encode server");
			return 0;
		}
	}

	req->cb(req, 0, DQLITE_RESPONSE_SERVERS, 0);

	return 0;
}

void raftTransferCb(struct raft_transfer *r)
{
	struct gateway *g = r->data;
	struct handle *req = g->req;
	struct response_empty response = { 0 };
	g->req = NULL;
	sqlite3_free(r);
	if (g->raft->state == RAFT_LEADER) {
		tracef("transfer failed");
		failure(req, DQLITE_ERROR, "leadership transfer failed");
	} else {
		SUCCESS_V0(empty, EMPTY);
	}
}

static int handle_transfer(struct gateway *g, struct handle *req)
{
	tracef("handle transfer");
	struct cursor *cursor = &req->cursor;
	struct raft_transfer *r;
	int rv;
	START_V0(transfer, empty);
	(void)response;

	CHECK_LEADER(req);

	r = sqlite3_malloc(sizeof *r);
	if (r == NULL) {
		tracef("malloc failed");
		return DQLITE_NOMEM;
	}
	r->data = g;
	g->req = req;

	rv = raft_transfer(g->raft, r, request.id, raftTransferCb);
	if (rv != 0) {
		tracef("raft_transfer failed %d", rv);
		g->req = NULL;
		sqlite3_free(r);
		failure(req, translateRaftErrCode(rv), raft_strerror(rv));
		return 0;
	}

	return 0;
}

static int handle_describe(struct gateway *g, struct handle *req)
{
	tracef("handle describe");
	struct cursor *cursor = &req->cursor;
	START_V0(describe, metadata);
	if (request.format != DQLITE_REQUEST_DESCRIBE_FORMAT_V0) {
		tracef("bad format");
		failure(req, SQLITE_PROTOCOL, "bad format version");
	}
	response.failure_domain = g->config->failure_domain;
	response.weight = g->config->weight;
	SUCCESS_V0(metadata, METADATA);
	return 0;
}

static int handle_weight(struct gateway *g, struct handle *req)
{
	tracef("handle weight");
	struct cursor *cursor = &req->cursor;
	START_V0(weight, empty);
	g->config->weight = request.weight;
	SUCCESS_V0(empty, EMPTY);
	return 0;
}

int gateway__handle(struct gateway *g,
		    struct handle *req,
		    int type,
		    int schema,
		    struct buffer *buffer,
		    handle_cb cb)
{
	tracef("gateway handle");
	int rc = 0;
	sqlite3_stmt *stmt = NULL;  // used for DQLITE_REQUEST_INTERRUPT

	if (g->req == NULL) {
		goto handle;
	}

	/* Request in progress. TODO The current implementation doesn't allow
	 * reading a new request while a query is yielding rows, in that case
	 * gateway__resume in write_cb will indicate it has not finished
	 * returning results and a new request (in this case, the interrupt)
	 * will not be read. */
	if (g->req->type == DQLITE_REQUEST_QUERY &&
	    type == DQLITE_REQUEST_INTERRUPT) {
		goto handle;
	}
	if (g->req->type == DQLITE_REQUEST_QUERY_SQL &&
	    type == DQLITE_REQUEST_INTERRUPT) {
		stmt = g->req->stmt;
		goto handle;
	}

	/* Receiving a request when one is ongoing on the same connection
	 * is a hard error. The connection will be stopped due to the non-0
	 * return code in case asserts are off. */
	assert(false);
	return SQLITE_BUSY;

handle:
	req->type = type;
	req->schema = schema;
	req->cb = cb;
	req->buffer = buffer;
	req->db_id = 0;
	req->stmt = stmt;
	req->exec_count = 0;
	req->work = (pool_work_t){};

	switch (type) {
#define DISPATCH(LOWER, UPPER, _)            \
	case DQLITE_REQUEST_##UPPER:         \
		rc = handle_##LOWER(g, req); \
		break;
		REQUEST__TYPES(DISPATCH);
		default:
			tracef("unrecognized request type %d", type);
			failure(req, DQLITE_PARSE, "unrecognized request type");
			rc = 0;
	}

	return rc;
}

int gateway__resume(struct gateway *g, bool *finished)
{
	if (g->req == NULL || (g->req->type != DQLITE_REQUEST_QUERY &&
			       g->req->type != DQLITE_REQUEST_QUERY_SQL)) {
		tracef("gateway resume - finished");
		*finished = true;
		return 0;
	}
	tracef("gateway resume - not finished");
	*finished = false;

	g->req->work = (pool_work_t){};
	handle_query_work_cb(g->leader->exec);
	return 0;
}
