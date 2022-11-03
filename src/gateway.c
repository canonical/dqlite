#include "gateway.h"

#include "bind.h"
#include "protocol.h"
#include "query.h"
#include "request.h"
#include "response.h"
#include "tracing.h"
#include "translate.h"
#include "tuple.h"
#include "vfs.h"

void gateway__init(struct gateway *g,
		   struct config *config,
		   struct registry *registry,
		   struct raft *raft)
{
        tracef("gateway init");
	g->config = config;
	g->registry = registry;
	g->raft = raft;
	g->leader = NULL;
	g->req = NULL;
	g->stmt = NULL;
	g->stmt_finalize = false;
	g->exec.data = g;
	g->sql = NULL;
	stmt__registry_init(&g->stmts);
	g->barrier.data = g;
	g->barrier.cb = NULL;
	g->barrier.leader = NULL;
	g->protocol = DQLITE_PROTOCOL_VERSION;
}

void gateway__leader_close(struct gateway *g, int reason)
{
	if (g == NULL || g->leader == NULL) {
		tracef("gateway:%p or gateway->leader are NULL", g);
		return;
	}

	if (g->req != NULL) {
		if (g->leader->inflight != NULL) {
			tracef("finish inflight apply request");
			struct raft_apply *req = &g->leader->inflight->req;
			req->cb(req, reason, NULL);
			assert(g->req == NULL);
			assert(g->stmt == NULL);
		} else if (g->barrier.cb != NULL) {
			tracef("finish inflight barrier");
			/* This is not a typo, g->barrier.req.cb is a wrapper
			 * around g->barrier.cb and when called, will set g->barrier.cb to NULL.
			 * */
			struct raft_barrier *b = &g->barrier.req;
			b->cb(b, reason);
			assert(g->barrier.cb == NULL);
		} else if (g->leader->exec != NULL && g->leader->exec->barrier.cb != NULL) {
			tracef("finish inflight exec barrier");
			struct raft_barrier *b = &g->leader->exec->barrier.req;
			b->cb(b, reason);
			assert(g->leader->exec == NULL);
		} else if (g->stmt != NULL && g->req->type != DQLITE_REQUEST_QUERY
					   && g->req->type != DQLITE_REQUEST_EXEC) {
			/* Regular exec and query stmt's will be closed when the
			 * registry is closed below. */
			tracef("finalize exec_sql or query_sql type:%d", g->req->type);
			sqlite3_finalize(g->stmt);
			g->stmt = NULL;
			g->req = NULL;
		}
	}
	stmt__registry_close(&g->stmts);
	leader__close(g->leader);
	sqlite3_free(g->leader);
	g->leader = NULL;
}

void gateway__close(struct gateway *g)
{
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
#define START_V0(REQ, RES, ...)                                                    \
	struct request_##REQ request = {0};                                        \
	struct response_##RES response = {0};                                      \
	{                                                                          \
		int rv_;                                                           \
		if (req->schema != 0) {                                            \
			tracef("bad schema version %d", req->schema);              \
			failure(req, DQLITE_PARSE, "unrecognized schema version"); \
			return 0;                                                  \
		}                                                                  \
		rv_ = request_##REQ##__decode(cursor, &request);                   \
		if (rv_ != 0) {                                                    \
			return rv_;                                                \
		}                                                                  \
	}

#define CHECK_LEADER(REQ)                                            \
	if (raft_state(REQ->gateway->raft) != RAFT_LEADER) {         \
		failure(REQ, SQLITE_IOERR_NOT_LEADER, "not leader"); \
		return 0;                                            \
	}

/* Encode the given success response and invoke the request callback */
#define SUCCESS(LOWER, UPPER)                                                  \
	{                                                                      \
		size_t _n = response_##LOWER##__sizeof(&response);             \
		void *_cursor;                                                 \
		assert(_n % 8 == 0);                                           \
		_cursor = buffer__advance(req->buffer, _n);                    \
		/* Since responses are small and the buffer it's at least 4096 \
		 * bytes, this can't fail. */                                  \
		assert(_cursor != NULL);                                       \
		response_##LOWER##__encode(&response, &_cursor);               \
		req->cb(req, 0, DQLITE_RESPONSE_##UPPER);                      \
	}

/* Lookup the database with the given ID.
 *
 * TODO: support more than one database per connection? */
#define LOOKUP_DB(ID)                                                \
	if (ID != 0 || req->gateway->leader == NULL) {               \
		failure(req, SQLITE_NOTFOUND, "no database opened"); \
		return 0;                                            \
	}

/* Lookup the statement with the given ID. */
#define LOOKUP_STMT(ID)                                      \
	stmt = stmt__registry_get(&req->gateway->stmts, ID); \
	if (stmt == NULL) {                                  \
		failure(req, SQLITE_NOTFOUND,                \
			"no statement with the given id");   \
		return 0;                                    \
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
	struct response_failure failure;
	size_t n;
	void *cursor;
	failure.code = (uint64_t)code;
	failure.message = message;
	n = response_failure__sizeof(&failure);
	assert(n % 8 == 0);
	cursor = buffer__advance(req->buffer, n);
	/* The buffer has at least 4096 bytes, and error messages are shorter
	 * than that. So this can't fail. */
	assert(cursor != NULL);
	response_failure__encode(&failure, &cursor);
	req->cb(req, 0, DQLITE_RESPONSE_FAILURE);
}

static int handle_leader_legacy(struct handle *req)
{
        tracef("handle leader legacy");
	struct cursor *cursor = &req->cursor;
	START_V0(leader, server_legacy);
	raft_id id;
	raft_leader(req->gateway->raft, &id, &response.address);
	if (response.address == NULL) {
		response.address = "";
	}
	SUCCESS(server_legacy, SERVER_LEGACY);
	return 0;
}

static int handle_leader(struct handle *req)
{
        tracef("handle leader");
	struct cursor *cursor = &req->cursor;
	struct gateway *g = req->gateway;
	raft_id id = 0;
	const char *address = NULL;
	unsigned i;
	if (g->protocol == DQLITE_PROTOCOL_VERSION_LEGACY) {
		return handle_leader_legacy(req);
	}
	START_V0(leader, server);

	/* Only voters might now who the leader is. */
	for (i = 0; i < g->raft->configuration.n; i++) {
		struct raft_server *server = &g->raft->configuration.servers[i];
		if (server->id == g->raft->id && server->role == RAFT_VOTER) {
                        tracef("handle leader - dispatch to %llu", server->id);
			raft_leader(req->gateway->raft, &id, &address);
			break;
		}
	}

	response.id = id;
	response.address = address;
	if (response.address == NULL) {
		response.address = "";
	}
	SUCCESS(server, SERVER);
	return 0;
}

static int handle_client(struct handle *req)
{
        tracef("handle client");
	struct cursor *cursor = &req->cursor;
	START_V0(client, welcome);
	response.heartbeat_timeout = req->gateway->config->heartbeat_timeout;
	SUCCESS(welcome, WELCOME);
	return 0;
}

static int handle_open(struct handle *req)
{
        tracef("handle open");
	struct cursor *cursor = &req->cursor;
	struct gateway *g = req->gateway;
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
	SUCCESS(db, DB);
	return 0;
}

static void prepareBarrierCb(struct barrier *barrier, int status)
{
	tracef("prepare barrier cb status:%d", status);
	struct gateway *g = barrier->data;
	struct handle *req = g->req;
	struct response_stmt response = {0};
	const char *sql = g->sql;
	struct stmt *stmt;
	const char *tail;
	int rc;

	assert(req != NULL);
	stmt = stmt__registry_get(&g->stmts, req->stmt_id);
	assert(stmt != NULL);
	g->req = NULL;
	g->sql = NULL;
	if (status != 0) {
		stmt__registry_del(&g->stmts, stmt);
		failure(req, status, "barrier error");
		return;
	}

	rc = sqlite3_prepare_v2(g->leader->conn, sql, -1, &stmt->stmt,
				&tail);
	if (rc != SQLITE_OK) {
		tracef("prepare barrier cb sqlite prepare failed %d %s", rc, sqlite3_errmsg(g->leader->conn));
		stmt__registry_del(&g->stmts, stmt);
		failure(req, rc, sqlite3_errmsg(g->leader->conn));
		return;
	}

	if (stmt->stmt == NULL) {
		tracef("prepare barrier cb empty statement");
		stmt__registry_del(&g->stmts, stmt);
		/* FIXME Should we use a code other than 0 here? */
		failure(req, 0, "empty statement");
		return;
	}

	response.db_id = (uint32_t)req->db_id;
	response.id = (uint32_t)stmt->id;
	response.params = (uint64_t)sqlite3_bind_parameter_count(stmt->stmt);
	SUCCESS(stmt, STMT);
}

static int handle_prepare(struct handle *req)
{
	tracef("handle prepare");
	struct cursor *cursor = &req->cursor;
	struct gateway *g = req->gateway;
	struct stmt *stmt;
	int rc;
	START_V0(prepare, stmt);
	CHECK_LEADER(req);
	LOOKUP_DB(request.db_id);
	(void)response;
	rc = stmt__registry_add(&g->stmts, &stmt);
	if (rc != 0) {
                tracef("handle prepare registry add failed %d", rc);
		return rc;
	}
	assert(stmt != NULL);
	/* This cast is safe as long as the TODO in LOOKUP_DB is not
	 * implemented. */
	req->db_id = (size_t)request.db_id;
	req->stmt_id = stmt->id;
	g->req = req;
	g->sql = request.sql;
	rc = leader__barrier(g->leader, &g->barrier, prepareBarrierCb);
	if (rc != 0) {
		tracef("handle prepare barrier failed %d", rc);
		stmt__registry_del(&g->stmts, stmt);
		g->req = NULL;
		g->sql = NULL;
		return rc;
	}
	return 0;
}

/* Fill a result response with the last inserted ID and number of rows
 * affected. */
static void fill_result(struct gateway *g, struct response_result *response)
{
	assert(g->leader != NULL);
	response->last_insert_id =
	    (uint64_t)sqlite3_last_insert_rowid(g->leader->conn);
	response->rows_affected = (uint64_t)sqlite3_changes(g->leader->conn);
}

static const char *error_message(sqlite3 *db, int rc) {
	switch (rc) {
	case SQLITE_IOERR_LEADERSHIP_LOST:
		return "disk I/O error";
	case SQLITE_IOERR_WRITE:
		return "disk I/O error";
	case SQLITE_ABORT:
		return "abort";
	}

	return sqlite3_errmsg(db);
}

static void leader_exec_cb(struct exec *exec, int status)
{
	struct gateway *g = exec->data;
	struct handle *req = g->req;
	sqlite3_stmt *stmt = g->stmt;
	struct response_result response;

	g->req = NULL;
	g->stmt = NULL;

	if (status == SQLITE_DONE) {
		fill_result(g, &response);
		SUCCESS(result, RESULT);
	} else {
		assert(g->leader != NULL);
		failure(req, status, error_message(g->leader->conn, status));
		sqlite3_reset(stmt);
	}
}

static int handle_exec(struct handle *req)
{
        tracef("handle exec");
	struct cursor *cursor = &req->cursor;
	struct gateway *g = req->gateway;
	struct stmt *stmt;
	struct request_exec request = {0};
	int tuple_format;
	int rv;

	switch (req->schema) {
		case 0:
			tuple_format = TUPLE__PARAMS;
			break;
		case 1:
			tuple_format = TUPLE__PARAMS32;
			break;
		default:
			tracef("bad schema version %d", req->schema);
			failure(req, DQLITE_PARSE, "unrecognized schema version");
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
	rv = bind__params(stmt->stmt, cursor, tuple_format);
	if (rv != 0) {
                tracef("handle exec bind failed %d", rv);
		failure(req, rv, "bind parameters");
		return 0;
	}
	g->req = req;
	g->stmt = stmt->stmt;
	rv = leader__exec(g->leader, &g->exec, stmt->stmt, leader_exec_cb);
	if (rv != 0) {
		tracef("handle exec leader exec failed %d", rv);
		g->stmt = NULL;
		g->req = NULL;
		return rv;
	}
	return 0;
}

/* Step through the given statement and populate the response buffer of the
 * given request with a single batch of rows.
 *
 * A single batch of rows is typically about the size of a memory page. */
static void query_batch(sqlite3_stmt *stmt, struct handle *req)
{
	struct gateway *g = req->gateway;
	struct response_rows response;
	int rc;

	rc = query__batch(stmt, req->buffer);
	if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
		sqlite3_reset(stmt);
		assert(g->leader != NULL);
		failure(req, rc, sqlite3_errmsg(g->leader->conn));
		goto done;
	}

	if (rc == SQLITE_ROW) {
		response.eof = DQLITE_RESPONSE_ROWS_PART;
		g->req = req;
		g->stmt = stmt;
		SUCCESS(rows, ROWS);
		return;
	} else {
		response.eof = DQLITE_RESPONSE_ROWS_DONE;
		SUCCESS(rows, ROWS);
	}

done:
	if (g->stmt_finalize) {
		/* TODO: do we care about errors? */
		sqlite3_finalize(stmt);
		g->stmt_finalize = false;
	}
	g->stmt = NULL;
	g->req = NULL;
}

static void query_barrier_cb(struct barrier *barrier, int status)
{
	tracef("query barrier cb status:%d", status);
	struct gateway *g = barrier->data;
	struct handle *handle = g->req;
	sqlite3_stmt *stmt = g->stmt;

	assert(handle != NULL);
	assert(stmt != NULL);

	g->stmt = NULL;
	g->req = NULL;

	if (status != 0) {
		if (g->stmt_finalize) {
			sqlite3_finalize(stmt);
			g->stmt_finalize = false;
		}
		failure(handle, status, "barrier error");
		return;
	}

	query_batch(stmt, handle);
}

static int handle_query(struct handle *req)
{
        tracef("handle query");
	struct cursor *cursor = &req->cursor;
	struct gateway *g = req->gateway;
	struct stmt *stmt;
	struct request_query request = {0};
	int tuple_format;
	int rv;

	switch (req->schema) {
		case 0:
			tuple_format = TUPLE__PARAMS;
			break;
		case 1:
			tuple_format = TUPLE__PARAMS32;
			break;
		default:
			tracef("bad schema version %d", req->schema);
			failure(req, DQLITE_PARSE, "unrecognized schema version");
			return 0;
	}
	/* The only difference in layout between the v0 and v1 requests is in the tuple,
	 * which isn't parsed until bind__params later on. */
	rv = request_query__decode(cursor, &request);
	if (rv != 0) {
		return rv;
	}

	CHECK_LEADER(req);
	LOOKUP_DB(request.db_id);
	LOOKUP_STMT(request.stmt_id);
	FAIL_IF_CHECKPOINTING;
	rv = bind__params(stmt->stmt, cursor, tuple_format);
	if (rv != 0) {
                tracef("handle query bind failed %d", rv);
		failure(req, rv, sqlite3_errmsg(g->leader->conn));
		return 0;
	}
	g->req = req;
	g->stmt = stmt->stmt;
	rv = leader__barrier(g->leader, &g->barrier, query_barrier_cb);
	if (rv != 0) {
                tracef("handle query leader barrier failed %d", rv);
		g->req = NULL;
		g->stmt = NULL;
		return rv;
	}
	return 0;
}

static int handle_finalize(struct handle *req)
{
        tracef("handle finalize");
	struct cursor *cursor = &req->cursor;
	struct stmt *stmt;
	int rv;
	START_V0(finalize, empty);
	LOOKUP_DB(request.db_id);
	LOOKUP_STMT(request.stmt_id);
	rv = stmt__registry_del(&req->gateway->stmts, stmt);
	if (rv != 0) {
                tracef("handle finalize registry del failed %d", rv);
		failure(req, rv, "finalize statement");
		return 0;
	}
	SUCCESS(empty, EMPTY);
	return 0;
}

static void handle_exec_sql_next(struct handle *req, bool done);

static void handle_exec_sql_cb(struct exec *exec, int status)
{
        tracef("handle exec sql cb status %d", status);
	struct gateway *g = exec->data;
	struct handle *req = g->req;

	if (status == SQLITE_DONE) {
		handle_exec_sql_next(req, true);
	} else {
		assert(g->leader != NULL);
		failure(req, status, error_message(g->leader->conn, status));
		sqlite3_reset(g->stmt);
		sqlite3_finalize(g->stmt);
		g->req = NULL;
		g->stmt = NULL;
		g->sql = NULL;
	}
}

static void handle_exec_sql_next(struct handle *req, bool done)
{
        tracef("handle exec sql next");
	struct cursor *cursor = &req->cursor;
	struct gateway *g = req->gateway;
	struct response_result response = {0};
	const char *tail;
	int tuple_format;
	int rv;

	if (g->sql == NULL || strcmp(g->sql, "") == 0) {
		goto success;
	}

	if (g->stmt != NULL) {
		sqlite3_finalize(g->stmt);
		g->stmt = NULL;
	}

	/* g->stmt will be set to NULL by sqlite when an error occurs. */
	assert(g->leader != NULL);
	rv = sqlite3_prepare_v2(g->leader->conn, g->sql, -1, &g->stmt, &tail);
	if (rv != SQLITE_OK) {
		tracef("exec sql prepare failed %d", rv);
		failure(req, rv, sqlite3_errmsg(g->leader->conn));
		goto done;
	}

	if (g->stmt == NULL) {
		goto success;
	}

	/* TODO: what about bindings for multi-statement SQL text? */
	if (!done) {
		switch (req->schema) {
			case 0:
				tuple_format = TUPLE__PARAMS;
				break;
			case 1:
				tuple_format = TUPLE__PARAMS32;
				break;
			default:
				/* Should have been caught by handle_exec_sql */
				assert(0);
		}
		rv = bind__params(g->stmt, cursor, tuple_format);
		if (rv != SQLITE_OK) {
			failure(req, rv, sqlite3_errmsg(g->leader->conn));
			goto done_after_prepare;
		}
	}

	g->sql = tail;
	g->req = req;

	rv = leader__exec(g->leader, &g->exec, g->stmt, handle_exec_sql_cb);
	if (rv != SQLITE_OK) {
		failure(req, rv, sqlite3_errmsg(g->leader->conn));
		goto done_after_prepare;
	}

	return;

success:
        tracef("handle exec sql next success");
	if (g->stmt != NULL) {
		fill_result(g, &response);
	}
	SUCCESS(result, RESULT);

done_after_prepare:
	if (g->stmt != NULL) {
		sqlite3_finalize(g->stmt);
	}
done:
	g->req = NULL;
	g->stmt = NULL;
	g->sql = NULL;
}

static void execSqlBarrierCb(struct barrier *barrier, int status)
{
	tracef("exec sql barrier cb status:%d", status);
	struct gateway *g = barrier->data;
	struct handle *req = g->req;
	const char *sql = g->sql;

	assert(req != NULL);
	g->req = NULL;
	g->sql = NULL;
	if (status != 0) {
		failure(req, status, "barrier error");
		return;
	}

	assert(g->req == NULL);
	assert(g->sql == NULL);
	assert(g->stmt == NULL);
	req->gateway->sql = sql;
	handle_exec_sql_next(req, false);
}

static int handle_exec_sql(struct handle *req)
{
	tracef("handle exec sql");
	struct cursor *cursor = &req->cursor;
	struct gateway *g = req->gateway;
	struct request_exec_sql request = {0};
	int rc;

	/* Fail early if the schema version isn't recognized, even though we won't
	 * use it until later. */
	if (req->schema != 0 && req->schema != 1) {
		tracef("bad schema version %d", req->schema);
		failure(req, DQLITE_PARSE, "unrecognized schema version");
		return 0;
	}
	/* The only difference in layout between the v0 and v1 requests is in the tuple,
	 * which isn't parsed until bind__params later on. */
	rc = request_exec_sql__decode(cursor, &request);
	if (rc != 0) {
		return rc;
	}

	CHECK_LEADER(req);
	LOOKUP_DB(request.db_id);
	FAIL_IF_CHECKPOINTING;
	g->req = req;
	g->sql = request.sql;
	rc = leader__barrier(g->leader, &g->barrier, execSqlBarrierCb);
	if (rc != 0) {
		tracef("handle exec sql barrier failed %d", rc);
		g->req = NULL;
		g->sql = NULL;
		return rc;
	}
	return 0;
}

static void querySqlBarrierCb(struct barrier *barrier, int status)
{
	tracef("query sql barrier cb status:%d", status);
	struct cursor *cursor;
	struct gateway *g = barrier->data;
	struct handle *req = g->req;
	const char *sql = g->sql;
	sqlite3_stmt *stmt;
	const char *tail;
	int tuple_format;
	int rv;

	assert(g->req != NULL);
	cursor = &req->cursor;
	g->req = NULL;
	assert(g->stmt == NULL);
	g->sql = NULL;
	if (status != 0) {
		failure(req, status, "barrier error");
		return;
	}

	rv = sqlite3_prepare_v2(g->leader->conn, sql, -1, &stmt,
				&tail);
	if (rv != SQLITE_OK) {
		tracef("handle query sql prepare failed %d", rv);
		failure(req, rv, sqlite3_errmsg(g->leader->conn));
		return;
	}

	if (stmt == NULL) {
		tracef("handle query sql empty statement");
		failure(req, rv, "empty statement");
		return;
	}

	switch (req->schema) {
		case 0:
			tuple_format = TUPLE__PARAMS;
			break;
		case 1:
			tuple_format = TUPLE__PARAMS32;
			break;
		default:
			/* Should have been caught by handle_query_sql */
			assert(0);
	}
	rv = bind__params(stmt, cursor, tuple_format);
	if (rv != 0) {
                tracef("handle query sql bind failed %d", rv);
		sqlite3_finalize(stmt);
		failure(req, rv, sqlite3_errmsg(g->leader->conn));
		return;
	}

	g->stmt_finalize = true;
	query_batch(stmt, req);
}

static int handle_query_sql(struct handle *req)
{
        tracef("handle query sql");
	struct cursor *cursor = &req->cursor;
	struct gateway *g = req->gateway;
	struct request_query_sql request = {0};
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
	g->sql = request.sql;
	rv = leader__barrier(g->leader, &g->barrier, querySqlBarrierCb);
	if (rv != 0) {
		tracef("handle query sql barrier failed %d", rv);
		g->req = NULL;
		g->sql = NULL;
		return rv;
	}
	return 0;
}

static int handle_interrupt(struct handle *req)
{
        tracef("handle interrupt");
	struct cursor *cursor = &req->cursor;
	struct gateway *g = req->gateway;
	START_V0(interrupt, empty);

	/* Take appropriate action depending on the cleanup code. */
	if (g->stmt_finalize) {
		sqlite3_finalize(g->stmt);
		g->stmt_finalize = false;
	}
	g->stmt = NULL;
	g->req = NULL;

	SUCCESS(empty, EMPTY);

	return 0;
}

struct change
{
	struct gateway *gateway;
	struct raft_change req;
};

static void raftChangeCb(struct raft_change *change, int status)
{
        tracef("raft change cb status %d", status);
	struct change *r = change->data;
	struct gateway *g = r->gateway;
	struct handle *req = g->req;
	struct response_empty response = {0};
	g->req = NULL;
	sqlite3_free(r);
	if (status != 0) {
		failure(req, translateRaftErrCode(status),
			raft_strerror(status));
	} else {
		SUCCESS(empty, EMPTY);
	}
}

static int handle_add(struct handle *req)
{
        tracef("handle add");
	struct cursor *cursor = &req->cursor;
	struct gateway *g = req->gateway;
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

static int handle_assign(struct handle *req)
{
        tracef("handle assign");
	struct cursor *cursor = &req->cursor;
	struct gateway *g = req->gateway;
	struct change *r;
	uint64_t role = DQLITE_VOTER;
	int rv;
	START_V0(assign, empty);
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

static int handle_remove(struct handle *req)
{
        tracef("handle remove");
	struct cursor *cursor = &req->cursor;
	struct gateway *g = req->gateway;
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
	void *cur;
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

static int handle_dump(struct handle *req)
{
        tracef("handle dump");
	struct cursor *cursor = &req->cursor;
	bool err = true;
	struct gateway *g = req->gateway;
	sqlite3_vfs *vfs;
	void *cur;
	char filename[1024] = {0};
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
	const char * wal_suffix = "-wal";
	strncpy(filename, request.filename, sizeof(filename) - strlen(wal_suffix) - 1);
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

	if (!err)
		req->cb(req, 0, DQLITE_RESPONSE_FILES);

	return 0;
}

static int encodeServer(struct gateway *g,
			unsigned i,
			struct buffer *buffer,
			int format)
{
	void *cur;
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

static int handle_cluster(struct handle *req)
{
        tracef("handle cluster");
	struct cursor *cursor = &req->cursor;
	struct gateway *g = req->gateway;
	unsigned i;
	void *cur;
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

	req->cb(req, 0, DQLITE_RESPONSE_SERVERS);

	return 0;
}

void raftTransferCb(struct raft_transfer *r)
{
	struct gateway *g = r->data;
	struct handle *req = g->req;
	struct response_empty response = {0};
	g->req = NULL;
	sqlite3_free(r);
	if (g->raft->state == RAFT_LEADER) {
                tracef("transfer failed");
		failure(req, DQLITE_ERROR, "leadership transfer failed");
	} else {
		SUCCESS(empty, EMPTY);
	}
}

static int handle_transfer(struct handle *req)
{
        tracef("handle transfer");
	struct cursor *cursor = &req->cursor;
	struct gateway *g = req->gateway;
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

static int handle_describe(struct handle *req)
{
        tracef("handle describe");
	struct cursor *cursor = &req->cursor;
	struct gateway *g = req->gateway;
	START_V0(describe, metadata);
	if (request.format != DQLITE_REQUEST_DESCRIBE_FORMAT_V0) {
                tracef("bad format");
		failure(req, SQLITE_PROTOCOL, "bad format version");
	}
	response.failure_domain = g->config->failure_domain;
	response.weight = g->config->weight;
	SUCCESS(metadata, METADATA);
	return 0;
}

static int handle_weight(struct handle *req)
{
        tracef("handle weight");
	struct cursor *cursor = &req->cursor;
	struct gateway *g = req->gateway;
	START_V0(weight, empty);
	g->config->weight = request.weight;
	SUCCESS(empty, EMPTY);
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

	/* Check if there is a request in progress. */
	if (g->req != NULL && type != DQLITE_REQUEST_HEARTBEAT) {
		if (g->req->type == DQLITE_REQUEST_QUERY ||
		    g->req->type == DQLITE_REQUEST_QUERY_SQL) {
			/* TODO: handle interrupt requests */
			assert(type == DQLITE_REQUEST_INTERRUPT);
			goto handle;
		}
		if (g->req->type == DQLITE_REQUEST_EXEC ||
		    g->req->type == DQLITE_REQUEST_EXEC_SQL) {
                        tracef("gateway handle - BUSY");
			return SQLITE_BUSY;
		}
		assert(0);
	}

handle:
	req->type = type;
	req->schema = schema;
	req->gateway = g;
	req->cb = cb;
	req->buffer = buffer;
	req->db_id = 0;
	req->stmt_id = 0;

	switch (type) {
#define DISPATCH(LOWER, UPPER, _)                 \
	case DQLITE_REQUEST_##UPPER:              \
		rc = handle_##LOWER(req); \
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
	assert(g->stmt != NULL);
        tracef("gateway resume - not finished");
	*finished = false;
	query_batch(g->stmt, g->req);
	return 0;
}
