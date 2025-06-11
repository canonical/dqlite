#include "gateway.h"
#include <sqlite3.h>

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

#define RAFT_GATEWAY_PARSE 0xff01 /* Internal use only */

static bool sqlite3_statement_empty(sqlite3 *conn, const char *sql)
{
	if (sql == NULL || sql[0] == '\0') {
		return true;
	}
	sqlite3_stmt *stmt = NULL;
	int rc = sqlite3_prepare_v2(conn, sql, -1, &stmt, NULL);
	sqlite3_finalize(stmt);
	return rc == SQLITE_OK && stmt == NULL;
}

static void interrupt(struct gateway *g);

void gateway__init(struct gateway *g,
		   struct config *config,
		   struct registry *registry,
		   struct raft *raft)
{
	tracef("gateway init");
	*g = (struct gateway){
		.config = config,
		.registry = registry,
		.raft = raft,
		.protocol = DQLITE_PROTOCOL_VERSION,
	};
	stmt__registry_init(&g->stmts);
}

static void gateway__leader_close_cb(struct leader *leader)
{
	PRE(leader->data != NULL);
	struct gateway *g = leader->data;
	raft_free(leader);
	g->leader = NULL;
	stmt__registry_close(&g->stmts);
	if (g->close_cb != NULL) {
		g->close_cb(g);
	}
}

static void gateway_finalize(struct gateway *g)
{
	if (g->leader != NULL) {
		/* Before closing the gateway, signal to the existing leader that we
		 * are closing and wait to drain the queue. */
		leader__close(g->leader, gateway__leader_close_cb);
	} else if (g->close_cb != NULL) {
		stmt__registry_close(&g->stmts);
		g->close_cb(g);
	}
}

void gateway__close(struct gateway *g, gateway_close_cb cb)
{
	PRE(cb != NULL);
	g->close_cb = cb;
	if (g->req != NULL) {
		tracef("gateway deferred close");
		/* An exec is still running, so it is not possible to close
		 * right away. Instead, we wait for the exec to finish and then
		 * call the close callback. */
		interrupt(g);
	} else {
		tracef("gateway close");
		gateway_finalize(g);
	}
}

#define DECLARE_REQUEST(REQ, ...) struct request_##REQ request = { 0 }
#define DECLARE_RESPONSE(REQ, RES, ...) \
	DECLARE_REQUEST(REQ);           \
	struct response_##RES response = { 0 }

#define __GET_DECLARE_RESPONSE_MACRO(REQ, RES, MACRO, ...) MACRO

#define DECLARE_V0(...)                                             \
	__GET_DECLARE_RESPONSE_MACRO(__VA_ARGS__, DECLARE_RESPONSE, \
				     DECLARE_REQUEST)               \
	(__VA_ARGS__)

#define INIT_V0(REQ, ...)                                             \
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

/* START_V0(request_type[, response_type]) declares a request for protocol
 * version 0 and decodes it. If response_type is also passed, a response will
 * also be declared and initialized to 0. */
#define START_V0(...)            \
	DECLARE_V0(__VA_ARGS__); \
	INIT_V0(__VA_ARGS__)

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

/* Lookup the database with the given ID. */
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
	char *cursor;
	struct response_failure failure = {
		.code = (uint64_t)code,
		.message = message,
	};
	size_t n = response_failure__sizeof(&failure);
	assert(n % 8 == 0);
	cursor = buffer__advance(req->buffer, n);
	/* The buffer has at least 4096 bytes, and error messages are shorter
	 * than that. So this can't fail. */
	assert(cursor != NULL);
	response_failure__encode(&failure, &cursor);
	req->cb(req, code, DQLITE_RESPONSE_FAILURE, 0);
}

/**
 * exec_failure translate the tuple (raft_error, sqlite_error) into
 * a SQLITE_XXX error code and message and calls failure. It is important
 * not to call any sqlite3_XXX function after the failure and before calling
 * this function as it might change the error message in the connection.
 */
static void exec_failure(struct gateway *g, struct handle *req, int raft_rc)
{
	PRE(g->req == NULL);
	PRE(raft_rc != 0);

	if (raft_rc == RAFT_BUSY) {
		return failure(req, SQLITE_BUSY, sqlite3_errstr(SQLITE_BUSY));
	}
	
	if (raft_rc == RAFT_NOTLEADER) {
		return failure(req, SQLITE_IOERR_NOT_LEADER, "not leader");
	}
	
	if (raft_rc == RAFT_LEADERSHIPLOST) {
		return failure(req, SQLITE_IOERR_LEADERSHIP_LOST,
			       "leadership lost");
	}

	if (raft_rc == RAFT_GATEWAY_PARSE) {
		return failure(req, SQLITE_ERROR, "bind parameters");
	}

	if (raft_rc == RAFT_ERROR) {
		/* Generic error, check if there is some more information in the
		 * connection */
		int sqlite_status = sqlite3_extended_errcode(g->leader->conn);
		if (sqlite_status == SQLITE_ROW) {
			return failure(
			    req, SQLITE_ERROR,
			    "rows yielded when none expected for EXEC request");
		}

		if (sqlite_status != SQLITE_OK && sqlite_status != SQLITE_DONE) {
			return failure(req, sqlite_status,
				       sqlite3_errmsg(g->leader->conn));
		}
	}

	return failure(req, SQLITE_IOERR, "leader exec failed");
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
	g->leader = raft_malloc(sizeof *g->leader);
	if (g->leader == NULL) {
		tracef("malloc failed");
		return DQLITE_NOMEM;
	}
	rc = leader__init(g->leader, db, g->raft);
	if (rc != 0) {
		tracef("leader init failed %d", rc);
		raft_free(g->leader);
		g->leader = NULL;
		return rc;
	}
	g->leader->data = g;
	response.id = 0;
	SUCCESS_V0(db, DB);
	return 0;
}

static void handle_prepare_done_cb(struct exec *exec)
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
	g->req = NULL;

	if (g->close_cb) {
		/* The gateway is closing. All resources should be closed. */
		sqlite3_finalize(stmt);
		return gateway_finalize(g);
	}
	if (status != 0) {
		return exec_failure(g, req, status);
	}

	if (stmt == NULL) {
		/* FIXME Should we use a code other than 0 here? */
		return failure(req, 0, "empty statement");
	}


	if (req->schema == DQLITE_PREPARE_STMT_SCHEMA_V0) {
		if (!sqlite3_statement_empty(g->leader->conn, tail)) {
			sqlite3_finalize(stmt);
			return failure(req, SQLITE_ERROR,
				"nonempty statement tail");
		}
	}

	struct stmt *registry_stmt;
	int rc = stmt__registry_add(&g->stmts, &registry_stmt);
	if (rc != 0) {
		sqlite3_finalize(stmt);
		return failure(req, SQLITE_NOMEM, "stmt registry add failed");
	}
	registry_stmt->stmt = stmt;

	struct response_stmt response_v0 = {};
	struct response_stmt_with_offset response_v1 = {};
	switch (req->schema) {
		case DQLITE_PREPARE_STMT_SCHEMA_V0:
			response_v0.db_id = (uint32_t)req->db_id;
			response_v0.id = (uint32_t)registry_stmt->id;
			response_v0.params =
				(uint64_t)sqlite3_bind_parameter_count(
				registry_stmt->stmt);
			SUCCESS(stmt, STMT, response_v0,
				DQLITE_PREPARE_STMT_SCHEMA_V0);
			break;
		case DQLITE_PREPARE_STMT_SCHEMA_V1:
			response_v1.db_id = (uint32_t)req->db_id;
			response_v1.id = (uint32_t)registry_stmt->id;
			response_v1.params =
				(uint64_t)sqlite3_bind_parameter_count(
				registry_stmt->stmt);
			response_v1.offset = (uint64_t)(tail - sql);
			SUCCESS(stmt_with_offset, STMT_WITH_OFFSET,
				response_v1,
				DQLITE_PREPARE_STMT_SCHEMA_V1);
			break;
		default:
			assert(0);
	}
}

static int handle_prepare(struct gateway *g, struct handle *req)
{
	tracef("handle prepare");
	struct cursor *cursor = &req->cursor;
	struct request_prepare request = { 0 };
	int rc;

	if (!IN(req->schema, DQLITE_PREPARE_STMT_SCHEMA_V0,
		DQLITE_PREPARE_STMT_SCHEMA_V1)) {
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
	*exec = (struct exec){
		.data = g,
		.sql = request.sql,
	};
	g->req = req;
	leader_exec(g->leader, exec, NULL, handle_prepare_done_cb);
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

static void handle_exec_work_cb(struct exec *exec)
{
	PRE(exec->stmt != NULL);
	struct gateway *g = exec->data;
	struct handle *req = g->req;

	int rv = 0;
	rv = bind__params(exec->stmt, &req->decoder);
	if (rv != DQLITE_OK) {
		leader_exec_result(exec, RAFT_GATEWAY_PARSE);
	} else {
		req->parameters_bound = true;
		rv = sqlite3_step(exec->stmt);
		leader_exec_result(exec,
				   rv == SQLITE_DONE ? RAFT_OK : RAFT_ERROR);
	}

	TAIL return leader_exec_resume(exec);
}

static void handle_exec_done_cb(struct exec *exec)
{
	struct gateway *g = exec->data;
	PRE(g->leader != NULL && g->req != NULL);
	int raft_status = exec->status;
	struct handle *req = g->req;
	sqlite3_stmt *stmt = exec->stmt;
	raft_free(exec);

	g->req = NULL;

	if (g->close_cb != NULL) {
		return gateway_finalize(g);
	}

	if (raft_status != 0) {
		exec_failure(g, req, raft_status);
		goto done;
	}

	struct response_result response;
	fill_result(g, &response);
	SUCCESS(result, RESULT, response, 0);

done:
	sqlite3_clear_bindings(stmt);
	sqlite3_reset(stmt);
}

static int handle_exec(struct gateway *g, struct handle *req)
{
	tracef("handle exec schema:%" PRIu8, req->schema);
	struct cursor *cursor = &req->cursor;
	struct stmt *stmt;
	struct request_exec request = { 0 };
	int rv;

	if (!IN(req->schema, DQLITE_REQUEST_PARAMS_SCHEMA_V0,
		DQLITE_REQUEST_PARAMS_SCHEMA_V1)) {
		tracef("bad schema version %d", req->schema);
		failure(req, SQLITE_ERROR, "unrecognized schema version");
		return 0;
	}

	/* The v0 and v1 schemas only differ in the layout of the tuple,
	 * so we can use the same decode function for both. */
	rv = request_exec__decode(cursor, &request);
	if (rv != 0) {
		return rv;
	}

	int format = req->schema == DQLITE_REQUEST_PARAMS_SCHEMA_V0
			 ? TUPLE__PARAMS
			 : TUPLE__PARAMS32;
	rv = tuple_decoder__init(&req->decoder, 0, format, cursor);
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

static void handle_exec_sql_done_cb(struct exec *exec)
{
	struct gateway *g = exec->data;
	struct handle *req = g->req;
	int raft_status = exec->status;
	struct response_result response = {};

	/* Statement must be finalized manually as it is not in the registry */
	sqlite3_stmt *stmt = exec->stmt;

	if (raft_status == 0 && g->close_cb == NULL && 
		exec->tail != NULL && exec->tail[0] != '\0') {
		sqlite3_finalize(stmt);
		req->parameters_bound = false;
		*exec = (struct exec){
			.data = g,
			.sql = exec->tail,
		};
		return leader_exec(g->leader, exec, handle_exec_work_cb,
				   handle_exec_sql_done_cb);
	}
	
	g->req = NULL;
	raft_free(exec);

	if (g->close_cb != NULL) {
		sqlite3_finalize(stmt);
		return gateway_finalize(g);
	}

	if (raft_status != 0) {
		exec_failure(g, req, raft_status);
	} else {
		fill_result(g, &response);
		SUCCESS(result, RESULT, response, 0);
	}
	sqlite3_finalize(stmt);
}

static int handle_exec_sql(struct gateway *g, struct handle *req)
{
	tracef("handle exec sql schema:%" PRIu8, req->schema);
	struct cursor *cursor = &req->cursor;
	struct request_exec_sql request = { 0 };
	int rv;

	if (!IN(req->schema, DQLITE_REQUEST_PARAMS_SCHEMA_V0,
		DQLITE_REQUEST_PARAMS_SCHEMA_V1)) {
		tracef("bad schema version %d", req->schema);
		failure(req, SQLITE_ERROR, "unrecognized schema version");
		return 0;
	}
	/* The only difference in layout between the v0 and v1 requests is in
	 * the tuple, which isn't parsed until bind__params later on. */
	rv = request_exec_sql__decode(cursor, &request);
	if (rv != 0) {
		return rv;
	}

	int format = req->schema == DQLITE_REQUEST_PARAMS_SCHEMA_V0
			 ? TUPLE__PARAMS
			 : TUPLE__PARAMS32;
	rv = tuple_decoder__init(&req->decoder, 0, format, cursor);
	if (rv != 0) {
		return rv;
	}

	CHECK_LEADER(req);
	LOOKUP_DB(request.db_id);
	FAIL_IF_CHECKPOINTING;
	g->req = req;

	struct exec *exec = raft_malloc(sizeof *exec);
	if (exec == NULL) {
		return DQLITE_NOMEM;
	}
	*exec = (struct exec){
		.data = g,
		.sql = request.sql,
	};
	leader_exec(g->leader, exec, handle_exec_work_cb,
		    handle_exec_sql_done_cb);
	return 0;
}

static void handle_query_work_cb(struct exec *exec)
{
	struct gateway *g = exec->data;
	PRE(g->req != NULL && g->leader != NULL);
	struct handle *req = g->req;

	if (req->cancellation_requested) {
		/* Nothing else to do. */
		TAIL return leader_exec_resume(exec);
	}

	if (!sqlite3_statement_empty(exec->leader->conn, exec->tail)) {
		leader_exec_result(exec, RAFT_ERROR);
		TAIL return leader_exec_resume(exec);
	}

	exec->tail = NULL;
	int rc;
	if (!req->parameters_bound) {
		PRE(!sqlite3_stmt_busy(exec->stmt));
		rc = bind__params(exec->stmt, &req->decoder);
		if (rc != DQLITE_OK ||
		    tuple_decoder__remaining(&req->decoder) > 0) {
			leader_exec_result(exec, RAFT_GATEWAY_PARSE);
			TAIL return leader_exec_resume(exec);
		}
		/* FIXME(marco6): Should I check if all bindings were consumed?
		 * And moreover, should I allow parameters altogether in this
		 * case? */
		req->parameters_bound = true;
	}

	rc = query__batch(exec->stmt, req->buffer);
	if (rc == SQLITE_ROW) {
		/* If the statement is still running, do not resume the
		 * exec state machine, but send a response instead. The
		 * execution will resume as soon as the data has been
		 * sent. See gateway__resume. */
		struct response_rows response = {
			.eof = DQLITE_RESPONSE_ROWS_PART,
		};
		SUCCESS(rows, ROWS, response, 0);
		return;
	}

	leader_exec_result(exec, rc == SQLITE_DONE ? RAFT_OK : RAFT_ERROR);
	TAIL return leader_exec_resume(exec);
}

static void handle_query_done_cb(struct exec *exec)
{
	struct gateway *g = exec->data;
	PRE(g != NULL);
	struct handle *req = g->req;
	PRE(req != NULL);
	int status = exec->status;
	sqlite3_stmt *stmt = exec->stmt;
	g->req = NULL;
	raft_free(exec);

	if (g->close_cb != NULL) {
		return gateway_finalize(g);
	}

	if (status != 0) {
		exec_failure(g, req, status);
		goto done;
	}


	if (req->cancellation_requested) {
		struct response_empty response = { 0 };
		SUCCESS(empty, EMPTY, response, 0);
		goto done;
	}

	struct response_rows response = {
		.eof = DQLITE_RESPONSE_ROWS_DONE,
	};
	SUCCESS(rows, ROWS, response, 0);

done:
	sqlite3_clear_bindings(stmt);
	sqlite3_reset(stmt);
}

static int handle_query(struct gateway *g, struct handle *req)
{
	tracef("handle query schema:%" PRIu8, req->schema);
	struct cursor *cursor = &req->cursor;
	struct stmt *stmt;
	struct request_query request = { 0 };
	int rv;

	if (!IN(req->schema, DQLITE_REQUEST_PARAMS_SCHEMA_V0,
		DQLITE_REQUEST_PARAMS_SCHEMA_V1)) {
		tracef("bad schema version %d", req->schema);
		failure(req, SQLITE_ERROR, "unrecognized schema version");
		return 0;
	}
	/* The only difference in layout between the v0 and v1 requests is in
	 * the tuple, which isn't parsed until bind__params later on. */
	rv = request_query__decode(cursor, &request);
	if (rv != 0) {
		return rv;
	}
	int format = req->schema == DQLITE_REQUEST_PARAMS_SCHEMA_V0
			 ? TUPLE__PARAMS
			 : TUPLE__PARAMS32;
	rv = tuple_decoder__init(&req->decoder, 0, format, cursor);
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
	*exec = (struct exec){
		.data = g,
		.stmt = stmt->stmt,
	};
	leader_exec(g->leader, exec, handle_query_work_cb,
		    handle_query_done_cb);
	return 0;
}

static void handle_query_sql_done_cb(struct exec *exec)
{
	struct gateway *g = exec->data;
	PRE(g != NULL);
	struct handle *req = g->req;
	PRE(req != NULL);
	int status = exec->status;
	bool tail = exec->stmt != NULL && exec->tail != NULL;
	sqlite3_stmt *stmt = exec->stmt;
	g->req = NULL;
	raft_free(exec);

	if (g->close_cb != NULL) {
		/* Statement must be finalized manually as it is not in the registry */
		sqlite3_finalize(stmt);
		return gateway_finalize(g);
	}

	if (status != RAFT_OK && tail) {
		failure(req, SQLITE_ERROR, "nonempty statement tail");
		goto done;
	}

	if (status != RAFT_OK && !tail) {
		exec_failure(g, req, status);
		goto done;
	}

	if (req->cancellation_requested) {
		struct response_empty response = { 0 };
		SUCCESS(empty, EMPTY, response, 0);
		goto done;
	}

	if (!req->parameters_bound) {
		failure(req, 0, "empty statement");
		goto done;
	}

	struct response_rows response = {
		.eof = DQLITE_RESPONSE_ROWS_DONE,
	};
	SUCCESS(rows, ROWS, response, 0);

done:
	sqlite3_finalize(stmt);
}

static int handle_query_sql(struct gateway *g, struct handle *req)
{
	tracef("handle query sql schema:%" PRIu8, req->schema);
	struct cursor *cursor = &req->cursor;
	struct request_query_sql request = { 0 };
	int rv;

	/* Fail early if the schema version isn't recognized. */
	if (!IN(req->schema, DQLITE_REQUEST_PARAMS_SCHEMA_V0,
		DQLITE_REQUEST_PARAMS_SCHEMA_V1)) {
		tracef("bad schema version %d", req->schema);
		failure(req, SQLITE_ERROR, "unrecognized schema version");
		return 0;
	}
	/* Schema version only affect the tuple format, which is parsed later */
	rv = request_query_sql__decode(cursor, &request);
	if (rv != 0) {
		return rv;
	}
	int format = req->schema == DQLITE_REQUEST_PARAMS_SCHEMA_V0
			 ? TUPLE__PARAMS
			 : TUPLE__PARAMS32;
	rv = tuple_decoder__init(&req->decoder, 0, format, cursor);
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
	*exec = (struct exec){
		.data = g,
		.sql = request.sql,
	};

	leader_exec(g->leader, exec, handle_query_work_cb,
		    handle_query_sql_done_cb);
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
	SUCCESS_V0(empty, EMPTY);
	return 0;
}

static void interrupt(struct gateway *g)
{
	g->req->cancellation_requested = true;
	if (g->leader != NULL && g->leader->exec != NULL) {
		leader_exec_abort(g->leader->exec);
	}
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
	raft_free(r);
	if (g->close_cb != NULL) {
		g->close_cb(g);
	} else if (status != 0) {
		failure(req, translateRaftErrCode(status),
			raft_strerror(status));
	} else {
		SUCCESS(empty, EMPTY, response, 0);
	}
}

static int handle_add(struct gateway *g, struct handle *req)
{
	tracef("handle add");
	struct cursor *cursor = &req->cursor;
	struct change *r;
	int rv;
	START_V0(add);

	CHECK_LEADER(req);

	r = raft_malloc(sizeof *r);
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
		raft_free(r);
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
	START_V0(promote_or_assign);

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

	r = raft_malloc(sizeof *r);
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
		raft_free(r);
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
	START_V0(remove);

	CHECK_LEADER(req);

	r = raft_malloc(sizeof *r);
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
		raft_free(r);
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

	/* It is not possible to use g->leader->db->vfs as dump call can
	 * be issued without opening the leader connection first. */
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
	raft_free(r);
	if (g->close_cb != NULL) {
		g->close_cb(g);
	} else if (g->raft->state == RAFT_LEADER) {
		tracef("transfer failed");
		failure(req, DQLITE_ERROR, "leadership transfer failed");
	} else {
		SUCCESS(empty, EMPTY, response, 0);
	}
}

static int handle_transfer(struct gateway *g, struct handle *req)
{
	tracef("handle transfer");
	struct cursor *cursor = &req->cursor;
	struct raft_transfer *r;
	int rv;
	START_V0(transfer);

	CHECK_LEADER(req);

	r = raft_malloc(sizeof *r);
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
		raft_free(r);
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

	if (g->close_cb != NULL) {
		/* The gateway is closing. */
		return 0;
	}

	if (g->req == NULL) {
		goto handle;
	}

	/* Request in progress. TODO The current implementation doesn't allow
	 * reading a new request while a query is yielding rows, in that case
	 * gateway__resume in write_cb will indicate it has not finished
	 * returning results and a new request (in this case, the interrupt)
	 * will not be read. */
	if (type == DQLITE_REQUEST_INTERRUPT &&
	    (g->req->type == DQLITE_REQUEST_QUERY ||
	     g->req->type == DQLITE_REQUEST_QUERY_SQL ||
	     g->req->type == DQLITE_REQUEST_EXEC ||
	     g->req->type == DQLITE_REQUEST_EXEC_SQL)) {
		/* In this case, the response will be sent by the callback of
		 * the executing query once stopped or finished. */
		interrupt(g);
		return 0;
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
	req->parameters_bound = 0;
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
