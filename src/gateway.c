#include "gateway.h"
#include "request.h"
#include "response.h"
#include "bind.h"

void gateway__init(struct gateway *g,
		   struct dqlite_logger *logger,
		   struct options *options,
		   struct registry *registry,
		   struct raft *raft)
{
	g->logger = logger;
	g->options = options;
	g->registry = registry;
	g->raft = raft;
	g->leader = NULL;
	g->req = NULL;
	g->stmt = NULL;
	g->stmt_finalize = false;
	stmt__registry_init(&g->stmts);
}

void gateway__close(struct gateway *g)
{
	stmt__registry_close(&g->stmts);
	if (g->leader != NULL) {
		leader__close(g->leader);
		sqlite3_free(g->leader);
	}
}

/* Declare a request struct and a response struct of the appropriate types and
 * decode the request. */
#define START(REQ, RES)                                          \
	struct request_##REQ request;                            \
	struct response_##RES response;                          \
	{                                                        \
		int rc2;                                         \
		rc2 = request_##REQ##__decode(cursor, &request); \
		if (rc2 != 0) {                                  \
			return rc2;                              \
		}                                                \
	}

/* Encode the given success response and invoke the request callback */
#define SUCCESS(LOWER, UPPER)                                                  \
	{                                                                      \
		size_t n = response_##LOWER##__sizeof(&response);              \
		void *cursor;                                                  \
		assert(n % 8 == 0);                                            \
		cursor = buffer__advance(req->buffer, n);                      \
		/* Since responses are small and the buffer it's at least 4096 \
		 * bytes, this can't fail. */                                  \
		assert(cursor != NULL);                                        \
		response_##LOWER##__encode(&response, &cursor);                \
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

/* Encode fa failure response and invoke the request callback */
static void failure(struct handle *req, int code, const char *message)
{
	struct response_failure failure;
	size_t n;
	void *cursor;
	failure.code = code;
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

static int handle_leader(struct handle *req, struct cursor *cursor)
{
	START(leader, server);
	unsigned id;
	raft_leader(req->gateway->raft, &id, &response.address);
	if (response.address == NULL) {
		response.address = "";
	}
	SUCCESS(server, SERVER);
	return 0;
}

static int handle_client(struct handle *req, struct cursor *cursor)
{
	START(client, welcome);
	response.heartbeat_timeout = req->gateway->options->heartbeat_timeout;
	SUCCESS(welcome, WELCOME);
	return 0;
}

static int handle_open(struct handle *req, struct cursor *cursor)
{
	struct gateway *g = req->gateway;
	struct db *db;
	int rc;
	START(open, db);
	if (g->leader != NULL) {
		failure(req, SQLITE_BUSY,
			"a database for this connection is already open");
		return 0;
	}
	rc = registry__db_get(g->registry, request.filename, &db);
	if (rc != 0) {
		return rc;
	}
	g->leader = sqlite3_malloc(sizeof *g->leader);
	if (g->leader == NULL) {
		return DQLITE_NOMEM;
	}
	rc = leader__init(g->leader, db);
	if (rc != 0) {
		return rc;
	}
	response.id = 0;
	SUCCESS(db, DB);
	return 0;
}

static int handle_prepare(struct handle *req, struct cursor *cursor)
{
	struct gateway *g = req->gateway;
	struct stmt *stmt;
	int rc;
	START(prepare, stmt);
	LOOKUP_DB(request.db_id);
	response.id = 1;
	rc = stmt__registry_add(&g->stmts, &stmt);
	if (rc != 0) {
		return rc;
	}
	assert(stmt != NULL);
	stmt->db = g->leader->conn;
	rc = sqlite3_prepare_v2(stmt->db, request.sql, -1, &stmt->stmt,
				&stmt->tail);
	if (rc != SQLITE_OK) {
		failure(req, rc, sqlite3_errmsg(stmt->db));
		return 0;
	}
	response.db_id = request.db_id;
	response.id = stmt->id;
	response.params = sqlite3_bind_parameter_count(stmt->stmt);
	SUCCESS(stmt, STMT);
	return 0;
}

static void handle_exec_cb(struct exec *exec, int status)
{
	struct gateway *g = exec->data;
	struct handle *req = g->req;
	struct stmt *stmt = g->stmt;
	struct response_result response;

	g->req = NULL;
	g->stmt = NULL;

	if (status == SQLITE_DONE) {
		response.last_insert_id = sqlite3_last_insert_rowid(stmt->db);
		response.rows_affected = sqlite3_changes(stmt->db);
		SUCCESS(result, RESULT);
	} else {
		failure(req, status, "exec error");
		sqlite3_reset(stmt->stmt);
	}
}

static int handle_exec(struct handle *req, struct cursor *cursor)
{
	struct gateway *g = req->gateway;
	struct stmt *stmt;
	int rc;
	START(exec, result);
	LOOKUP_DB(request.db_id);
	LOOKUP_STMT(request.stmt_id);
	(void)response;
	rc = bind__params(stmt->stmt, cursor);
	if (rc != 0) {
		failure(req, rc, "bind parameters");
		return 0;
	}
	g->req = req;
	g->stmt = stmt;
	g->exec.data = g;
	rc = leader__exec(g->leader, &g->exec, stmt->stmt, handle_exec_cb);
	if (rc != 0) {
		return rc;
	}
	return 0;
}

/* Step through the given statement and populate the response buffer of the
 * given request with a single batch of rows.
 *
 * A single batch of rows is typically about the size of a memory page. */
static void query_batch(struct stmt *stmt, struct handle *req)
{
	struct gateway *g = req->gateway;
	struct response_rows response;
	int rc;

	rc = stmt__query(stmt, req->buffer);
	if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
		sqlite3_reset(stmt->stmt);
		failure(req, rc, "query error");
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
		sqlite3_finalize(stmt->stmt);
		g->stmt_finalize = false;
	}
	g->stmt = NULL;
	g->req = NULL;
}

static int handle_query(struct handle *req, struct cursor *cursor)
{
	struct stmt *stmt;
	int rc;
	START(query, rows);
	LOOKUP_DB(request.db_id);
	LOOKUP_STMT(request.stmt_id);
	(void)response;
	rc = bind__params(stmt->stmt, cursor);
	if (rc != 0) {
		failure(req, rc, "bind parameters");
		return 0;
	}
	query_batch(stmt, req);
	return 0;
}

int gateway__handle(struct gateway *g,
		    struct handle *req,
		    int type,
		    struct cursor *cursor,
		    struct buffer *buffer,
		    handle_cb cb)
{
	int rc;

	/* Check if there is a request in progress. */
	if (g->req != NULL && type != DQLITE_REQUEST_HEARTBEAT) {
		if (g->req->type == DQLITE_REQUEST_QUERY ||
		    g->req->type == DQLITE_REQUEST_QUERY_SQL) {
			/* TODO: handle interrupt requests */
			assert(0);
		}
		if (g->req->type == DQLITE_REQUEST_EXEC ||
		    g->req->type == DQLITE_REQUEST_EXEC_SQL) {
			return SQLITE_BUSY;
		}
		assert(0);
	}

	req->type = type;
	req->gateway = g;
	req->cb = cb;
	req->buffer = buffer;

	switch (type) {
#define DISPATCH(LOWER, UPPER, _)                 \
	case DQLITE_REQUEST_##UPPER:              \
		rc = handle_##LOWER(req, cursor); \
		break;
		REQUEST__TYPES(DISPATCH);
	}

	return rc;
}

int gateway__resume(struct gateway *g)
{
	assert(g->req != NULL);
	assert(g->req->type == DQLITE_REQUEST_QUERY ||
	       g->req->type == DQLITE_REQUEST_QUERY_SQL);
	assert(g->stmt != NULL);
	query_batch(g->stmt, g->req);
	return 0;
}
