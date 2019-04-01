#include "gateway.h"
#include "request.h"
#include "response.h"

/* Context for exec requests */
struct gateway_exec
{
	struct handle *handle;
	struct stmt *stmt;
	struct exec exec;
};

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

static void handle_exec_cb(struct exec *exec, int status)
{
	struct gateway_exec *r = exec->data;
	struct handle *req = r->handle;
	struct stmt *stmt = r->stmt;
	struct response_result response;

	sqlite3_free(r);

	if (status == SQLITE_DONE) {
		response.last_insert_id = sqlite3_last_insert_rowid(stmt->db);
		response.rows_affected = sqlite3_changes(stmt->db);
		SUCCESS(result, RESULT);
	} else {
		failure(r->handle, status, "exec error");
		sqlite3_reset(stmt->stmt);
	}
}

static int handle_exec(struct handle *req, struct cursor *cursor)
{
	struct gateway *g = req->gateway;
	struct stmt *stmt;
	struct gateway_exec *r;
	int rc;
	START(exec, result);
	LOOKUP_DB(request.db_id);
	LOOKUP_STMT(request.stmt_id);
	(void)response;
	r = sqlite3_malloc(sizeof *r);
	if (r == NULL) {
		return DQLITE_NOMEM;
	}
	r->handle = req;
	r->stmt = stmt;
	r->exec.data = r;
	rc = leader__exec(g->leader, &r->exec, stmt->stmt, handle_exec_cb);
	if (rc != 0) {
		return rc;
	}
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

int gateway__handle(struct gateway *g,
		    struct handle *req,
		    int type,
		    struct cursor *cursor,
		    struct buffer *buffer,
		    handle_cb cb)
{
	int rc;

	/* Abort if we can't accept the request at this time */
	if (g->req != NULL) {
		return DQLITE_PROTO;
	}

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
