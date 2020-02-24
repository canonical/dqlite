#include "gateway.h"

#include "bind.h"
#include "protocol.h"
#include "query.h"
#include "request.h"
#include "response.h"
#include "vfs.h"

void gateway__init(struct gateway *g,
		   struct config *config,
		   struct registry *registry,
		   struct raft *raft)
{
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
	g->protocol = DQLITE_PROTOCOL_VERSION;
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
		int rv_;                                         \
		rv_ = request_##REQ##__decode(cursor, &request); \
		if (rv_ != 0) {                                  \
			return rv_;                              \
		}                                                \
	}

#define CHECK_LEADER(REQ)                                            \
	if (raft_state(req->gateway->raft) != RAFT_LEADER) {         \
		failure(req, SQLITE_IOERR_NOT_LEADER, "not leader"); \
		return 0;                                            \
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

static int handle_leader_legacy(struct handle *req, struct cursor *cursor)
{
	START(leader, server_legacy);
	raft_id id;
	raft_leader(req->gateway->raft, &id, &response.address);
	if (response.address == NULL) {
		response.address = "";
	}
	SUCCESS(server_legacy, SERVER_LEGACY);
	return 0;
}

static int handle_leader(struct handle *req, struct cursor *cursor)
{
	if (req->gateway->protocol == DQLITE_PROTOCOL_VERSION_LEGACY) {
		return handle_leader_legacy(req, cursor);
	}
	START(leader, server);
	raft_id id;
	raft_leader(req->gateway->raft, &id, &response.address);
	response.id = id;
	if (response.address == NULL) {
		response.address = "";
	}
	SUCCESS(server, SERVER);
	return 0;
}

static int handle_client(struct handle *req, struct cursor *cursor)
{
	START(client, welcome);
	response.heartbeat_timeout = req->gateway->config->heartbeat_timeout;
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
	rc = leader__init(g->leader, db, g->raft);
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
	const char *tail;
	int rc;
	START(prepare, stmt);
	LOOKUP_DB(request.db_id);
	rc = stmt__registry_add(&g->stmts, &stmt);
	if (rc != 0) {
		return rc;
	}
	assert(stmt != NULL);
	rc = sqlite3_prepare_v2(g->leader->conn, request.sql, -1, &stmt->stmt,
				&tail);
	if (rc != SQLITE_OK) {
		failure(req, rc, sqlite3_errmsg(g->leader->conn));
		return 0;
	}
	response.db_id = request.db_id;
	response.id = stmt->id;
	response.params = sqlite3_bind_parameter_count(stmt->stmt);
	SUCCESS(stmt, STMT);
	return 0;
}

/* Fill a result response with the last inserted ID and number of rows
 * affected. */
static void fill_result(struct gateway *g, struct response_result *response)
{
	response->last_insert_id = sqlite3_last_insert_rowid(g->leader->conn);
	response->rows_affected = sqlite3_changes(g->leader->conn);
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
		failure(req, status, sqlite3_errmsg(g->leader->conn));
		sqlite3_reset(stmt);
	}
}

static int handle_exec(struct handle *req, struct cursor *cursor)
{
	struct gateway *g = req->gateway;
	struct stmt *stmt;
	int rv;
	START(exec, result);
	LOOKUP_DB(request.db_id);
	LOOKUP_STMT(request.stmt_id);
	(void)response;
	rv = bind__params(stmt->stmt, cursor);
	if (rv != 0) {
		failure(req, rv, "bind parameters");
		return 0;
	}
	g->req = req;
	g->stmt = stmt->stmt;
	rv = leader__exec(g->leader, &g->exec, stmt->stmt, leader_exec_cb);
	if (rv != 0) {
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
	struct gateway *g = barrier->data;
	struct handle *handle = g->req;
	sqlite3_stmt *stmt = g->stmt;

	assert(handle != NULL);
	assert(stmt != NULL);

	g->stmt = NULL;
	g->req = NULL;

	if (status != 0) {
		failure(handle, status, "barrier error");
		return;
	}

	query_batch(stmt, handle);
}

static int handle_query(struct handle *req, struct cursor *cursor)
{
	struct gateway *g = req->gateway;
	struct stmt *stmt;
	int rv;
	START(query, rows);
	CHECK_LEADER(req);
	LOOKUP_DB(request.db_id);
	LOOKUP_STMT(request.stmt_id);
	(void)response;
	rv = bind__params(stmt->stmt, cursor);
	if (rv != 0) {
		failure(req, rv, sqlite3_errmsg(g->leader->conn));
		return 0;
	}
	g->req = req;
	g->stmt = stmt->stmt;
	rv = leader__barrier(g->leader, &g->barrier, query_barrier_cb);
	if (rv != 0) {
		g->req = NULL;
		g->stmt = NULL;
		return rv;
	}
	return 0;
}

static int handle_finalize(struct handle *req, struct cursor *cursor)
{
	struct stmt *stmt;
	int rv;
	START(finalize, empty);
	LOOKUP_DB(request.db_id);
	LOOKUP_STMT(request.stmt_id);
	rv = stmt__registry_del(&req->gateway->stmts, stmt);
	if (rv != 0) {
		failure(req, rv, "finalize statement");
		return 0;
	}
	SUCCESS(empty, EMPTY);
	return 0;
}

static void handle_exec_sql_next(struct handle *req, struct cursor *cursor);

static void handle_exec_sql_cb(struct exec *exec, int status)
{
	struct gateway *g = exec->data;
	struct handle *req = g->req;

	if (status == SQLITE_DONE) {
		handle_exec_sql_next(req, NULL);
	} else {
		failure(req, status, sqlite3_errmsg(g->leader->conn));
		sqlite3_reset(g->stmt);
		sqlite3_finalize(g->stmt);
		g->req = NULL;
		g->stmt = NULL;
		g->sql = NULL;
	}
}

static void handle_exec_sql_next(struct handle *req, struct cursor *cursor)
{
	struct gateway *g = req->gateway;
	struct response_result response;
	const char *tail;
	sqlite3_stmt *stmt;
	int rv;

	if (g->sql == NULL || strcmp(g->sql, "") == 0) {
		goto success;
	}

	if (g->stmt != NULL) {
		sqlite3_finalize(g->stmt);
		g->stmt = NULL;
	}

	rv = sqlite3_prepare_v2(g->leader->conn, g->sql, -1, &stmt, &tail);
	if (rv != SQLITE_OK) {
		failure(req, rv, sqlite3_errmsg(g->leader->conn));
		goto done;
	}

	if (stmt == NULL) {
		goto success;
	}

	g->stmt = stmt;

	/* TODO: what about bindings for multi-statement SQL text? */
	if (cursor != NULL) {
		rv = bind__params(stmt, cursor);
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

static int handle_exec_sql(struct handle *req, struct cursor *cursor)
{
	struct gateway *g = req->gateway;
	START(exec_sql, result);
	LOOKUP_DB(request.db_id);
	(void)response;
	assert(g->req == NULL);
	assert(g->sql == NULL);
	assert(g->stmt == NULL);
	req->gateway->sql = request.sql;
	handle_exec_sql_next(req, cursor);
	return 0;
}

static int handle_query_sql(struct handle *req, struct cursor *cursor)
{
	struct gateway *g = req->gateway;
	const char *tail;
	int rv;
	START(query_sql, rows);
	CHECK_LEADER(req);
	LOOKUP_DB(request.db_id);
	(void)response;
	rv = sqlite3_prepare_v2(g->leader->conn, request.sql, -1, &g->stmt,
				&tail);
	if (rv != SQLITE_OK) {
		failure(req, rv, sqlite3_errmsg(g->leader->conn));
		return 0;
	}
	rv = bind__params(g->stmt, cursor);
	if (rv != 0) {
		failure(req, rv, sqlite3_errmsg(g->leader->conn));
		return 0;
	}
	g->stmt_finalize = true;
	g->req = req;
	rv = leader__barrier(g->leader, &g->barrier, query_barrier_cb);
	if (rv != 0) {
		g->req = NULL;
		g->stmt = NULL;
		return rv;
	}
	return 0;
}

static int handle_interrupt(struct handle *req, struct cursor *cursor)
{
	struct gateway *g = req->gateway;
	START(interrupt, empty);

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

/* Translate a raft error to a dqlite one. */
static int translateRaftErrCode(int code)
{
	switch (code) {
		case RAFT_NOTLEADER:
			return SQLITE_IOERR_NOT_LEADER;
		case RAFT_LEADERSHIPLOST:
			return SQLITE_IOERR_LEADERSHIP_LOST;
		case RAFT_CANTCHANGE:
			return SQLITE_BUSY;
		default:
			return SQLITE_ERROR;
	}
}

struct change
{
	struct gateway *gateway;
	struct raft_change req;
};

static void raftChangeCb(struct raft_change *change, int status)
{
	struct change *r = change->data;
	struct gateway *g = r->gateway;
	struct handle *req = g->req;
	struct response_empty response;
	g->req = NULL;
	sqlite3_free(r);
	if (status != 0) {
		failure(req, translateRaftErrCode(status),
			raft_strerror(status));
	} else {
		SUCCESS(empty, EMPTY);
	}
}

static int handle_add(struct handle *req, struct cursor *cursor)
{
	struct gateway *g = req->gateway;
	struct change *r;
	int rv;
	START(add, empty);
	(void)response;

	CHECK_LEADER(req);

	r = sqlite3_malloc(sizeof *r);
	if (r == NULL) {
		return DQLITE_NOMEM;
	}
	r->gateway = g;
	r->req.data = r;

	rv = raft_add(g->raft, &r->req, request.id, request.address,
		      raftChangeCb);
	if (rv != 0) {
		sqlite3_free(r);
		failure(req, translateRaftErrCode(rv), raft_strerror(rv));
		return 0;
	}
	g->req = req;

	return 0;
}

/* Translate a dqlite role code to its raft equivalent. */
static int translateDqliteRole(int role)
{
	switch (role) {
		case DQLITE_VOTER:
			return RAFT_VOTER;
		case DQLITE_STANDBY:
			return RAFT_STANDBY;
		case DQLITE_SPARE:
			return RAFT_SPARE;
		default:
			/* For backward compat with clients that don't set a
			 * role. */
			return DQLITE_VOTER;
	}
}

static int handle_assign(struct handle *req, struct cursor *cursor)
{
	struct gateway *g = req->gateway;
	struct change *r;
	uint64_t role = DQLITE_VOTER;
	int rv;
	START(assign, empty);
	(void)response;

	CHECK_LEADER(req);

	/* Detect if this is an assign role request, instead of the former
	 * promote request. */
	if (cursor->cap > 0) {
		rv = uint64__decode(cursor, &role);
		if (rv != 0) {
			return rv;
		}
	}

	r = sqlite3_malloc(sizeof *r);
	if (r == NULL) {
		return DQLITE_NOMEM;
	}
	r->gateway = g;
	r->req.data = r;

	rv = raft_assign(g->raft, &r->req, request.id,
			 translateDqliteRole(role), raftChangeCb);
	if (rv != 0) {
		sqlite3_free(r);
		failure(req, translateRaftErrCode(rv), raft_strerror(rv));
		return 0;
	}
	g->req = req;

	return 0;
}

static int handle_remove(struct handle *req, struct cursor *cursor)
{
	struct gateway *g = req->gateway;
	struct change *r;
	int rv;
	START(remove, empty);
	(void)response;

	CHECK_LEADER(req);

	r = sqlite3_malloc(sizeof *r);
	if (r == NULL) {
		return DQLITE_NOMEM;
	}
	r->gateway = g;
	r->req.data = r;

	rv = raft_remove(g->raft, &r->req, request.id, raftChangeCb);
	if (rv != 0) {
		sqlite3_free(r);
		failure(req, translateRaftErrCode(rv), raft_strerror(rv));
		return 0;
	}
	g->req = req;

	return 0;
}

static int dumpFile(struct gateway *g,
		    const char *filename,
		    struct buffer *buffer)
{
	void *cur;
	void *buf;
	size_t file_size;
	uint64_t len;
	int rv;

	rv = vfsFileRead(g->config->name, filename, &buf, &file_size);
	if (rv != 0) {
		return rv;
	}
	len = file_size;

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

	if (len == 0) {
		return 0;
	}

	assert(len % 8 == 0);
	assert(buf != NULL);

	cur = buffer__advance(buffer, len);
	if (cur == NULL) {
		goto oom;
	}
	memcpy(cur, buf, len);

	sqlite3_free(buf);

	return 0;

oom:
	sqlite3_free(buf);
	return DQLITE_NOMEM;
}

static int handle_dump(struct handle *req, struct cursor *cursor)
{
	struct gateway *g = req->gateway;
	void *cur;
	int rv;
	char filename[1024];
	START(dump, files);

	response.n = 2;
	cur = buffer__advance(req->buffer, response_files__sizeof(&response));
	assert(cur != NULL);
	response_files__encode(&response, &cur);

	/* Main database file. */
	rv = dumpFile(g, request.filename, req->buffer);
	if (rv != 0) {
		failure(req, rv, "failed to dump database");
		return 0;
	}

	strcpy(filename, request.filename);
	strcat(filename, "-wal");
	rv = dumpFile(g, filename, req->buffer);
	if (rv != 0) {
		failure(req, rv, "failed to dump wal file");
		return 0;
	}

	req->cb(req, 0, DQLITE_RESPONSE_FILES);

	return 0;
}

/* Translate a raft role code to its dqlite equivalent. */
static int translateRaftRole(int role)
{
	switch (role) {
		case RAFT_VOTER:
			return DQLITE_VOTER;
		case RAFT_STANDBY:
			return DQLITE_STANDBY;
		case RAFT_SPARE:
			return DQLITE_SPARE;
		default:
			assert(0);
			return -1;
	}
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

	id = g->raft->configuration.servers[i].id;
	address = g->raft->configuration.servers[i].address;
	role = translateRaftRole(g->raft->configuration.servers[i].role);

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

static int handle_cluster(struct handle *req, struct cursor *cursor)
{
	struct gateway *g = req->gateway;
	unsigned i;
	void *cur;
	int rv;
	START(cluster, servers);

	response.n = g->raft->configuration.n;
	cur = buffer__advance(req->buffer, response_servers__sizeof(&response));
	assert(cur != NULL);
	response_servers__encode(&response, &cur);

	for (i = 0; i < response.n; i++) {
		rv = encodeServer(g, i, req->buffer, request.format);
		if (rv != 0) {
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
	struct response_empty response;
	g->req = NULL;
	sqlite3_free(r);
	if (g->raft->state == RAFT_LEADER) {
		failure(req, DQLITE_ERROR, "leadership transfer failed");
	} else {
		SUCCESS(empty, EMPTY);
	}
}

static int handle_transfer(struct handle *req, struct cursor *cursor)
{
	struct gateway *g = req->gateway;
	struct raft_transfer *r;
	int rv;
	START(transfer, empty);
	(void)response;

	CHECK_LEADER(req);

	r = sqlite3_malloc(sizeof *r);
	if (r == NULL) {
		return DQLITE_NOMEM;
	}
	r->data = g;

	rv = raft_transfer(g->raft, r, request.id, raftTransferCb);
	if (rv != 0) {
		sqlite3_free(r);
		failure(req, translateRaftErrCode(rv), raft_strerror(rv));
		return 0;
	}
	g->req = req;

	return 0;
}

int gateway__handle(struct gateway *g,
		    struct handle *req,
		    int type,
		    struct cursor *cursor,
		    struct buffer *buffer,
		    handle_cb cb)
{
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
			return SQLITE_BUSY;
		}
		assert(0);
	}

handle:
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

int gateway__resume(struct gateway *g, bool *finished)
{
	if (g->req == NULL || (g->req->type != DQLITE_REQUEST_QUERY &&
			       g->req->type != DQLITE_REQUEST_QUERY_SQL)) {
		*finished = true;
		return 0;
	}
	assert(g->stmt != NULL);
	*finished = false;
	query_batch(g->stmt, g->req);
	return 0;
}
