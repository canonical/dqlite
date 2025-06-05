#include "../../include/dqlite.h"
#include "../../src/gateway.h"
#include "../../src/lib/threadpool.h"
#include "../../src/request.h"
#include "../../src/response.h"
#include "../../src/tuple.h"
#include "../lib/cluster.h"
#include "../lib/raft_heap.h"
#include "../lib/runner.h"

TEST_MODULE(gateway);

/******************************************************************************
 *
 * Fixture.
 *
 ******************************************************************************/

/* Context for a gateway handle request. */
struct context {
	bool invoked;
	bool resume;  /* True to resume the request when closing */
	int status;
	uint8_t type;
	uint8_t schema;
};

/* Drive a single gateway. Each gateway is associated with a different raft
 * node. */
struct connection {
	struct gateway gateway;
	struct buffer buf1;   /* Request payload */
	struct buffer buf2;   /* Response payload */
	struct cursor cursor; /* Response read cursor */
	struct handle handle; /* Async handle request */
	struct context context;
};

#define FIXTURE                               \
	FIXTURE_CLUSTER;                          \
	struct connection connections[N_SERVERS]; \
	struct gateway *gateway;                  \
	struct buffer *buf1;                      \
	struct cursor *cursor;                    \
	struct buffer *buf2;                      \
	struct handle *handle;                    \
	struct context *context;

#define SETUP                                                       \
	unsigned i;                                                     \
	int rc;                                                         \
	SETUP_CLUSTER(V2);                                              \
	for (i = 0; i < N_SERVERS; i++) {                               \
		struct connection *c = &f->connections[i];                  \
		struct config *config;                                      \
		config = CLUSTER_CONFIG(i);                                 \
		config->page_size = 512;                                    \
		gateway__init(&c->gateway, config, CLUSTER_REGISTRY(i),     \
			      CLUSTER_RAFT(i));                                 \
		c->handle.data = &c->context;                               \
		rc = buffer__init(&c->buf1);                                \
		munit_assert_int(rc, ==, 0);                                \
		rc = buffer__init(&c->buf2);                                \
		munit_assert_int(rc, ==, 0);                                \
	}                                                               \
	test_raft_heap_setup(params, user_data);                        \
	pool_ut_fallback()->flags |= POOL_FOR_UT_NOT_ASYNC;             \
	pool_ut_fallback()->flags |= POOL_FOR_UT;                       \
	SELECT(0)

#define TEAR_DOWN                                      \
	unsigned i;                                        \
	test_raft_heap_tear_down(data);                    \
	for (i = 0; i < N_SERVERS; i++) {                  \
		struct connection *c = &f->connections[i];     \
		f->context = &c->context;                      \
		c->context.invoked = false;                    \
		gateway__close(&c->gateway, closeCb);          \
		if (c->context.resume) {                       \
			bool finished;                             \
			gateway__resume(&c->gateway, &finished);   \
		}                                              \
		WAIT;                                          \
		buffer__close(&c->buf1);                       \
		buffer__close(&c->buf2);                       \
	}                                                  \
	TEAR_DOWN_CLUSTER;

static void closeCb(struct gateway *g) {
	struct connection *c = CONTAINER_OF(g, struct connection, gateway);
	c->context.invoked = true;
}

static void handleCb(struct handle *req,
		     int status,
		     uint8_t type,
		     uint8_t schema)
{
	struct connection *conn = CONTAINER_OF(req, struct connection, handle);
	struct context *ctx = &conn->context;
	ctx->invoked = true;
	ctx->resume = true;
	ctx->status = status;
	ctx->type = type;
	ctx->schema = schema;
}

static void barrierCb(struct raft_barrier *req, int status) {
	(void)req;
	(void)status;
}

/******************************************************************************
 *
 * Helper macros.
 *
 ******************************************************************************/

/* Select which gateway to use for performing requests. */
#define SELECT(I)                                \
	f->gateway = &f->connections[I].gateway; \
	f->buf1 = &f->connections[I].buf1;       \
	f->buf2 = &f->connections[I].buf2;       \
	f->cursor = &f->connections[I].cursor;   \
	f->context = &f->connections[I].context; \
	f->handle = &f->connections[I].handle

/* Allocate the payload buffer, encode a request of the given lower case name
 * and initialize the fixture cursor. */
#define ENCODE(REQUEST, LOWER)                                  \
	{                                                       \
		size_t n2 = request_##LOWER##__sizeof(REQUEST); \
		char *cursor;                                   \
		buffer__reset(f->buf1);                         \
		cursor = buffer__advance(f->buf1, n2);          \
		munit_assert_ptr_not_null(cursor);              \
		request_##LOWER##__encode(REQUEST, &cursor);    \
	}

/* Encode N parameters with the given values in the given format */
#define ENCODE_PARAMS(N, VALUES, FORMAT)                                      \
	{                                                                     \
		struct tuple_encoder encoder;                                 \
		unsigned long i2;                                             \
		int rc2;                                                      \
		rc2 = tuple_encoder__init(&encoder, N, FORMAT, f->buf1);      \
		munit_assert_int(rc2, ==, 0);                                 \
		for (i2 = 0; i2 < N; i2++) {                                  \
			rc2 = tuple_encoder__next(&encoder, &((VALUES)[i2])); \
			munit_assert_int(rc2, ==, 0);                         \
		}                                                             \
	}

/* Decode a response of the given lower/upper case name using the buffer that
 * was written by the gateway. */
#define DECODE(RESPONSE, LOWER)                                        \
	{                                                              \
		int rc2;                                               \
		rc2 = response_##LOWER##__decode(f->cursor, RESPONSE); \
		munit_assert_int(rc2, ==, 0);                          \
	}

/* Decode a row with N columns filling the given values. */
#define DECODE_ROW(N, VALUES)                                                  \
	{                                                                      \
		struct tuple_decoder decoder;                                  \
		int i2;                                                        \
		int rc2;                                                       \
		rc2 = tuple_decoder__init(&decoder, N, TUPLE__ROW, f->cursor); \
		munit_assert_int(rc2, ==, 0);                                  \
		for (i2 = 0; i2 < N; i2++) {                                   \
			rc2 = tuple_decoder__next(&decoder, &((VALUES)[i2]));  \
			munit_assert_int(rc2, ==, 0);                          \
		}                                                              \
	}

#define HANDLE_SCHEMA_STATUS(TYPE, SCHEMA, RC)                             \
	{                                                                  \
		int rc2;                                                   \
		f->handle->cursor.p = buffer__cursor(f->buf1, 0);          \
		f->handle->cursor.cap = buffer__offset(f->buf1);           \
		buffer__reset(f->buf2);                                    \
		f->context->invoked = false;                               \
		f->context->resume = false;                               \
		f->context->status = -1;                                   \
		f->context->type = -1;                                     \
		rc2 = gateway__handle(f->gateway, f->handle, TYPE, SCHEMA, \
				      f->buf2, handleCb);                  \
		munit_assert_int(rc2, ==, RC);                             \
	}

/* Handle a request of the given type and check for the given return code. */
#define HANDLE_STATUS(TYPE, RC) HANDLE_SCHEMA_STATUS(TYPE, 0, RC)

/* Handle a request of the given type and check that no error occurs. */
#define HANDLE(TYPE) HANDLE_STATUS(DQLITE_REQUEST_##TYPE, 0)

#define CONNECT(i) gateway__init(f->gateway, CLUSTER_CONFIG(i), CLUSTER_REGISTRY(i), CLUSTER_RAFT(i))

/* Open a leader connection against the "test" database */
#define OPEN                              \
	{                                 \
		struct request_open open; \
		open.filename = "test";   \
		open.vfs = "";            \
		ENCODE(&open, open);      \
		HANDLE(OPEN);             \
		ASSERT_CALLBACK(0, DB);   \
	}

/* Prepare a statement. The ID will be saved in stmt_id. */
#define PREPARE(SQL)                            \
	{                                       \
		struct request_prepare prepare; \
		struct response_stmt stmt;      \
		prepare.db_id = 0;              \
		prepare.sql = SQL;              \
		ENCODE(&prepare, prepare);      \
		HANDLE(PREPARE);                \
		WAIT;                           \
		ASSERT_CALLBACK(0, STMT);       \
		DECODE(&stmt, stmt);            \
		stmt_id = stmt.id;              \
	}

/* Finalize the statement with the given ID. */
#define FINALIZE(STMT_ID)                         \
	{                                         \
		struct request_finalize finalize; \
		finalize.db_id = 0;               \
		finalize.stmt_id = STMT_ID;       \
		ENCODE(&finalize, finalize);      \
		HANDLE(FINALIZE);                 \
		ASSERT_CALLBACK(0, EMPTY);        \
	}

/* Submit a request to execute the given statement. */
#define EXEC_SUBMIT(STMT_ID)              \
	{                                 \
		struct request_exec exec; \
		exec.db_id = 0;           \
		exec.stmt_id = STMT_ID;   \
		ENCODE(&exec, exec);      \
		HANDLE(EXEC);             \
	}

/* Submit a request to execute the given statement. */
#define EXEC_SQL_SUBMIT(SQL)                      \
	{                                         \
		struct request_exec_sql exec_sql; \
		exec_sql.db_id = 0;               \
		exec_sql.sql = SQL;               \
		ENCODE(&exec_sql, exec_sql);      \
		HANDLE(EXEC_SQL);                 \
	}

/* Submit a request to execute the given statement. */
#define QUERY_SQL_SUBMIT(SQL)                       \
	{                                           \
		struct request_query_sql query_sql; \
		query_sql.db_id = 0;                \
		query_sql.sql = SQL;                \
		ENCODE(&query_sql, query_sql);      \
		HANDLE(QUERY_SQL);                  \
	}

/* Wait for the last request to complete */
#define WAIT                                            \
	{                                               \
		unsigned _i;                            \
		for (_i = 0; _i < 60; _i++) {           \
			CLUSTER_STEP;                   \
			if (f->context->invoked) {      \
				break;                  \
			}                               \
		}                                       \
		munit_assert_true(f->context->invoked); \
	}

/* Prepare and exec a statement. */
#define EXEC(SQL)                               \
	{                                       \
		uint64_t _stmt_id;              \
		struct request_prepare prepare; \
		struct response_stmt stmt;      \
		prepare.db_id = 0;              \
		prepare.sql = SQL;              \
		ENCODE(&prepare, prepare);      \
		HANDLE(PREPARE);                \
		WAIT;                           \
		ASSERT_CALLBACK(0, STMT);       \
		DECODE(&stmt, stmt);            \
		_stmt_id = stmt.id;             \
		EXEC_SUBMIT(_stmt_id);          \
		WAIT;                           \
		ASSERT_CALLBACK(0, RESULT);     \
		FINALIZE(_stmt_id);             \
	}

/* Execute a pragma statement to lowers SQLite's page cache size, in order to
 * force it to write uncommitted dirty pages to the WAL and hance trigger calls
 * to the xFrames hook with non-commit batches. */
#define LOWER_CACHE_SIZE EXEC("PRAGMA cache_size = 1")

/******************************************************************************
 *
 * Assertions.
 *
 ******************************************************************************/

/* Assert that the handle callback has been invoked with the given status and
 * response type. Also, initialize the fixture's cursor to read the response
 * buffer. */
#define ASSERT_CALLBACK(STATUS, UPPER)                                   \
	munit_assert_true(f->context->invoked);                          \
	munit_assert_int(f->context->status, ==, STATUS);                \
	munit_assert_int(f->context->type, ==, DQLITE_RESPONSE_##UPPER); \
	f->cursor->p = buffer__cursor(f->buf2, 0);                       \
	f->cursor->cap = buffer__offset(f->buf2);                        \
	buffer__reset(f->buf2);                                          \
	f->context->invoked = false;

/* Assert that the failure response generated by the gateway matches the given
 * details. */
#define ASSERT_FAILURE(CODE, MESSAGE)                                \
	{                                                            \
		struct response_failure failure;                     \
		int rc2;                                             \
		rc2 = response_failure__decode(f->cursor, &failure); \
		munit_assert_int(rc2, ==, 0);                        \
		munit_assert_int(failure.code, ==, CODE);            \
		munit_assert_string_equal(failure.message, MESSAGE); \
	}

/******************************************************************************
 *
 * leader
 *
 ******************************************************************************/

struct leader_fixture {
	FIXTURE;
	struct request_leader request;
	struct response_server response;
};

TEST_SUITE(leader);
TEST_SETUP(leader)
{
	struct leader_fixture *f = munit_malloc(sizeof *f);
	SETUP;
	return f;
}
TEST_TEAR_DOWN(leader)
{
	struct leader_fixture *f = data;
	TEAR_DOWN;
	free(f);
}

/* If the leader is not available, an empty string is returned. */
TEST_CASE(leader, not_available, NULL)
{
	struct leader_fixture *f = data;
	(void)params;
	ENCODE(&f->request, leader);
	HANDLE(LEADER);
	ASSERT_CALLBACK(0, SERVER);
	DECODE(&f->response, server);
	munit_assert_int(f->response.id, ==, 0);
	munit_assert_string_equal(f->response.address, "");
	return MUNIT_OK;
}

/* The leader is the same node serving the request. */
TEST_CASE(leader, same_node, NULL)
{
	struct leader_fixture *f = data;
	(void)params;
	CLUSTER_ELECT(0);
	ENCODE(&f->request, leader);
	HANDLE(LEADER);
	ASSERT_CALLBACK(0, SERVER);
	DECODE(&f->response, server);
	munit_assert_string_equal(f->response.address, "1");
	return MUNIT_OK;
}

/* The leader is a different node than the one serving the request. */
TEST_CASE(leader, other_node, NULL)
{
	struct leader_fixture *f = data;
	(void)params;
	CLUSTER_ELECT(1);
	ENCODE(&f->request, leader);
	HANDLE(LEADER);
	ASSERT_CALLBACK(0, SERVER);
	DECODE(&f->response, server);
	munit_assert_string_equal(f->response.address, "2");
	return MUNIT_OK;
}

/******************************************************************************
 *
 * open
 *
 ******************************************************************************/

struct open_fixture {
	FIXTURE;
	struct request_open request;
	struct response_db response;
};

TEST_SUITE(open);
TEST_SETUP(open)
{
	struct open_fixture *f = munit_malloc(sizeof *f);
	SETUP;
	return f;
}
TEST_TEAR_DOWN(open)
{
	struct open_fixture *f = data;
	TEAR_DOWN;
	free(f);
}

/* Successfully open a database connection. */
TEST_CASE(open, success, NULL)
{
	struct open_fixture *f = data;
	(void)params;
	f->request.filename = "test";
	f->request.vfs = "";
	ENCODE(&f->request, open);
	HANDLE(OPEN);
	ASSERT_CALLBACK(0, DB);
	DECODE(&f->response, db);
	munit_assert_int(f->response.id, ==, 0);
	return MUNIT_OK;
}

TEST_GROUP(open, error);

/* Attempting to open two databases on the same gateway results in an error. */
TEST_CASE(open, error, twice, NULL)
{
	struct open_fixture *f = data;
	(void)params;
	f->request.filename = "test";
	f->request.vfs = "";
	ENCODE(&f->request, open);
	HANDLE(OPEN);
	ASSERT_CALLBACK(0, DB);
	ENCODE(&f->request, open);
	HANDLE(OPEN);
	ASSERT_CALLBACK(SQLITE_BUSY, FAILURE);
	ASSERT_FAILURE(SQLITE_BUSY,
		       "a database for this connection is already open");
	return MUNIT_OK;
}

/******************************************************************************
 *
 * prepare
 *
 ******************************************************************************/

struct prepare_fixture {
	FIXTURE;
	struct request_prepare request;
	struct response_stmt response;
};

TEST_SUITE(prepare);
TEST_SETUP(prepare)
{
	struct prepare_fixture *f = munit_malloc(sizeof *f);
	SETUP;
	OPEN;
	return f;
}
TEST_TEAR_DOWN(prepare)
{
	struct prepare_fixture *f = data;
	TEAR_DOWN;
	free(f);
}

/* Successfully prepare a statement. */
TEST_CASE(prepare, success, NULL)
{
	struct prepare_fixture *f = data;
	(void)params;
	f->request.db_id = 0;
	f->request.sql = "CREATE TABLE test (n INT)";
	CLUSTER_ELECT(0);
	ENCODE(&f->request, prepare);
	HANDLE(PREPARE);
	WAIT;
	ASSERT_CALLBACK(0, STMT);
	DECODE(&f->response, stmt);
	munit_assert_int(f->response.id, ==, 0);
	return MUNIT_OK;
}

/* Prepare an empty statement. */
TEST_CASE(prepare, empty1, NULL)
{
	struct prepare_fixture *f = data;
	(void)params;
	f->request.db_id = 0;
	f->request.sql = "";
	CLUSTER_ELECT(0);
	ENCODE(&f->request, prepare);
	HANDLE(PREPARE);
	WAIT;
	ASSERT_CALLBACK(0, FAILURE);
	ASSERT_FAILURE(0, "empty statement");
	munit_assert_int(f->response.id, ==, 0);
	return MUNIT_OK;
}

/* Prepare an empty statement. */
TEST_CASE(prepare, empty2, NULL)
{
	struct prepare_fixture *f = data;
	(void)params;
	f->request.db_id = 0;
	f->request.sql = " -- This is a comment";
	CLUSTER_ELECT(0);
	ENCODE(&f->request, prepare);
	HANDLE(PREPARE);
	WAIT;
	ASSERT_CALLBACK(0, FAILURE);
	ASSERT_FAILURE(0, "empty statement");
	munit_assert_int(f->response.id, ==, 0);
	return MUNIT_OK;
}

/* Prepare an invalid statement. */
TEST_CASE(prepare, invalid, NULL)
{
	struct prepare_fixture *f = data;
	(void)params;
	f->request.db_id = 0;
	f->request.sql = "NOT SQL";
	CLUSTER_ELECT(0);
	ENCODE(&f->request, prepare);
	HANDLE(PREPARE);
	WAIT;
	ASSERT_CALLBACK(SQLITE_ERROR, FAILURE);
	ASSERT_FAILURE(SQLITE_ERROR, "near \"NOT\": syntax error");
	munit_assert_int(f->response.id, ==, 0);
	return MUNIT_OK;
}

/* Prepare a statement and close the gateway early. */
TEST_CASE(prepare, closing, NULL)
{
	struct prepare_fixture *f = data;
	(void)params;

	f->request.db_id = 0;
	f->request.sql = "CREATE TABLE test (n INT)";
	ENCODE(&f->request, prepare);
	CLUSTER_ELECT(0);
	HANDLE(PREPARE);
	return MUNIT_OK;
}

/* Submit a prepare request that triggers a failed barrier operation. */
TEST_CASE(prepare, barrier_error, NULL)
{
	struct prepare_fixture *f = data;
	(void)params;

	CLUSTER_ELECT(0);
	CLUSTER_APPLIED(2);

	/* Send a barrier which will fail. */
	struct raft_barrier faulty_barrier = {};
	raft_fixture_append_fault(&f->cluster, 0, 0);
	int rv = raft_barrier(CLUSTER_RAFT(0), &faulty_barrier, barrierCb);
	munit_assert_int(rv, ==, 0);

	f->request.db_id = 0;
	f->request.sql = "SELECT 1";
	ENCODE(&f->request, prepare);
	HANDLE_STATUS(DQLITE_REQUEST_PREPARE, RAFT_OK);
	WAIT;
	ASSERT_CALLBACK(SQLITE_IOERR, FAILURE);
	ASSERT_FAILURE(SQLITE_IOERR, "leader exec failed");
	return MUNIT_OK;
}

/* Submit a prepare request to a non-leader node. */
TEST_CASE(prepare, non_leader, NULL)
{
	struct prepare_fixture *f = data;
	(void)params;

	CLUSTER_ELECT(0);
	SELECT(1);
	f->request.db_id = 0;
	f->request.sql = "CREATE TABLE test (n INT)";
	ENCODE(&f->request, prepare);
	HANDLE(PREPARE);
	WAIT;
	ASSERT_CALLBACK(SQLITE_IOERR_NOT_LEADER, FAILURE);
	ASSERT_FAILURE(SQLITE_IOERR_NOT_LEADER, "not leader");
	return MUNIT_OK;
}

/* Try to prepare a string containing more than one statement. */
TEST_CASE(prepare, nonempty_tail, NULL)
{
	struct prepare_fixture *f = data;
	(void)params;
	f->request.db_id = 0;
	f->request.sql = "CREATE TABLE test (n INT); SELECT * FROM test";
	CLUSTER_ELECT(0);
	ENCODE(&f->request, prepare);
	HANDLE(PREPARE);
	WAIT;
	ASSERT_CALLBACK(SQLITE_ERROR, FAILURE);
	ASSERT_FAILURE(SQLITE_ERROR, "nonempty statement tail");
	return MUNIT_OK;
}

/* Try to prepare a string containing a comment after the statement. */
TEST_CASE(prepare, comment_in_tail, NULL)
{
	struct prepare_fixture *f = data;
	(void)params;
	f->request.db_id = 0;
	f->request.sql = "CREATE TABLE test (n INT); /* comment */";
	CLUSTER_ELECT(0);
	ENCODE(&f->request, prepare);
	HANDLE(PREPARE);
	WAIT;
	ASSERT_CALLBACK(0, STMT);
	return MUNIT_OK;
}

/* Try to prepare a string containing more than one statement, successfully. */
TEST_CASE(prepare, nonempty_tail_v1, NULL)
{
	struct prepare_fixture *f = data;
	struct response_stmt_with_offset response = { 0 };
	struct request_exec exec = { 0 };
	uint64_t offset;
	int rc;

	(void)params;
	f->request.db_id = 0;
	f->request.sql = "CREATE TABLE test (n INT); SELECT * FROM test";
	CLUSTER_ELECT(0);
	ENCODE(&f->request, prepare);
	HANDLE_SCHEMA_STATUS(DQLITE_REQUEST_PREPARE, 1, 0);
	WAIT;
	ASSERT_CALLBACK(0, STMT_WITH_OFFSET);
	DECODE(&response, stmt_with_offset);
	munit_assert_int(response.id, ==, 0);
	munit_assert_uint64(response.offset, ==, 26);
	offset = response.offset;

	ENCODE(&exec, exec);
	f->handle->cursor.p = buffer__cursor(f->buf1, 0);
	f->handle->cursor.cap = buffer__offset(f->buf1);
	buffer__reset(f->buf2);
	f->context->invoked = false;
	f->context->status = -1;
	f->context->type = -1;
	rc =
	    gateway__handle(f->gateway, f->handle, DQLITE_REQUEST_EXEC,
			    DQLITE_REQUEST_PARAMS_SCHEMA_V0, f->buf2, handleCb);
	munit_assert_int(rc, ==, 0);
	WAIT;
	ASSERT_CALLBACK(0, RESULT);

	f->request.sql += offset;
	ENCODE(&f->request, prepare);
	HANDLE_SCHEMA_STATUS(DQLITE_REQUEST_PREPARE, 1, 0);
	WAIT;
	ASSERT_CALLBACK(0, STMT_WITH_OFFSET);
	DECODE(&response, stmt_with_offset);
	munit_assert_int(response.id, ==, 1);
	munit_assert_uint64(response.offset, ==, 19);

	return MUNIT_OK;
}

/******************************************************************************
 *
 * exec
 *
 ******************************************************************************/

struct exec_fixture {
	FIXTURE;
	struct request_exec request;
	struct response_result response;
};

TEST_SUITE(exec);
TEST_SETUP(exec)
{
	struct exec_fixture *f = munit_malloc(sizeof *f);
	SETUP;
	OPEN;
	return f;
}
TEST_TEAR_DOWN(exec)
{
	struct exec_fixture *f = data;
	TEAR_DOWN;
	free(f);
}

/* Successfully execute a simple statement with no parameters. */
TEST_CASE(exec, simple, NULL)
{
	struct exec_fixture *f = data;
	uint64_t stmt_id;
	(void)params;
	CLUSTER_ELECT(0);
	PREPARE("CREATE TABLE test (n INT)");
	f->request.db_id = 0;
	f->request.stmt_id = stmt_id;
	ENCODE(&f->request, exec);
	HANDLE(EXEC);
	WAIT;
	ASSERT_CALLBACK(0, RESULT);
	DECODE(&f->response, result);
	munit_assert_int(f->response.last_insert_id, ==, 0);
	munit_assert_int(f->response.rows_affected, ==, 0);
	return MUNIT_OK;
}

/* Successfully execute a statement with a one parameter. */
TEST_CASE(exec, one_param, NULL)
{
	struct exec_fixture *f = data;
	struct value value;
	uint64_t stmt_id;
	(void)params;
	CLUSTER_ELECT(0);

	/* Create the test table */
	EXEC("CREATE TABLE test (n INT)");

	/* Insert a row with one parameter */
	PREPARE("INSERT INTO test VALUES (?)");
	f->request.stmt_id = stmt_id;
	ENCODE(&f->request, exec);
	value.type = SQLITE_INTEGER;
	value.integer = 7;
	ENCODE_PARAMS(1, &value, TUPLE__PARAMS);
	HANDLE(EXEC);
	WAIT;
	ASSERT_CALLBACK(0, RESULT);
	DECODE(&f->response, result);
	munit_assert_int(f->response.last_insert_id, ==, 1);
	munit_assert_int(f->response.rows_affected, ==, 1);

	return MUNIT_OK;
}

/* Successfully execute a statement with a blob parameter. */
TEST_CASE(exec, blob, NULL)
{
	struct exec_fixture *f = data;
	struct request_query query;
	struct value value;
	char buf[8] = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h' };
	uint64_t stmt_id;
	uint64_t n;
	const char *column;
	(void)params;
	CLUSTER_ELECT(0);

	/* Create the test table */
	EXEC("CREATE TABLE test (data BLOB)");

	/* Insert a row with one parameter */
	PREPARE("INSERT INTO test VALUES (?)");
	f->request.stmt_id = stmt_id;
	ENCODE(&f->request, exec);
	value.type = SQLITE_BLOB;
	value.blob.base = buf;
	value.blob.len = sizeof buf;
	ENCODE_PARAMS(1, &value, TUPLE__PARAMS);
	HANDLE(EXEC);
	WAIT;
	ASSERT_CALLBACK(0, RESULT);
	DECODE(&f->response, result);
	munit_assert_int(f->response.last_insert_id, ==, 1);
	munit_assert_int(f->response.rows_affected, ==, 1);

	PREPARE("SELECT data FROM test");
	query.db_id = 0;
	query.stmt_id = stmt_id;
	ENCODE(&query, query);
	HANDLE(QUERY);
	ASSERT_CALLBACK(0, ROWS);

	uint64__decode(f->cursor, &n);
	munit_assert_int(n, ==, 1);
	text__decode(f->cursor, &column);
	munit_assert_string_equal(column, "data");
	DECODE_ROW(1, &value);
	munit_assert_int(value.type, ==, SQLITE_BLOB);
	munit_assert_int(value.blob.len, ==, sizeof buf);
	munit_assert_int(value.blob.base[0], ==, 'a');
	munit_assert_int(value.blob.base[7], ==, 'h');

	return MUNIT_OK;
}

/* The server is not the leader anymore when the first frames hook for a
 * non-commit frames batch fires. The same leader gets re-elected. */
TEST_CASE(exec, frames_not_leader_1st_non_commit_re_elected, NULL)
{
	struct exec_fixture *f = data;
	uint64_t stmt_id;
	unsigned i;
	(void)params;
	CLUSTER_ELECT(0);

	/* Accumulate enough dirty data to fill the page cache */
	LOWER_CACHE_SIZE;
	EXEC("CREATE TABLE test (n INT)");
	EXEC("BEGIN");
	for (i = 0; i < 162; i++) {
		EXEC("INSERT INTO test(n) VALUES(1)");
	}

	/* Trigger a page cache flush to the WAL, which fails because we are not
	 * leader anymore */
	PREPARE("INSERT INTO test(n) VALUES(1)");
	CLUSTER_DEPOSE;
	EXEC_SUBMIT(stmt_id);
	ASSERT_CALLBACK(SQLITE_IOERR_NOT_LEADER, FAILURE);
	ASSERT_FAILURE(SQLITE_IOERR_NOT_LEADER, "not leader");

	/* Re-elect ourselves and re-try */
	CLUSTER_ELECT(0);
	EXEC("INSERT INTO test(n) VALUES(1)");

	return MUNIT_OK;
}

/* The server is not the leader anymore when the first frames hook for a
 * non-commit frames batch fires. Another leader gets re-elected. */
TEST_CASE(exec, frames_not_leader_1st_non_commit_other_elected, NULL)
{
	struct exec_fixture *f = data;
	uint64_t stmt_id;
	unsigned i;
	(void)params;
	CLUSTER_ELECT(0);

	/* Accumulate enough dirty data to fill the page cache */
	LOWER_CACHE_SIZE;
	EXEC("CREATE TABLE test (n INT)");
	EXEC("BEGIN");
	for (i = 0; i < 162; i++) {
		EXEC("INSERT INTO test(n) VALUES(1)");
	}

	/* Trigger a page cache flush to the WAL, which fails because we are not
	 * leader anymore */
	PREPARE("INSERT INTO test(n) VALUES(1)");
	CLUSTER_DEPOSE;
	EXEC_SUBMIT(stmt_id);
	ASSERT_CALLBACK(SQLITE_IOERR_NOT_LEADER, FAILURE);
	ASSERT_FAILURE(SQLITE_IOERR_NOT_LEADER, "not leader");

	/* Elect another leader and re-try */
	CLUSTER_ELECT(1);
	SELECT(1);
	OPEN;
	EXEC("INSERT INTO test(n) VALUES(1)");

	return MUNIT_OK;
}

/* The server is not the leader anymore when the second frames hook for a
 * non-commit frames batch fires. The same leader gets re-elected. */
TEST_CASE(exec, frames_not_leader_2nd_non_commit_re_elected, NULL)
{
	struct exec_fixture *f = data;
	uint64_t stmt_id;
	unsigned i;
	(void)params;
	CLUSTER_ELECT(0);

	/* Accumulate enough dirty data to fill the page cache a first time,
	 * flush it and then fill it a second time. */
	LOWER_CACHE_SIZE;
	EXEC("CREATE TABLE test (n INT)");
	EXEC("BEGIN");
	for (i = 0; i < 234; i++) {
		EXEC("INSERT INTO test(n) VALUES(1)");
	}

	/* Trigger a second page cache flush to the WAL, which fails because we
	 * are not leader anymore */
	PREPARE("INSERT INTO test(n) VALUES(1)");
	CLUSTER_DEPOSE;
	EXEC_SUBMIT(stmt_id);
	ASSERT_CALLBACK(SQLITE_IOERR_NOT_LEADER, FAILURE);
	ASSERT_FAILURE(SQLITE_IOERR_NOT_LEADER, "not leader");

	/* Re-elect ourselves and re-try */
	CLUSTER_ELECT(0);
	EXEC("INSERT INTO test(n) VALUES(1)");

	return MUNIT_OK;
}

/* The gateway is closed while a raft commit is in flight. */
TEST_CASE(exec, close_while_in_flight, NULL)
{
	struct exec_fixture *f = data;
	unsigned i;
	(void)params;
	CLUSTER_ELECT(0);

	/* Accumulate enough dirty data to fill the page cache and trigger
	 * an apply request. */
	LOWER_CACHE_SIZE;
	EXEC("CREATE TABLE test (n INT)");
	EXEC("BEGIN");
	for (i = 0; i < 162; i++) {
		EXEC("INSERT INTO test(n) VALUES(1)");
	}


	/* Trigger a second page cache flush to the WAL, and abort before it's
	 * done. */
	EXEC_SQL_SUBMIT("INSERT INTO test(n) VALUES(1)");
	return MUNIT_OK;
}

/* The server is not the leader anymore when the second frames hook for a
 * non-commit frames batch fires. Another leader gets elected. */
TEST_CASE(exec, frames_not_leader_2nd_non_commit_other_elected, NULL)
{
	struct exec_fixture *f = data;
	uint64_t stmt_id;
	unsigned i;
	(void)params;
	CLUSTER_ELECT(0);

	/* Accumulate enough dirty data to fill the page cache a first time,
	 * flush it and then fill it a second time. */
	LOWER_CACHE_SIZE;
	EXEC("CREATE TABLE test (n INT)");
	EXEC("BEGIN");
	for (i = 0; i < 234; i++) {
		EXEC("INSERT INTO test(n) VALUES(1)");
	}

	/* Trigger a second page cache flush to the WAL, which fails because we
	 * are not leader anymore */
	PREPARE("INSERT INTO test(n) VALUES(1)");
	CLUSTER_DEPOSE;
	EXEC_SUBMIT(stmt_id);
	ASSERT_CALLBACK(SQLITE_IOERR_NOT_LEADER, FAILURE);

	/* Elect another leader and re-try */
	CLUSTER_ELECT(1);
	SELECT(1);
	OPEN;
	EXEC("INSERT INTO test(n) VALUES(1)");

	return MUNIT_OK;
}

/* The server loses leadership after trying to apply the first Frames command
 * for a non-commit frames batch. The same leader gets re-elected. */
TEST_CASE(exec, frames_leadership_lost_1st_non_commit_re_elected, NULL)
{
	struct exec_fixture *f = data;
	uint64_t stmt_id;
	unsigned i;
	(void)params;
	CLUSTER_ELECT(0);

	/* Accumulate enough dirty data to fill the page cache */
	LOWER_CACHE_SIZE;
	EXEC("CREATE TABLE test (n INT)");
	EXEC("BEGIN");
	for (i = 0; i < 162; i++) {
		EXEC("INSERT INTO test(n) VALUES(1)");
	}

	/* Trigger a page cache flush to the WAL */
	EXEC("INSERT INTO test(n) VALUES(1)");

	/* Try to commit */
	PREPARE("COMMIT");
	EXEC_SUBMIT(stmt_id);
	CLUSTER_DEPOSE;
	ASSERT_CALLBACK(SQLITE_IOERR_LEADERSHIP_LOST, FAILURE);
	ASSERT_FAILURE(SQLITE_IOERR_LEADERSHIP_LOST, "leadership lost");

	/* Re-elect ourselves and re-try */
	CLUSTER_ELECT(0);
	EXEC("INSERT INTO test(n) VALUES(1)");

	return MUNIT_OK;
}

/* The server is not the leader anymore when the undo hook for a writing
 * transaction fires. The same leader gets re-elected. */
TEST_CASE(exec, undo_not_leader_pending_re_elected, NULL)
{
	struct exec_fixture *f = data;
	uint64_t stmt_id;
	unsigned i;
	(void)params;
	CLUSTER_ELECT(0);

	/* Accumulate enough dirty data to fill the page cache a first time */
	LOWER_CACHE_SIZE;
	EXEC("CREATE TABLE test (n INT)");
	EXEC("BEGIN");
	for (i = 0; i < 163; i++) {
		EXEC("INSERT INTO test(n) VALUES(1)");
	}

	/* Trying to rollback fails because we are not leader anymore */
	PREPARE("ROLLBACK");
	CLUSTER_DEPOSE;
	EXEC_SUBMIT(stmt_id);
	ASSERT_CALLBACK(SQLITE_IOERR_NOT_LEADER, FAILURE);
	ASSERT_FAILURE(SQLITE_IOERR_NOT_LEADER, "not leader");

	/* Re-elect ourselves and re-try */
	CLUSTER_ELECT(0);
	EXEC("INSERT INTO test(n) VALUES(1)");

	return MUNIT_OK;
}

/* The server is not the leader anymore when the undo hook for a writing
 * transaction fires. Another leader gets elected. */
TEST_CASE(exec, undo_not_leader_pending_other_elected, NULL)
{
	struct exec_fixture *f = data;
	uint64_t stmt_id;
	unsigned i;
	(void)params;
	CLUSTER_ELECT(0);

	/* Accumulate enough dirty data to fill the page cache a first time */
	LOWER_CACHE_SIZE;
	EXEC("CREATE TABLE test (n INT)");
	EXEC("BEGIN");
	for (i = 0; i < 163; i++) {
		EXEC("INSERT INTO test(n) VALUES(1)");
	}

	/* Trying to rollback fails because we are not leader anymore */
	PREPARE("ROLLBACK");
	CLUSTER_DEPOSE;
	EXEC_SUBMIT(stmt_id);
	ASSERT_CALLBACK(SQLITE_IOERR_NOT_LEADER, FAILURE);
	ASSERT_FAILURE(SQLITE_IOERR_NOT_LEADER, "not leader");

	/* Re-elect ourselves and re-try */
	CLUSTER_ELECT(1);
	SELECT(1);
	OPEN;
	EXEC("INSERT INTO test(n) VALUES(1)");

	return MUNIT_OK;
}

/* Succesfully vacuum a database */
TEST_CASE(exec, vacuum, NULL)
{
	struct exec_fixture *f = data;
	uint64_t stmt_id;
	(void)params;
	CLUSTER_ELECT(0);

	/* Create some free pages */
	LOWER_CACHE_SIZE;
	EXEC("CREATE TABLE test (n INTEGER PRIMARY KEY NOT NULL)");
	EXEC("WITH RECURSIVE seq(n) AS ("
		"    SELECT 1 UNION ALL     "
		"    SELECT n+1 FROM seq    "
		"    WHERE  n < 10000       "
		")                          "
		"INSERT INTO test(n)        "
		"SELECT n FROM seq          "
	)
	EXEC("DELETE FROM test WHERE n <= 5000");

	PREPARE("VACUUM");
	f->request.db_id = 0;
	f->request.stmt_id = stmt_id;
	ENCODE(&f->request, exec);
	HANDLE(EXEC);
	WAIT;
	ASSERT_CALLBACK(0, RESULT);
	DECODE(&f->response, result);
	munit_assert_int(f->response.last_insert_id, ==, 10000);
	munit_assert_int(f->response.rows_affected, ==,   5000);

	/* Make sure that the freelist is now empty. */
	struct value value;
	uint64_t n;
	text_t column;
	QUERY_SQL_SUBMIT("PRAGMA freelist_count");
	WAIT;
	ASSERT_CALLBACK(0, ROWS);
	uint64__decode(f->cursor, &n);
	munit_assert_int(n, ==, 1);
	text__decode(f->cursor, &column);
	munit_assert_string_equal(column, "freelist_count");
	DECODE_ROW(1, &value);
	munit_assert_int(value.type, ==, SQLITE_INTEGER);
	munit_assert_int(value.integer, ==, 0);
	
	return MUNIT_OK;
}


/* Succesfully vacuum a database */
TEST_CASE(exec, vacuum_variants, NULL)
{
	struct exec_fixture *f = data;
	uint64_t stmt_id;
	(void)params;
	CLUSTER_ELECT(0);

	LOWER_CACHE_SIZE;
	EXEC("CREATE TABLE test (n INTEGER PRIMARY KEY NOT NULL)");
	EXEC("WITH RECURSIVE seq(n) AS ("
		"    SELECT 1 UNION ALL     "
		"    SELECT n+1 FROM seq    "
		"    WHERE  n < 10000       "
		")                          "
		"INSERT INTO test(n)        "
		"SELECT n FROM seq          "
	)

	const char* vacuum_variants[] = {
		" VACUUM \r\t\n ",
		" VACUUM \r\t\n main \t\n",
		" VACUUM \r\t\n 'main' \t\n",
		" VACUUM \r\t\n \"main\" \t\n",
	};
	for (size_t i = 0; i < sizeof(vacuum_variants) / sizeof(vacuum_variants[0]); i++) {
		/* Create some free pages */
		EXEC("DELETE FROM test WHERE n < (SELECT MIN(n) + 1000 FROM test)");
		PREPARE(vacuum_variants[i]);
		f->request.db_id = 0;
		f->request.stmt_id = stmt_id;
		ENCODE(&f->request, exec);
		HANDLE(EXEC);
		WAIT;
		ASSERT_CALLBACK(0, RESULT);
		DECODE(&f->response, result);
		munit_assert_int(f->response.last_insert_id, ==, 10000);
		munit_assert_int(f->response.rows_affected, ==,   1000);
	}
	return MUNIT_OK;
}

/* Fail to vacuum into a file */
TEST_CASE(exec, vacuum_into_fails, NULL)
{
	struct exec_fixture *f = data;
	uint64_t stmt_id;
	(void)params;
	CLUSTER_ELECT(0);

	/* Create some free pages */
	LOWER_CACHE_SIZE;
	EXEC("CREATE TABLE test (n INTEGER PRIMARY KEY NOT NULL)");
	EXEC("WITH RECURSIVE seq(n) AS ("
		"    SELECT 1 UNION ALL     "
		"    SELECT n+1 FROM seq    "
		"    WHERE  n < 10000       "
		")                          "
		"INSERT INTO test(n)        "
		"SELECT n FROM seq          "
	)
	EXEC("DELETE FROM test WHERE n <= 5000");

	PREPARE("VACUUM INTO 'should_fail'");
	f->request.db_id = 0;
	f->request.stmt_id = stmt_id;
	ENCODE(&f->request, exec);
	HANDLE(EXEC);
	WAIT;
	ASSERT_CALLBACK(SQLITE_AUTH, FAILURE);
	return MUNIT_OK;
}

/* A follower remains behind and needs to restore state from a snapshot. */
TEST_CASE(exec, restore, NULL)
{
	struct exec_fixture *f = data;
	uint64_t stmt_id;
	struct request_query request;
	struct response_rows response;
	struct value value;
	uint64_t n;
	const char *column;
	(void)params;
	CLUSTER_SNAPSHOT_THRESHOLD(0, 5);
	CLUSTER_SNAPSHOT_TRAILING(0, 2);
	CLUSTER_ELECT(0);
	CLUSTER_DISCONNECT(0, 1);
	EXEC("CREATE TABLE test (n INT)");
	EXEC("INSERT INTO test(n) VALUES(1)");
	EXEC("INSERT INTO test(n) VALUES(2)");
	CLUSTER_RECONNECT(0, 1);
	CLUSTER_APPLIED(4);

	/* TODO: the query below fails because we can exec queries only against
	 * the leader. */
	return MUNIT_SKIP;

	/* The follower contains the expected rows. */
	SELECT(1);
	OPEN;
	PREPARE("SELECT n FROM test");
	request.db_id = 0;
	request.stmt_id = stmt_id;
	ENCODE(&request, query);
	HANDLE(QUERY);
	ASSERT_CALLBACK(0, ROWS);
	uint64__decode(f->cursor, &n);
	munit_assert_int(n, ==, 1);
	text__decode(f->cursor, &column);
	munit_assert_string_equal(column, "n");
	DECODE_ROW(1, &value);
	munit_assert_int(value.type, ==, SQLITE_INTEGER);
	munit_assert_int(value.integer, ==, 1);
	DECODE_ROW(1, &value);
	munit_assert_int(value.type, ==, SQLITE_INTEGER);
	munit_assert_int(value.integer, ==, 2);
	DECODE(&response, rows);
	munit_assert_ullong(response.eof, ==, DQLITE_RESPONSE_ROWS_DONE);
	return MUNIT_OK;
}


/* Submit a prepare request that triggers a failed barrier operation. */
TEST_CASE(exec, barrier_error, NULL)
{
	struct exec_fixture *f = data;
	uint64_t stmt_id;
	(void)params;
	CLUSTER_ELECT(0);
	CLUSTER_APPLIED(2);

	PREPARE("CREATE TABLE test (n INT)");

	/* Send a barrier which will fail. */
	struct raft_barrier faulty_barrier = {};
	raft_fixture_append_fault(&f->cluster, 0, 0);
	int rv = raft_barrier(CLUSTER_RAFT(0), &faulty_barrier, barrierCb);
	munit_assert_int(rv, ==, 0);

	f->request.db_id = 0;
	f->request.stmt_id = stmt_id;
	ENCODE(&f->request, exec);
	HANDLE_STATUS(DQLITE_REQUEST_EXEC, RAFT_OK);
	WAIT;
	ASSERT_CALLBACK(SQLITE_IOERR, FAILURE);
	ASSERT_FAILURE(SQLITE_IOERR, "leader exec failed");

	return MUNIT_OK;
}

/* Close the gateway early while an exec barrier is in flight. */
TEST_CASE(exec, barrier_closing, NULL)
{
	struct exec_fixture *f = data;
	uint64_t stmt_id, prev_stmt_id;
	(void)params;

	CLUSTER_ELECT(0);
	EXEC("CREATE TABLE test (n INT)");

	/* Save this stmt to exec later */
	PREPARE("INSERT INTO test(n) VALUES(2)");
	prev_stmt_id = stmt_id;

	/* Submit exec request, then depose the leader before it commits */
	PREPARE("INSERT INTO test(n) VALUES(1)");
	EXEC_SUBMIT(stmt_id);
	CLUSTER_DEPOSE;
	ASSERT_CALLBACK(SQLITE_IOERR_LEADERSHIP_LOST, FAILURE);

	/* Now try to exec the other stmt (triggering a barrier) and close early
	 */
	CLUSTER_ELECT(0);
	EXEC_SUBMIT(prev_stmt_id);
	return MUNIT_OK;
}

/* Send an exec request in the new (schema version 1) format, which
 * supports larger numbers of parameters. */
TEST_CASE(exec, manyParams, NULL)
{
	struct exec_fixture *f = data;
	uint64_t stmt_id;
	size_t len = 20000;
	char *sql = munit_malloc(len);
	size_t pos;
	size_t i;
	size_t num_exec_params = 999;
	struct value *values = munit_calloc(num_exec_params, sizeof(*values));
	(void)params;

	pos = snprintf(sql, len, "DELETE FROM test WHERE n = ?");
	for (i = 1; i < num_exec_params; i++) {
		pos += snprintf(sql + pos, len - pos, " OR n = ?");
	}

	for (i = 0; i < num_exec_params; i++) {
		values[i].type = SQLITE_INTEGER;
		values[i].integer = i;
	}

	CLUSTER_ELECT(0);
	EXEC("CREATE TABLE test (n INT)");
	PREPARE(sql);
	f->request.db_id = 0;
	f->request.stmt_id = stmt_id;
	ENCODE(&f->request, exec);
	ENCODE_PARAMS(num_exec_params, values, TUPLE__PARAMS32);
	HANDLE_SCHEMA_STATUS(DQLITE_REQUEST_EXEC, 1, 0);
	WAIT;
	ASSERT_CALLBACK(0, RESULT);

	FINALIZE(stmt_id);
	free(values);
	free(sql);
	return MUNIT_OK;
}

TEST_CASE(exec, unexpectedRow, NULL)
{
	struct exec_fixture *f = data;
	uint64_t stmt_id;
	(void)params;
	CLUSTER_ELECT(0);
	PREPARE("SELECT * FROM (VALUES (1), (2))");
	f->request.db_id = 0;
	f->request.stmt_id = stmt_id;
	ENCODE(&f->request, exec);
	HANDLE(EXEC);
	WAIT;
	ASSERT_CALLBACK(SQLITE_ERROR, FAILURE);
	ASSERT_FAILURE(SQLITE_ERROR,
		       "rows yielded when none expected for EXEC request");
	return MUNIT_OK;
}

TEST_CASE(exec, malformed_parameters, NULL)
{
	struct exec_fixture *f = data;
	uint64_t stmt_id;
	(void)params;
	CLUSTER_ELECT(0);
	EXEC("CREATE TABLE test(id CHECK (id <> 2))");

	struct value param = {
		.type = SQLITE_INTEGER,
		.integer = 2,
	};
	PREPARE("INSERT INTO test VALUES (?)");
	f->request.db_id = 0;
	f->request.stmt_id = stmt_id;
	ENCODE(&f->request, exec);
	ENCODE_PARAMS(1, &param, TUPLE__PARAMS);
	HANDLE(EXEC);
	WAIT;
	ASSERT_CALLBACK(SQLITE_CONSTRAINT_CHECK, FAILURE);
	ASSERT_FAILURE(SQLITE_CONSTRAINT_CHECK,
		       "CHECK constraint failed: id <> 2");

	ENCODE(&f->request, exec);
	char *cursor = buffer__advance(f->buf1, 8);
	cursor[0] = 1;
	cursor[1] = 0xff;
	HANDLE(EXEC);
	WAIT;
	ASSERT_CALLBACK(SQLITE_ERROR, FAILURE);
	ASSERT_FAILURE(SQLITE_ERROR, "bind parameters");

	return MUNIT_OK;
}


/******************************************************************************
 *
 * query
 *
 ******************************************************************************/

struct query_fixture {
	FIXTURE;
	struct request_query request;
	struct response_rows response;
};

TEST_SUITE(query);
TEST_SETUP(query)
{
	struct query_fixture *f = munit_malloc(sizeof *f);
	SETUP;
	OPEN;
	CLUSTER_ELECT(0);
	EXEC("CREATE TABLE test (n INT, data BLOB)");
	return f;
}
TEST_TEAR_DOWN(query)
{
	struct query_fixture *f = data;
	TEAR_DOWN;
	free(f);
}

/* Successfully query a simple statement with no parameters and yielding no
 * rows. */
TEST_CASE(query, simple, NULL)
{
	struct query_fixture *f = data;
	uint64_t stmt_id;
	uint64_t n;
	const char *column;
	(void)params;
	PREPARE("SELECT n FROM test");
	f->request.db_id = 0;
	f->request.stmt_id = stmt_id;
	ENCODE(&f->request, query);
	HANDLE(QUERY);
	ASSERT_CALLBACK(0, ROWS);
	uint64__decode(f->cursor, &n);
	munit_assert_int(n, ==, 1);
	text__decode(f->cursor, &column);
	munit_assert_string_equal(column, "n");
	DECODE(&f->response, rows);
	munit_assert_ullong(f->response.eof, ==, DQLITE_RESPONSE_ROWS_DONE);

	return MUNIT_OK;
}

/* Successfully query a simple statement with no parameters yielding one row. */
TEST_CASE(query, one_row, NULL)
{
	struct query_fixture *f = data;
	uint64_t stmt_id;
	uint64_t n;
	const char *column;
	struct value value;
	(void)params;
	EXEC("INSERT INTO test(n) VALUES(666)");

	PREPARE("SELECT n FROM test");
	f->request.db_id = 0;
	f->request.stmt_id = stmt_id;
	ENCODE(&f->request, query);
	HANDLE(QUERY);
	ASSERT_CALLBACK(0, ROWS);

	uint64__decode(f->cursor, &n);
	munit_assert_int(n, ==, 1);
	text__decode(f->cursor, &column);
	munit_assert_string_equal(column, "n");
	DECODE_ROW(1, &value);
	munit_assert_int(value.type, ==, SQLITE_INTEGER);
	munit_assert_int(value.integer, ==, 666);
	DECODE(&f->response, rows);
	munit_assert_ullong(f->response.eof, ==, DQLITE_RESPONSE_ROWS_DONE);

	return MUNIT_OK;
}

/* Calculate max amount of rows that can fit in 1 response buffer.
 * A response buffer has _SC_PAGESIZE size.
 * A response consists of n tuples each row_sz in size
 * and an 8B EOF marker. */
static unsigned max_rows_buffer(unsigned tuple_row_sz)
{
	unsigned buf_sz = sysconf(_SC_PAGESIZE);
	unsigned eof_sz = 8;
	return (buf_sz - eof_sz) / tuple_row_sz;
}

/* Successfully query that yields a large number of rows that need to be split
 * into several reponses. */
TEST_CASE(query, large, NULL)
{
	struct query_fixture *f = data;
	unsigned i;
	uint64_t stmt_id;
	uint64_t n;
	const char *column;
	struct value value;
	bool finished;
	(void)params;

	/* 16 = 8B header + 8B value (int) */
	unsigned n_rows_buffer = max_rows_buffer(16);
	struct value n_rows = { .type = SQLITE_INTEGER, .integer = n_rows_buffer*2-1 };
	PREPARE("WITH RECURSIVE seq(n) AS ("
            "	SELECT 1               "
            "	UNION ALL              "
            "	SELECT n+1             "
            "	FROM seq WHERE n < ?   "
            ")                         "
            "SELECT * FROM seq         ");
	f->request.db_id = 0;
	f->request.stmt_id = stmt_id;
	ENCODE(&f->request, query);
	ENCODE_PARAMS(1, &n_rows, TUPLE__PARAMS);
	HANDLE(QUERY);
	ASSERT_CALLBACK(0, ROWS);

	uint64__decode(f->cursor, &n);
	munit_assert_int(n, ==, 1);
	text__decode(f->cursor, &column);
	munit_assert_string_equal(column, "n");

	/* First response contains max amount of rows */
	for (i = 1; i <= n_rows_buffer; i++) {
		DECODE_ROW(1, &value);
		munit_assert_int(value.type, ==, SQLITE_INTEGER);
		munit_assert_int(value.integer, ==, i);
	}

	DECODE(&f->response, rows);
	munit_assert_ullong(f->response.eof, ==, DQLITE_RESPONSE_ROWS_PART);

	gateway__resume(f->gateway, &finished);
	munit_assert_false(finished);

	ASSERT_CALLBACK(0, ROWS);

	uint64__decode(f->cursor, &n);
	munit_assert_int(n, ==, 1);
	text__decode(f->cursor, &column);
	munit_assert_string_equal(column, "n");

	/* Second, and last, response contains 1 less than maximum amount */
	for (; i <= n_rows_buffer*2 - 1; i++) {
		DECODE_ROW(1, &value);
		munit_assert_int(value.type, ==, SQLITE_INTEGER);
		munit_assert_int(value.integer, ==, i);
	}

	DECODE(&f->response, rows);
	munit_assert_ullong(f->response.eof, ==, DQLITE_RESPONSE_ROWS_DONE);

	gateway__resume(f->gateway, &finished);
	munit_assert_true(finished);
	return MUNIT_OK;
}

TEST_CASE(query, modifying, NULL)
{
	struct query_fixture *f = data;
	uint64_t stmt_id;
	uint64_t n;
	bool finished;
	(void)params;

	PREPARE("INSERT INTO test(n) VALUES(123)");
	f->request.db_id = 0;
	f->request.stmt_id = stmt_id;
	ENCODE(&f->request, query);
	HANDLE(QUERY);
	WAIT;
	ASSERT_CALLBACK(0, ROWS);

	uint64__decode(f->cursor, &n);
	munit_assert_int(n, ==, 0);
	DECODE(&f->response, rows);
	munit_assert_ullong(f->response.eof, ==, DQLITE_RESPONSE_ROWS_DONE);

	gateway__resume(f->gateway, &finished);
	munit_assert_true(finished);
	return MUNIT_OK;
}

TEST_CASE(query, returning, NULL)
{
	struct query_fixture *f = data;
	uint64_t stmt_id;
	uint64_t n;
	const char *column;
	struct value value;
	bool finished;
	(void)params;

	PREPARE("INSERT INTO test(n) VALUES(123) RETURNING n");
	f->request.db_id = 0;
	f->request.stmt_id = stmt_id;
	ENCODE(&f->request, query);
	HANDLE(QUERY);
	WAIT;
	ASSERT_CALLBACK(0, ROWS);

	uint64__decode(f->cursor, &n);
	munit_assert_int(n, ==, 1);
	text__decode(f->cursor, &column);
	munit_assert_string_equal(column, "n");

	/* First response contains max amount of rows */
	DECODE_ROW(1, &value);
	munit_assert_int(value.type, ==, SQLITE_INTEGER);
	munit_assert_int(value.integer, ==, 123);

	DECODE(&f->response, rows);
	munit_assert_ullong(f->response.eof, ==, DQLITE_RESPONSE_ROWS_DONE);

	gateway__resume(f->gateway, &finished);
	munit_assert_true(finished);
	return MUNIT_OK;
}

TEST_CASE(query, returning_large, NULL)
{
	struct query_fixture *f = data;
	int i;
	uint64_t stmt_id;
	uint64_t n;
	const char *column;
	struct value value;
	bool finished;
	(void)params;

	/* Query 2 response buffers worth of rows */
	/* 16 = 8B header + 8B value (int) */
	int64_t n_rows_buffer = max_rows_buffer(16);
	struct value n_rows = { .type = SQLITE_INTEGER, .integer = n_rows_buffer*2-1 };

	PREPARE("WITH RECURSIVE seq(n) AS ("
            "	SELECT 1               "
            "	UNION ALL              "
            "	SELECT n+1             "
            "	FROM seq WHERE n < ?   "
            ")                         "
            "INSERT INTO test(n) SELECT n FROM seq RETURNING n");
	f->request.db_id = 0;
	f->request.stmt_id = stmt_id;
	ENCODE(&f->request, query);
	ENCODE_PARAMS(1, &n_rows, TUPLE__PARAMS);
	HANDLE(QUERY);
	WAIT;
	ASSERT_CALLBACK(0, ROWS);

	uint64__decode(f->cursor, &n);
	munit_assert_int(n, ==, 1);
	text__decode(f->cursor, &column);
	munit_assert_string_equal(column, "n");

	/* First response contains max amount of rows */
	for (i = 1; i <= n_rows_buffer; i++) {
		DECODE_ROW(1, &value);
		munit_assert_int(value.type, ==, SQLITE_INTEGER);
		munit_assert_int(value.integer, ==, i);
	}

	DECODE(&f->response, rows);
	munit_assert_ullong(f->response.eof, ==, DQLITE_RESPONSE_ROWS_PART);

	gateway__resume(f->gateway, &finished);
	munit_assert_false(finished);

	WAIT;
	ASSERT_CALLBACK(0, ROWS);

	uint64__decode(f->cursor, &n);
	munit_assert_int(n, ==, 1);
	text__decode(f->cursor, &column);
	munit_assert_string_equal(column, "n");

	/* Second, and last, response contains 1 less than maximum amount */
	for (; i < 2*n_rows_buffer; i++) {
		DECODE_ROW(1, &value);
		munit_assert_int(value.type, ==, SQLITE_INTEGER);
		munit_assert_int(value.integer, ==, i);
	}

	DECODE(&f->response, rows);
	munit_assert_ullong(f->response.eof, ==, DQLITE_RESPONSE_ROWS_DONE);

	gateway__resume(f->gateway, &finished);
	munit_assert_true(finished);
	return MUNIT_OK;
}

TEST_CASE(query, returning_interrupt, NULL)
{
	struct query_fixture *f = data;
	struct request_interrupt interrupt;
	int i;
	uint64_t stmt_id;
	uint64_t n;
	const char *column;
	struct value value;
	bool finished;
	(void)params;

	/* Query 2 response buffers worth of rows */
	/* 16 = 8B header + 8B value (int) */
	int64_t n_rows_buffer = max_rows_buffer(16);
	struct value n_rows = { .type = SQLITE_INTEGER, .integer = n_rows_buffer*2-1 };

	PREPARE("WITH RECURSIVE seq(n) AS ("
            "	SELECT 1               "
            "	UNION ALL              "
            "	SELECT n+1             "
            "	FROM seq WHERE n < ?   "
            ")                         "
            "INSERT INTO test(n) SELECT n FROM seq RETURNING n");
	f->request.db_id = 0;
	f->request.stmt_id = stmt_id;
	ENCODE(&f->request, query);
	ENCODE_PARAMS(1, &n_rows, TUPLE__PARAMS);
	HANDLE(QUERY);
	WAIT;
	ASSERT_CALLBACK(0, ROWS);

	uint64__decode(f->cursor, &n);
	munit_assert_int(n, ==, 1);
	text__decode(f->cursor, &column);
	munit_assert_string_equal(column, "n");

	/* First response contains max amount of rows */
	for (i = 1; i <= n_rows_buffer; i++) {
		DECODE_ROW(1, &value);
		munit_assert_int(value.type, ==, SQLITE_INTEGER);
		munit_assert_int(value.integer, ==, i);
	}

	DECODE(&f->response, rows);
	munit_assert_ullong(f->response.eof, ==, DQLITE_RESPONSE_ROWS_PART);

	ENCODE(&interrupt, interrupt);
	HANDLE(INTERRUPT);
	/* The interrupt should not return immediately, but wait for
	 * the query to end. */
	munit_assert_false(f->context->invoked);
	gateway__resume(f->gateway, &finished);
	munit_assert_false(finished);
	ASSERT_CALLBACK(0, EMPTY);

	/* Make sure rows are not there. */
	PREPARE("SELECT * FROM test");
	ENCODE(&f->request, query);
	HANDLE(QUERY);
	WAIT;
	ASSERT_CALLBACK(0, EMPTY);

	return MUNIT_OK;
}

/* Perform a query using a prepared statement with parameters */
TEST_CASE(query, params, NULL)
{
	struct query_fixture *f = data;
	struct value values[2];
	uint64_t stmt_id;
	(void)params;
	EXEC("BEGIN");
	EXEC("INSERT INTO test(n) VALUES(1)");
	EXEC("INSERT INTO test(n) VALUES(2)");
	EXEC("INSERT INTO test(n) VALUES(3)");
	EXEC("INSERT INTO test(n) VALUES(4)");
	EXEC("COMMIT");

	PREPARE("SELECT n FROM test WHERE n > ? AND n < ?");
	f->request.db_id = 0;
	f->request.stmt_id = stmt_id;

	ENCODE(&f->request, query);
	values[0].type = SQLITE_INTEGER;
	values[0].integer = 1;
	values[1].type = SQLITE_INTEGER;
	values[1].integer = 4;
	ENCODE_PARAMS(2, values, TUPLE__PARAMS);

	HANDLE(QUERY);
	ASSERT_CALLBACK(0, ROWS);
	return MUNIT_OK;
}

/* Interrupt a large query. */
TEST_CASE(query, interrupt, NULL)
{
	struct query_fixture *f = data;
	struct request_interrupt interrupt;
	unsigned i;
	uint64_t stmt_id;
	uint64_t n;
	const char *column;
	struct value value;
	(void)params;

	/* Query 2 response buffers worth of rows */
	/* 16 = 8B header + 8B value (int) */
	int64_t n_rows_buffer = max_rows_buffer(16);
	struct value n_rows = { .type = SQLITE_INTEGER, .integer = n_rows_buffer*2 };

	PREPARE("WITH RECURSIVE seq(n) AS ("
            "	SELECT 1               "
            "	UNION ALL              "
            "	SELECT n+1             "
            "	FROM seq WHERE n < ?   "
            ")                         "
            "SELECT * FROM seq         ");
	f->request.db_id = 0;
	f->request.stmt_id = stmt_id;
	ENCODE(&f->request, query);
	ENCODE_PARAMS(1, &n_rows, TUPLE__PARAMS);
	HANDLE(QUERY);
	ASSERT_CALLBACK(0, ROWS);

	uint64__decode(f->cursor, &n);
	munit_assert_int(n, ==, 1);
	text__decode(f->cursor, &column);
	munit_assert_string_equal(column, "n");

	for (i = 0; i < n_rows_buffer; i++) {
		DECODE_ROW(1, &value);
		munit_assert_int(value.type, ==, SQLITE_INTEGER);
		munit_assert_int(value.integer, ==, i+1);
	}

	DECODE(&f->response, rows);
	munit_assert_ullong(f->response.eof, ==, DQLITE_RESPONSE_ROWS_PART);

	ENCODE(&interrupt, interrupt);
	HANDLE(INTERRUPT);
	/* The interrupt should not return immediately, but wait for
	 * the query to end. */
	munit_assert_false(f->context->invoked);
	bool finished;
	gateway__resume(f->gateway, &finished);
	munit_assert_false(finished);
	ASSERT_CALLBACK(0, EMPTY);

	return MUNIT_OK;
}

/* Interrupt without an active query. */
TEST_CASE(query, interruptInactive, NULL)
{
	struct query_fixture *f = data;
	struct request_interrupt interrupt;
	(void)params;

	ENCODE(&interrupt, interrupt);
	HANDLE(INTERRUPT);
	ASSERT_CALLBACK(0, EMPTY);

	return MUNIT_OK;
}

/* Close the gateway during a large query. */
TEST_CASE(query, largeClose, NULL)
{
	struct query_fixture *f = data;
	unsigned i;
	uint64_t stmt_id;
	uint64_t n;
	const char *column;
	struct value value;
	(void)params;
	/* Query 2 response buffers worth of rows */
	/* 16 = 8B header + 8B value (int) */
	int64_t n_rows_buffer = max_rows_buffer(16);
	struct value n_rows = { .type = SQLITE_INTEGER, .integer = n_rows_buffer*2 };

	PREPARE("WITH RECURSIVE seq(n) AS ("
            "	SELECT 1               "
            "	UNION ALL              "
            "	SELECT n+1             "
            "	FROM seq WHERE n < ?   "
            ")                         "
            "SELECT * FROM seq         ");
	f->request.db_id = 0;
	f->request.stmt_id = stmt_id;
	ENCODE(&f->request, query);
	ENCODE_PARAMS(1, &n_rows, TUPLE__PARAMS);
	HANDLE(QUERY);
	ASSERT_CALLBACK(0, ROWS);

	uint64__decode(f->cursor, &n);
	munit_assert_int(n, ==, 1);
	text__decode(f->cursor, &column);
	munit_assert_string_equal(column, "n");

	for (i = 0; i < n_rows_buffer; i++) {
		DECODE_ROW(1, &value);
		munit_assert_int(value.type, ==, SQLITE_INTEGER);
		munit_assert_int(value.integer, ==, i+1);
	}

	DECODE(&f->response, rows);
	munit_assert_ullong(f->response.eof, ==, DQLITE_RESPONSE_ROWS_PART);

	return MUNIT_OK;
}

/* Submit a query request right after the server has been re-elected and needs
 * to catch up with logs. */
TEST_CASE(query, barrier, NULL)
{
	struct query_fixture *f = data;
	uint64_t stmt_id;
	(void)params;

	PREPARE("INSERT INTO test(n) VALUES(1)");
	EXEC_SUBMIT(stmt_id);
	CLUSTER_DEPOSE;
	ASSERT_CALLBACK(SQLITE_IOERR_LEADERSHIP_LOST, FAILURE);

	/* Re-elect ourselves and issue a query request */
	CLUSTER_ELECT(0);
	PREPARE("SELECT n FROM test");
	f->request.db_id = 0;
	f->request.stmt_id = stmt_id;
	ENCODE(&f->request, query);
	HANDLE(QUERY);
	WAIT;
	ASSERT_CALLBACK(0, ROWS);
	return MUNIT_OK;
}

/* Submit a prepare request that triggers a failed barrier operation. */
TEST_CASE(query, barrier_error, NULL)
{
	struct query_fixture *f = data;
	uint64_t stmt_id;
	(void)params;

	CLUSTER_APPLIED(3);

	PREPARE("SELECT n FROM test");

	/* Send a barrier which will fail. */
	struct raft_barrier faulty_barrier = {};
	raft_fixture_append_fault(&f->cluster, 0, 0);
	int rv = raft_barrier(CLUSTER_RAFT(0), &faulty_barrier, barrierCb);
	munit_assert_int(rv, ==, 0);

	f->request.db_id = 0;
	f->request.stmt_id = stmt_id;
	ENCODE(&f->request, query);
	HANDLE_STATUS(DQLITE_REQUEST_QUERY, RAFT_OK);
	WAIT;
	ASSERT_CALLBACK(SQLITE_IOERR, FAILURE);
	ASSERT_FAILURE(SQLITE_IOERR, "leader exec failed");

	return MUNIT_OK;
}

/* Submit a query request right after the server has been re-elected and needs
 * to catch up with logs, but close early */
TEST_CASE(query, barrierInFlightQuery, NULL)
{
	struct query_fixture *f = data;
	uint64_t stmt_id;
	(void)params;

	PREPARE("INSERT INTO test(n) VALUES(1)");
	EXEC_SUBMIT(stmt_id);
	CLUSTER_DEPOSE;
	ASSERT_CALLBACK(SQLITE_IOERR_LEADERSHIP_LOST, FAILURE);

	/* Re-elect ourselves and issue a query request */
	CLUSTER_ELECT(0);

	PREPARE("SELECT n FROM test");
	f->request.db_id = 0;
	f->request.stmt_id = stmt_id;
	ENCODE(&f->request, query);
	HANDLE(QUERY);
	return MUNIT_OK;
}

/* Submit a query sql request right after the server has been re-elected and
 * needs to catch up with logs, but close early */
TEST_CASE(query, barrierInFlightQuerySql, NULL)
{
	struct query_fixture *f = data;
	uint64_t stmt_id;
	(void)params;

	PREPARE("INSERT INTO test(n) VALUES(1)");
	EXEC_SUBMIT(stmt_id);
	CLUSTER_DEPOSE;
	ASSERT_CALLBACK(SQLITE_IOERR_LEADERSHIP_LOST, FAILURE);

	/* Re-elect ourselves and issue a query request */
	CLUSTER_ELECT(0);

	QUERY_SQL_SUBMIT("SELECT n FROM test");
	return MUNIT_OK;
}

/* Submit an exec request right after the server has been re-elected and needs
 * to catch up with logs, but close early */
TEST_CASE(query, barrierInFlightExec, NULL)
{
	struct query_fixture *f = data;
	uint64_t stmt_id;
	(void)params;

	PREPARE("INSERT INTO test(n) VALUES(1)");
	EXEC_SUBMIT(stmt_id);
	CLUSTER_DEPOSE;
	ASSERT_CALLBACK(SQLITE_IOERR_LEADERSHIP_LOST, FAILURE);

	/* Re-elect ourselves and issue an exec request */
	CLUSTER_ELECT(0);

	PREPARE("INSERT INTO test(n) VALUES(2)");
	EXEC_SUBMIT(stmt_id);
	return MUNIT_OK;
}

/* Send a QUERY request in the new (schema version 1) format, which
 * supports larger numbers of parameters. */
TEST_CASE(query, manyParams, NULL)
{
	struct query_fixture *f = data;
	uint64_t stmt_id;
	size_t len = 20000;
	char *sql = munit_malloc(len);
	size_t pos;
	size_t i;
	size_t num_query_params = 999;
	struct value *values = munit_calloc(num_query_params, sizeof(*values));
	(void)params;

	pos = snprintf(sql, len, "SELECT (n) FROM test WHERE n = ?");
	for (i = 1; i < num_query_params; i++) {
		pos += snprintf(sql + pos, len - pos, " OR n = ?");
	}

	for (i = 0; i < num_query_params; i++) {
		values[i].type = SQLITE_INTEGER;
		values[i].integer = i;
	}

	PREPARE(sql);
	f->request.db_id = 0;
	f->request.stmt_id = stmt_id;
	ENCODE(&f->request, query);
	ENCODE_PARAMS(num_query_params, values, TUPLE__PARAMS32);
	HANDLE_SCHEMA_STATUS(DQLITE_REQUEST_QUERY, 1, 0);
	WAIT;
	ASSERT_CALLBACK(0, ROWS);

	FINALIZE(stmt_id);
	free(values);
	free(sql);
	return MUNIT_OK;
}


/* Successfully query that yields a large number of rows that need to be split
 * into several reponses. */
TEST_CASE(query, close_while_in_flight, NULL)
{
	struct query_fixture *f = data;
	unsigned i;
	uint64_t stmt_id;
	uint64_t n;
	const char *column;
	struct value value;
	bool finished;
	(void)params;
	EXEC("BEGIN");

	/* 16 = 8B header + 8B value (int) */
	unsigned n_rows_buffer = max_rows_buffer(16);
	/* Insert 1 less than 2 response buffers worth of rows, otherwise we
	 * need 3 responses, of which the last one contains no rows. */
	for (i = 0; i < ((2 * n_rows_buffer) - 1); i++) {
		EXEC("INSERT INTO test(n) VALUES(123)");
	}
	EXEC("COMMIT");

	PREPARE("SELECT n FROM test");
	f->request.db_id = 0;
	f->request.stmt_id = stmt_id;
	ENCODE(&f->request, query);
	HANDLE(QUERY);
	ASSERT_CALLBACK(0, ROWS);

	uint64__decode(f->cursor, &n);
	munit_assert_int(n, ==, 1);
	text__decode(f->cursor, &column);
	munit_assert_string_equal(column, "n");

	/* First response contains max amount of rows */
	for (i = 0; i < n_rows_buffer; i++) {
		DECODE_ROW(1, &value);
		munit_assert_int(value.type, ==, SQLITE_INTEGER);
		munit_assert_int(value.integer, ==, 123);
	}

	/* Simulate a gateway close */
	gateway__close(f->gateway, closeCb);
	gateway__resume(f->gateway, &finished);

	return MUNIT_OK;
}

/******************************************************************************
 *
 * finalize
 *
 ******************************************************************************/

struct finalize_fixture {
	FIXTURE;
	struct request_finalize request;
	struct response_empty response;
};

TEST_SUITE(finalize);
TEST_SETUP(finalize)
{
	struct finalize_fixture *f = munit_malloc(sizeof *f);
	SETUP;
	OPEN;
	return f;
}
TEST_TEAR_DOWN(finalize)
{
	struct finalize_fixture *f = data;
	TEAR_DOWN;
	free(f);
}

/* Finalize a prepared statement. */
TEST_CASE(finalize, success, NULL)
{
	uint64_t stmt_id;
	struct finalize_fixture *f = data;
	(void)params;
	CLUSTER_ELECT(0);
	PREPARE("CREATE TABLE test (n INT)");
	f->request.db_id = 0;
	f->request.stmt_id = stmt_id;
	ENCODE(&f->request, finalize);
	HANDLE(FINALIZE);
	ASSERT_CALLBACK(0, EMPTY);
	return MUNIT_OK;
}

/******************************************************************************
 *
 * exec_sql
 *
 ******************************************************************************/

struct exec_sql_fixture {
	FIXTURE;
	struct request_exec_sql request;
	struct response_result response;
};

TEST_SUITE(exec_sql);
TEST_SETUP(exec_sql)
{
	struct exec_sql_fixture *f = munit_malloc(sizeof *f);
	SETUP;
	CLUSTER_ELECT(0);
	OPEN;
	return f;
}
TEST_TEAR_DOWN(exec_sql)
{
	struct exec_sql_fixture *f = data;
	TEAR_DOWN;
	free(f);
}

/* Exec a SQL text with a single query. */
TEST_CASE(exec_sql, single, NULL)
{
	struct exec_sql_fixture *f = data;
	(void)params;
	f->request.db_id = 0;
	f->request.sql = "CREATE TABLE test (n INT)";
	ENCODE(&f->request, exec_sql);
	HANDLE(EXEC_SQL);
	CLUSTER_APPLIED(3);
	ASSERT_CALLBACK(0, RESULT);
	return MUNIT_OK;
}

/* Exec a SQL text with a single query. */
TEST_CASE(exec_sql, empty1, NULL)
{
	struct exec_sql_fixture *f = data;
	(void)params;
	f->request.db_id = 0;
	f->request.sql = "";
	ENCODE(&f->request, exec_sql);
	HANDLE(EXEC_SQL);
	WAIT;
	ASSERT_CALLBACK(0, RESULT);
	return MUNIT_OK;
}

/* Exec a SQL text with a single query. */
TEST_CASE(exec_sql, empty2, NULL)
{
	struct exec_sql_fixture *f = data;
	(void)params;
	f->request.db_id = 0;
	f->request.sql = " --   Comment";
	ENCODE(&f->request, exec_sql);
	HANDLE(EXEC_SQL);
	WAIT;
	ASSERT_CALLBACK(0, RESULT);
	return MUNIT_OK;
}

/* Exec an invalid SQL text with a single query. */
TEST_CASE(exec_sql, invalid, NULL)
{
	struct exec_sql_fixture *f = data;
	(void)params;
	f->request.db_id = 0;
	f->request.sql = "NOT SQL";
	ENCODE(&f->request, exec_sql);
	HANDLE(EXEC_SQL);
	WAIT;
	ASSERT_CALLBACK(SQLITE_ERROR, FAILURE);
	ASSERT_FAILURE(SQLITE_ERROR, "near \"NOT\": syntax error");
	return MUNIT_OK;
}

/* Exec a SQL text with a multiple queries. */
TEST_CASE(exec_sql, multi, NULL)
{
	struct exec_sql_fixture *f = data;
	(void)params;
	f->request.db_id = 0;
	f->request.sql =
	    "CREATE TABLE test (n INT); INSERT INTO test VALUES(1)";
	ENCODE(&f->request, exec_sql);
	HANDLE(EXEC_SQL);
	WAIT;
	ASSERT_CALLBACK(0, RESULT);
	return MUNIT_OK;
}

/* Exec an ATTACH DATABASE statement -- this should fail. */
TEST_CASE(exec_sql, attach, NULL)
{
	struct exec_sql_fixture *f = data;
	(void)params;
	f->request.db_id = 0;
	f->request.sql = "ATTACH DATABASE foo AS foo";
	ENCODE(&f->request, exec_sql);
	HANDLE(EXEC_SQL);
	WAIT;
	ASSERT_CALLBACK(SQLITE_AUTH, FAILURE);
	ASSERT_FAILURE(SQLITE_AUTH, "not authorized");
	return MUNIT_OK;
}

/* Exec an SQL text and close the gateway early. */
TEST_CASE(exec_sql, closing, NULL)
{
	struct exec_sql_fixture *f = data;
	(void)params;
	f->request.db_id = 0;
	f->request.sql = "CREATE TABLE test (n INT)";
	ENCODE(&f->request, exec_sql);
	HANDLE(EXEC_SQL);
	return MUNIT_OK;
}

/* Submit an EXEC_SQL request that triggers a failed barrier operation. */
TEST_CASE(exec_sql, barrier_error, NULL)
{
	struct exec_sql_fixture *f = data;
	(void)params;

	CLUSTER_APPLIED(2);

	/* Send a barrier which will fail. */
	struct raft_barrier faulty_barrier = {};
	raft_fixture_append_fault(&f->cluster, 0, 0);
	int rv = raft_barrier(CLUSTER_RAFT(0), &faulty_barrier, barrierCb);
	munit_assert_int(rv, ==, 0);

	f->request.db_id = 0;
	f->request.sql = "CREATE TABLE test (n INT)";
	ENCODE(&f->request, exec_sql);
	HANDLE_STATUS(DQLITE_REQUEST_EXEC_SQL, RAFT_OK);
	WAIT;
	ASSERT_CALLBACK(SQLITE_IOERR, FAILURE);
	ASSERT_FAILURE(SQLITE_IOERR, "leader exec failed");

	return MUNIT_OK;
}

/* Exec SQL text with a multiple queries and close the gateway early. */
TEST_CASE(exec_sql, closingMulti, NULL)
{
	struct exec_sql_fixture *f = data;
	(void)params;
	f->request.db_id = 0;
	f->request.sql =
	    "CREATE TABLE test (n INT);"
		"INSERT INTO test(i) VALUES(0);"
		"INSERT INTO test(i) VALUES(1);"
		"INSERT INTO test(i) VALUES(2);"
		"INSERT INTO test(i) VALUES(3);";
	ENCODE(&f->request, exec_sql);
	HANDLE(EXEC_SQL);

	for (int i = 0; i < 10; i++) {
		CLUSTER_STEP;
	}

	munit_assert_false(f->context->invoked);
	return MUNIT_OK;
}

/* Send an EXEC_SQL request in the new (schema version 1) format, which
 * supports larger numbers of parameters. */
TEST_CASE(exec_sql, manyParams, NULL)
{
	struct exec_sql_fixture *f = data;
	size_t len = 20000;
	char *sql = munit_malloc(len);
	size_t pos;
	size_t i;
	size_t num_exec_params = 999;
	struct value *values = munit_calloc(num_exec_params, sizeof(*values));
	(void)params;

	pos = snprintf(sql, len, "DELETE FROM test WHERE n = ?");
	for (i = 1; i < num_exec_params; i++) {
		pos += snprintf(sql + pos, len - pos, " OR n = ?");
	}

	for (i = 0; i < num_exec_params; i++) {
		values[i].type = SQLITE_INTEGER;
		values[i].integer = i;
	}

	EXEC("CREATE TABLE test (n INT)");
	f->request.db_id = 0;
	f->request.sql = sql;
	ENCODE(&f->request, exec_sql);
	ENCODE_PARAMS(num_exec_params, values, TUPLE__PARAMS32);
	HANDLE_SCHEMA_STATUS(DQLITE_REQUEST_EXEC_SQL, 1, 0);
	WAIT;
	ASSERT_CALLBACK(0, RESULT);

	free(values);
	free(sql);
	return MUNIT_OK;
}

/******************************************************************************
 *
 * query_sql
 *
 ******************************************************************************/

struct query_sql_fixture {
	FIXTURE;
	struct request_query_sql request;
	struct response_rows response;
};

TEST_SUITE(query_sql);
TEST_SETUP(query_sql)
{
	struct query_sql_fixture *f = munit_malloc(sizeof *f);
	SETUP;
	CLUSTER_ELECT(0);
	OPEN;
	EXEC("CREATE TABLE test (n INT)");
	return f;
}
TEST_TEAR_DOWN(query_sql)
{
	struct query_sql_fixture *f = data;
	TEAR_DOWN;
	free(f);
}

/* Exec a SQL query whose result set fits in a page. */
TEST_CASE(query_sql, small, NULL)
{
	struct query_sql_fixture *f = data;
	(void)params;
	EXEC("INSERT INTO test VALUES(123)");
	f->request.db_id = 0;
	f->request.sql = "SELECT n FROM test";
	ENCODE(&f->request, query_sql);
	HANDLE(QUERY_SQL);
	ASSERT_CALLBACK(0, ROWS);
	return MUNIT_OK;
}

/* Exec an empty query sql. */
TEST_CASE(query_sql, empty1, NULL)
{
	struct query_sql_fixture *f = data;
	(void)params;
	EXEC("INSERT INTO test VALUES(123)");
	f->request.db_id = 0;
	f->request.sql = "";
	ENCODE(&f->request, query_sql);
	HANDLE(QUERY_SQL);
	ASSERT_CALLBACK(0, FAILURE);
	ASSERT_FAILURE(0, "empty statement");
	return MUNIT_OK;
}

/* Exec an empty query sql. */
TEST_CASE(query_sql, empty2, NULL)
{
	struct query_sql_fixture *f = data;
	(void)params;
	EXEC("INSERT INTO test VALUES(123)");
	f->request.db_id = 0;
	f->request.sql = "               -- a comment";
	ENCODE(&f->request, query_sql);
	HANDLE(QUERY_SQL);
	ASSERT_CALLBACK(0, FAILURE);
	ASSERT_FAILURE(0, "empty statement");
	return MUNIT_OK;
}

/* Exec an invalid query sql. */
TEST_CASE(query_sql, invalid, NULL)
{
	struct query_sql_fixture *f = data;
	(void)params;
	EXEC("INSERT INTO test VALUES(123)");
	f->request.db_id = 0;
	f->request.sql = "NOT SQL";
	ENCODE(&f->request, query_sql);
	HANDLE(QUERY_SQL);
	ASSERT_CALLBACK(SQLITE_ERROR, FAILURE);
	ASSERT_FAILURE(SQLITE_ERROR, "near \"NOT\": syntax error");
	return MUNIT_OK;
}

/* Exec a SQL query whose result set needs multiple pages. */
TEST_CASE(query_sql, large, NULL)
{
	struct query_sql_fixture *f = data;
	(void)params;
	unsigned i;
	uint64_t n;
	const char *column;
	struct value value;
	bool finished;

	/* 16 = 8B header + 8B value (int) */
	unsigned n_rows_buffer = max_rows_buffer(16);
	struct value n_rows = { .type = SQLITE_INTEGER, .integer = n_rows_buffer*2-1 };
	f->request.db_id = 0;
	f->request.sql =
	    "WITH RECURSIVE seq(n) AS ("
	    "	SELECT 1               "
	    "	UNION ALL              "
	    "	SELECT n+1             "
	    "	FROM seq WHERE n < ?   "
	    ")                         "
	    "SELECT * FROM seq         ";
	ENCODE(&f->request, query_sql);
	ENCODE_PARAMS(1, &n_rows, TUPLE__PARAMS);
	HANDLE(QUERY_SQL);
	ASSERT_CALLBACK(0, ROWS);

	uint64__decode(f->cursor, &n);
	munit_assert_int(n, ==, 1);
	text__decode(f->cursor, &column);
	munit_assert_string_equal(column, "n");

	/* First response contains max amount of rows */
	for (i = 1; i <= n_rows_buffer; i++) {
		DECODE_ROW(1, &value);
		munit_assert_int(value.type, ==, SQLITE_INTEGER);
		munit_assert_int(value.integer, ==, i);
	}

	DECODE(&f->response, rows);
	munit_assert_ullong(f->response.eof, ==, DQLITE_RESPONSE_ROWS_PART);

	gateway__resume(f->gateway, &finished);
	munit_assert_false(finished);

	ASSERT_CALLBACK(0, ROWS);

	uint64__decode(f->cursor, &n);
	munit_assert_int(n, ==, 1);
	text__decode(f->cursor, &column);
	munit_assert_string_equal(column, "n");

	/* Second, and last, response contains 1 less than maximum amount */
	for (; i <= n_rows_buffer*2 - 1; i++) {
		DECODE_ROW(1, &value);
		munit_assert_int(value.type, ==, SQLITE_INTEGER);
		munit_assert_int(value.integer, ==, i);
	}

	DECODE(&f->response, rows);
	munit_assert_ullong(f->response.eof, ==, DQLITE_RESPONSE_ROWS_DONE);

	gateway__resume(f->gateway, &finished);
	munit_assert_true(finished);
	return MUNIT_OK;
}

/* Exec a SQL query whose result set needs multiple pages and close before
 * receiving the full result set. */
TEST_CASE(query_sql, largeClose, NULL)
{
	struct query_sql_fixture *f = data;
	(void)params;
	unsigned i;
	uint64_t n;
	const char *column;
	struct value value;

	unsigned n_rows_buffer = max_rows_buffer(16);
	struct value n_rows = { .type = SQLITE_INTEGER, .integer = n_rows_buffer*2-1 };
	f->request.db_id = 0;
	f->request.sql =
	    "WITH RECURSIVE seq(n) AS ("
	    "	SELECT 1               "
	    "	UNION ALL              "
	    "	SELECT n+1             "
	    "	FROM seq WHERE n < ?   "
	    ")                         "
	    "SELECT * FROM seq         ";
	ENCODE(&f->request, query_sql);
	ENCODE_PARAMS(1, &n_rows, TUPLE__PARAMS);
	HANDLE(QUERY_SQL);
	ASSERT_CALLBACK(0, ROWS);

	uint64__decode(f->cursor, &n);
	munit_assert_int(n, ==, 1);
	text__decode(f->cursor, &column);
	munit_assert_string_equal(column, "n");

	/* First response contains max amount of rows */
	for (i = 1; i <= n_rows_buffer; i++) {
		DECODE_ROW(1, &value);
		munit_assert_int(value.type, ==, SQLITE_INTEGER);
		munit_assert_int(value.integer, ==, i);
	}

	DECODE(&f->response, rows);
	munit_assert_ullong(f->response.eof, ==, DQLITE_RESPONSE_ROWS_PART);

	return MUNIT_OK;
}

TEST_CASE(query_sql, modifying, NULL)
{
	struct query_sql_fixture *f = data;
	uint64_t n;
	bool finished;
	(void)params;

	f->request.db_id = 0;
	f->request.sql = "INSERT INTO test(n) VALUES(123)";
	ENCODE(&f->request, query_sql);
	HANDLE(QUERY_SQL);
	WAIT;
	ASSERT_CALLBACK(0, ROWS);

	uint64__decode(f->cursor, &n);
	munit_assert_int(n, ==, 0);
	DECODE(&f->response, rows);
	munit_assert_ullong(f->response.eof, ==, DQLITE_RESPONSE_ROWS_DONE);

	gateway__resume(f->gateway, &finished);
	munit_assert_true(finished);
	return MUNIT_OK;
}

TEST_CASE(query_sql, returning, NULL)
{
	struct query_sql_fixture *f = data;
	uint64_t n;
	const char *column;
	struct value value;
	bool finished;
	(void)params;

	f->request.db_id = 0;
	f->request.sql = "INSERT INTO test(n) VALUES(123) RETURNING n";
	ENCODE(&f->request, query_sql);
	HANDLE(QUERY_SQL);
	WAIT;
	ASSERT_CALLBACK(0, ROWS);

	uint64__decode(f->cursor, &n);
	munit_assert_int(n, ==, 1);
	text__decode(f->cursor, &column);
	munit_assert_string_equal(column, "n");

	/* First response contains max amount of rows */
	DECODE_ROW(1, &value);
	munit_assert_int(value.type, ==, SQLITE_INTEGER);
	munit_assert_int(value.integer, ==, 123);

	DECODE(&f->response, rows);
	munit_assert_ullong(f->response.eof, ==, DQLITE_RESPONSE_ROWS_DONE);

	gateway__resume(f->gateway, &finished);
	munit_assert_true(finished);
	return MUNIT_OK;
}

TEST_CASE(query_sql, returning_large, NULL)
{
	struct query_sql_fixture *f = data;
	int i;
	uint64_t n;
	const char *column;
	struct value value;
	bool finished;
	(void)params;

	/* Query 2 response buffers worth of rows */
	/* 16 = 8B header + 8B value (int) */
	int64_t n_rows_buffer = max_rows_buffer(16);
	struct value n_rows = { .type = SQLITE_INTEGER, .integer = n_rows_buffer*2-1 };

	f->request.db_id = 0;
	f->request.sql = 
		"WITH RECURSIVE seq(n) AS ("
		"	SELECT 1               "
		"	UNION ALL              "
		"	SELECT n+1             "
		"	FROM seq WHERE n < ?   "
		")                         "
		"INSERT INTO test(n) SELECT n FROM seq RETURNING n";
	ENCODE(&f->request, query_sql);
	ENCODE_PARAMS(1, &n_rows, TUPLE__PARAMS);
	HANDLE(QUERY_SQL);
	WAIT;
	ASSERT_CALLBACK(0, ROWS);

	uint64__decode(f->cursor, &n);
	munit_assert_int(n, ==, 1);
	text__decode(f->cursor, &column);
	munit_assert_string_equal(column, "n");

	/* First response contains max amount of rows */
	for (i = 1; i <= n_rows_buffer; i++) {
		DECODE_ROW(1, &value);
		munit_assert_int(value.type, ==, SQLITE_INTEGER);
		munit_assert_int(value.integer, ==, i);
	}

	DECODE(&f->response, rows);
	munit_assert_ullong(f->response.eof, ==, DQLITE_RESPONSE_ROWS_PART);

	gateway__resume(f->gateway, &finished);
	munit_assert_false(finished);

	WAIT;
	ASSERT_CALLBACK(0, ROWS);

	uint64__decode(f->cursor, &n);
	munit_assert_int(n, ==, 1);
	text__decode(f->cursor, &column);
	munit_assert_string_equal(column, "n");

	/* Second, and last, response contains 1 less than maximum amount */
	for (; i < 2*n_rows_buffer; i++) {
		DECODE_ROW(1, &value);
		munit_assert_int(value.type, ==, SQLITE_INTEGER);
		munit_assert_int(value.integer, ==, i);
	}

	DECODE(&f->response, rows);
	munit_assert_ullong(f->response.eof, ==, DQLITE_RESPONSE_ROWS_DONE);

	gateway__resume(f->gateway, &finished);
	munit_assert_true(finished);
	return MUNIT_OK;
}

TEST_CASE(query_sql, returning_interrupt, NULL)
{
	struct query_sql_fixture *f = data;
	struct request_interrupt interrupt;
	int i;
	uint64_t n;
	const char *column;
	struct value value;
	bool finished;
	(void)params;

	/* Query 2 response buffers worth of rows */
	/* 16 = 8B header + 8B value (int) */
	int64_t n_rows_buffer = max_rows_buffer(16);
	struct value n_rows = { .type = SQLITE_INTEGER, .integer = n_rows_buffer*2-1 };

	f->request.db_id = 0;
	f->request.sql = 
		"WITH RECURSIVE seq(n) AS ("
		"	SELECT 1               "
		"	UNION ALL              "
		"	SELECT n+1             "
		"	FROM seq WHERE n < ?   "
		")                         "
		"INSERT INTO test(n) SELECT n FROM seq RETURNING n";
	ENCODE(&f->request, query_sql);
	ENCODE_PARAMS(1, &n_rows, TUPLE__PARAMS);
	HANDLE(QUERY_SQL);
	WAIT;
	ASSERT_CALLBACK(0, ROWS);

	uint64__decode(f->cursor, &n);
	munit_assert_int(n, ==, 1);
	text__decode(f->cursor, &column);
	munit_assert_string_equal(column, "n");

	/* First response contains max amount of rows */
	for (i = 1; i <= n_rows_buffer; i++) {
		DECODE_ROW(1, &value);
		munit_assert_int(value.type, ==, SQLITE_INTEGER);
		munit_assert_int(value.integer, ==, i);
	}

	DECODE(&f->response, rows);
	munit_assert_ullong(f->response.eof, ==, DQLITE_RESPONSE_ROWS_PART);

	ENCODE(&interrupt, interrupt);
	HANDLE(INTERRUPT);
	/* The interrupt should not return immediately, but wait for
	 * the query to end. */
	munit_assert_false(f->context->invoked);
	gateway__resume(f->gateway, &finished);
	munit_assert_false(finished);
	ASSERT_CALLBACK(0, EMPTY);

	/* Make sure rows are not there. */
	f->request.sql = "SELECT * FROM test";
	ENCODE(&f->request, query_sql);
	HANDLE(QUERY_SQL);
	WAIT;
	ASSERT_CALLBACK(0, EMPTY);

	return MUNIT_OK;
}

/* Interrupt a large query sql. */
TEST_CASE(query_sql, interrupt, NULL)
{
	struct query_sql_fixture *f = data;
	struct request_interrupt interrupt;
	unsigned i;
	uint64_t n;
	const char *column;
	struct value value;
	(void)params;
	EXEC("BEGIN");

	/* 16 = 8B header + 8B value (int) */
	unsigned n_rows_buffer = max_rows_buffer(16);
	/* Insert 2 response buffers worth of rows */
	for (i = 0; i < 2 * n_rows_buffer; i++) {
		EXEC("INSERT INTO test(n) VALUES(123)");
	}
	EXEC("COMMIT");

	f->request.db_id = 0;
	f->request.sql = "SELECT n FROM test";
	ENCODE(&f->request, query_sql);
	HANDLE(QUERY_SQL);
	ASSERT_CALLBACK(0, ROWS);

	uint64__decode(f->cursor, &n);
	munit_assert_int(n, ==, 1);
	text__decode(f->cursor, &column);
	munit_assert_string_equal(column, "n");

	for (i = 0; i < n_rows_buffer; i++) {
		DECODE_ROW(1, &value);
		munit_assert_int(value.type, ==, SQLITE_INTEGER);
		munit_assert_int(value.integer, ==, 123);
	}

	DECODE(&f->response, rows);
	munit_assert_ullong(f->response.eof, ==, DQLITE_RESPONSE_ROWS_PART);

	ENCODE(&interrupt, interrupt);
	HANDLE(INTERRUPT);
	/* The interrupt should not return immediately, but wait for
	 * the query to end. */
	munit_assert_false(f->context->invoked);
	bool finished;
	gateway__resume(f->gateway, &finished);
	munit_assert_false(finished);
	ASSERT_CALLBACK(0, EMPTY);

	return MUNIT_OK;
}

/* Perform a query with parameters */
TEST_CASE(query_sql, params, NULL)
{
	struct query_sql_fixture *f = data;
	struct value values[2];
	(void)params;
	EXEC("BEGIN");
	EXEC("INSERT INTO test(n) VALUES(1)");
	EXEC("INSERT INTO test(n) VALUES(2)");
	EXEC("INSERT INTO test(n) VALUES(3)");
	EXEC("INSERT INTO test(n) VALUES(4)");
	EXEC("COMMIT");

	f->request.db_id = 0;
	f->request.sql = "SELECT n FROM test WHERE n > ? AND n < ?";
	ENCODE(&f->request, query_sql);
	values[0].type = SQLITE_INTEGER;
	values[0].integer = 1;
	values[1].type = SQLITE_INTEGER;
	values[1].integer = 4;
	ENCODE_PARAMS(2, values, TUPLE__PARAMS);

	HANDLE(QUERY_SQL);
	ASSERT_CALLBACK(0, ROWS);
	return MUNIT_OK;
}

/* Perform a query and close the gateway early. */
TEST_CASE(query_sql, closing, NULL)
{
	struct query_sql_fixture *f = data;
	(void)params;
	EXEC("INSERT INTO test VALUES(123)");
	f->request.db_id = 0;
	f->request.sql = "SELECT n FROM test";
	ENCODE(&f->request, query_sql);
	HANDLE(QUERY_SQL);
	return MUNIT_OK;
}

/* Perform a query yielding a lot of rows and close the gateway early. */
TEST_CASE(query_sql, manyClosing, NULL)
{
	(void)params;
	struct query_sql_fixture *f = data;
	bool finished;
	int rv;

	/* Insert more than maximum amount of rows that can fit in a single
	 * response. 16 = 8B header + 8B value (int) */
	unsigned n_rows_buffer = max_rows_buffer(16);
	for (unsigned i = 0; i < n_rows_buffer + 32; i++) {
		EXEC("INSERT INTO test VALUES(123)");
	}
	f->request.db_id = 0;
	f->request.sql = "SELECT n FROM test";
	ENCODE(&f->request, query_sql);
	HANDLE(QUERY_SQL);
	gateway__close(f->gateway, closeCb);
	rv = gateway__resume(f->gateway, &finished);
	munit_assert_int(rv, ==, 0);
	return MUNIT_OK;
}

/* Submit a QUERY_SQL request that triggers a failed barrier operation. */
TEST_CASE(query_sql, barrier_error, NULL)
{
	struct query_sql_fixture *f = data;
	(void)params;

	CLUSTER_APPLIED(3);

	/* Send a barrier which will fail. */
	struct raft_barrier faulty_barrier = {};
	raft_fixture_append_fault(&f->cluster, 0, 0);
	int rv = raft_barrier(CLUSTER_RAFT(0), &faulty_barrier, barrierCb);
	munit_assert_int(rv, ==, 0);

	f->request.db_id = 0;
	f->request.sql = "SELECT n FROM test";
	ENCODE(&f->request, query_sql);
	HANDLE_STATUS(DQLITE_REQUEST_QUERY_SQL, RAFT_OK);
	WAIT;
	ASSERT_CALLBACK(SQLITE_IOERR, FAILURE);
	ASSERT_FAILURE(SQLITE_IOERR, "leader exec failed");

	return MUNIT_OK;
}

/* Send a QUERY_SQL request in the new (schema version 1) format, which
 * supports larger numbers of parameters. */
TEST_CASE(query_sql, manyParams, NULL)
{
	struct query_sql_fixture *f = data;
	size_t len = 20000;
	char *sql = munit_malloc(len);
	size_t pos;
	size_t i;
	size_t num_query_params = 999;
	struct value *values = munit_calloc(num_query_params, sizeof(*values));
	(void)params;

	pos = snprintf(sql, len, "SELECT (n) FROM test WHERE n = ?");
	for (i = 1; i < num_query_params; i++) {
		pos += snprintf(sql + pos, len - pos, " OR n = ?");
	}

	for (i = 0; i < num_query_params; i++) {
		values[i].type = SQLITE_INTEGER;
		values[i].integer = i;
	}

	f->request.db_id = 0;
	f->request.sql = sql;
	ENCODE(&f->request, query_sql);
	ENCODE_PARAMS(num_query_params, values, TUPLE__PARAMS32);
	HANDLE_SCHEMA_STATUS(DQLITE_REQUEST_QUERY_SQL, 1, 0);
	WAIT;
	ASSERT_CALLBACK(0, ROWS);

	free(values);
	free(sql);
	return MUNIT_OK;
}

/* Send a QUERY_SQL request containing more than one statement. */
TEST_CASE(query_sql, nonemptyTail, NULL)
{
	struct query_sql_fixture *f = data;
	(void)params;
	f->request.db_id = 0;
	f->request.sql = "SELECT * FROM test; SELECT (n) FROM test";
	ENCODE(&f->request, query_sql);
	HANDLE(QUERY_SQL);
	ASSERT_CALLBACK(SQLITE_ERROR, FAILURE);
	ASSERT_FAILURE(SQLITE_ERROR, "nonempty statement tail");
	return MUNIT_OK;
}

/******************************************************************************
 *
 * cluster
 *
 ******************************************************************************/

struct request_cluster_fixture {
	FIXTURE;
	struct request_cluster request;
	struct response_servers response;
};

TEST_SUITE(request_cluster);
TEST_SETUP(request_cluster)
{
	struct request_cluster_fixture *f = munit_malloc(sizeof *f);
	SETUP;
	CLUSTER_ELECT(0);
	return f;
}
TEST_TEAR_DOWN(request_cluster)
{
	struct request_cluster_fixture *f = data;
	TEAR_DOWN;
	free(f);
}

/* Submit a cluster request with an invalid format version. */
TEST_CASE(request_cluster, unrecognizedFormat, NULL)
{
	struct request_cluster_fixture *f = data;
	(void)params;
	f->request.format = 2;
	ENCODE(&f->request, cluster);
	HANDLE(CLUSTER);
	ASSERT_CALLBACK(DQLITE_PARSE, FAILURE);
	ASSERT_FAILURE(DQLITE_PARSE, "unrecognized cluster format");
	return MUNIT_OK;
}

/******************************************************************************
 *
 * dump
 *
 ******************************************************************************/

struct file {
	text_t name;
	blob_t content;
};

#define DECODE_FILE(file)                                         \
	do {                                                      \
		int _rv = text__decode(f->cursor, &(file)->name); \
		munit_assert_int(_rv, ==, DQLITE_OK);             \
		_rv = blob__decode(f->cursor, &(file)->content);  \
	} while (0)

struct request_dump_fixture {
	FIXTURE;
	struct request_dump request;
	struct response_files response;
};

TEST_SUITE(dump);
TEST_SETUP(dump)
{
	struct request_dump_fixture *f = munit_malloc(sizeof *f);
	SETUP;
	CLUSTER_ELECT(0);
	return f;
}
TEST_TEAR_DOWN(dump)
{
	struct request_dump_fixture *f = data;
	TEAR_DOWN;
	free(f);
}

TEST_CASE(dump, empty, NULL)
{
	(void)params;
	struct request_dump_fixture *f = data;
	f->request = (struct request_dump){
		.filename = "test",
	};
	ENCODE(&f->request, dump);
	HANDLE(DUMP);
	ASSERT_CALLBACK(DQLITE_OK, FILES);
	DECODE(&f->response, files);

	munit_assert_int(f->response.n, ==, 2);
	struct file main = {};
	DECODE_FILE(&main);
	munit_assert_string_equal(main.name, "test");
	munit_assert_int(main.content.len, ==, 0);

	struct file wal = {};
	DECODE_FILE(&wal);
	munit_assert_string_equal(wal.name, "test-wal");
	munit_assert_int(wal.content.len, ==, 0);

	munit_assert_int(f->cursor->cap, ==, 0);
	return MUNIT_OK;
}

TEST_CASE(dump, not_existent, NULL)
{
	(void)params;
	struct request_dump_fixture *f = data;
	f->request = (struct request_dump){
		.filename = "foo",
	};
	ENCODE(&f->request, dump);
	HANDLE(DUMP);
	ASSERT_CALLBACK(DQLITE_OK, FILES);
	DECODE(&f->response, files);

	munit_assert_int(f->response.n, ==, 2);
	struct file main = {};
	DECODE_FILE(&main);
	munit_assert_string_equal(main.name, "foo");
	munit_assert_int(main.content.len, ==, 0);

	struct file wal = {};
	DECODE_FILE(&wal);
	munit_assert_string_equal(wal.name, "foo-wal");
	munit_assert_int(wal.content.len, ==, 0);

	munit_assert_int(f->cursor->cap, ==, 0);

	return MUNIT_OK;
}

TEST_CASE(dump, simple, NULL)
{
	(void)params;
	struct request_dump_fixture *f = data;

	OPEN;
	EXEC("CREATE TABLE test (n INT, data BLOB)");
	EXEC("INSERT INTO test (n, data) VALUES (1, randomblob(256))");

	f->request = (struct request_dump){
		.filename = "test",
	};
	ENCODE(&f->request, dump);
	HANDLE(DUMP);
	ASSERT_CALLBACK(DQLITE_OK, FILES);
	DECODE(&f->response, files);

	munit_assert_int(f->response.n, ==, 2);
	struct file main = {};
	DECODE_FILE(&main);
	munit_assert_string_equal(main.name, "test");
	munit_assert_int(main.content.len, >, 0);

	struct file wal = {};
	DECODE_FILE(&wal);
	munit_assert_string_equal(wal.name, "test-wal");
	munit_assert_int(wal.content.len, >, 0);

	munit_assert_int(f->cursor->cap, ==, 0);
	return MUNIT_OK;
}

TEST_CASE(dump, simple_follower, NULL)
{
	(void)params;
	struct request_dump_fixture *f = data;

	OPEN;
	EXEC("CREATE TABLE test (n INT, data BLOB)");
	EXEC("INSERT INTO test (n, data) VALUES (1, randomblob(256))");
	CLUSTER_APPLIED(CLUSTER_LAST_INDEX(0));
	SELECT(1);

	f->request = (struct request_dump){
		.filename = "test",
	};
	ENCODE(&f->request, dump);
	HANDLE(DUMP);
	ASSERT_CALLBACK(DQLITE_OK, FILES);
	DECODE(&f->response, files);

	munit_assert_int(f->response.n, ==, 2);
	struct file main = {};
	DECODE_FILE(&main);
	munit_assert_string_equal(main.name, "test");
	munit_assert_int(main.content.len, >, 0);

	struct file wal = {};
	DECODE_FILE(&wal);
	munit_assert_string_equal(wal.name, "test-wal");
	munit_assert_int(wal.content.len, >, 0);

	munit_assert_int(f->cursor->cap, ==, 0);
	return MUNIT_OK;
}

TEST_CASE(dump, checkpointed, NULL)
{
	(void)params;
	struct request_dump_fixture *f = data;
	uint64_t stmt_id;

	OPEN;
	EXEC("CREATE TABLE test (data BLOB)");
	/* Make sure we force a checkpoint */
	struct config *config = f->gateway->config;
	struct value blobsize = {
		.type = SQLITE_INTEGER,
		.integer = config->page_size * config->checkpoint_threshold,
	};
	PREPARE(
	    "INSERT INTO test           "
	    "SELECT RANDOMBLOB((        "
	    "	SELECT ?                "
	    "	FROM pragma_page_size() "
	    "))                         ");
	struct request_exec request = {
		.db_id = 0,
		.stmt_id = stmt_id,
	};
	struct response_result response;
	ENCODE(&request, exec);
	ENCODE_PARAMS(1, &blobsize, TUPLE__PARAMS);
	HANDLE(EXEC);
	WAIT;
	ASSERT_CALLBACK(0, RESULT);
	DECODE(&response, result);
	munit_assert_int(response.rows_affected, ==, 1);

	f->request = (struct request_dump){
		.filename = "test",
	};
	ENCODE(&f->request, dump);
	HANDLE(DUMP);
	ASSERT_CALLBACK(DQLITE_OK, FILES);
	DECODE(&f->response, files);

	munit_assert_int(f->response.n, ==, 2);
	struct file main = {};
	DECODE_FILE(&main);
	munit_assert_string_equal(main.name, "test");
	munit_assert_int(main.content.len, >=,
			 config->checkpoint_threshold * config->page_size);

	struct file wal = {};
	DECODE_FILE(&wal);
	munit_assert_string_equal(wal.name, "test-wal");
	munit_assert_int(wal.content.len, ==, 0);

	munit_assert_int(f->cursor->cap, ==, 0);
	return MUNIT_OK;
}

/******************************************************************************
 *
 * invalid
 *
 ******************************************************************************/

struct invalid_fixture {
	FIXTURE;
	struct request_leader request;
	struct response_server response;
};

TEST_SUITE(invalid);
TEST_SETUP(invalid)
{
	struct invalid_fixture *f = munit_malloc(sizeof *f);
	SETUP;
	CLUSTER_ELECT(0);
	return f;
}
TEST_TEAR_DOWN(invalid)
{
	struct invalid_fixture *f = data;
	TEAR_DOWN;
	free(f);
}

/* Submit a request with an unrecognized type. */
TEST_CASE(invalid, requestType, NULL)
{
	struct invalid_fixture *f = data;
	(void)params;
	ENCODE(&f->request, leader);
	HANDLE_STATUS(123, 0);
	ASSERT_CALLBACK(DQLITE_PARSE, FAILURE);
	ASSERT_FAILURE(DQLITE_PARSE, "unrecognized request type");
	return MUNIT_OK;
}
