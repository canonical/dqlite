#include "../../include/dqlite.h"

#include "../lib/cluster.h"
#include "../lib/runner.h"

#include "../../src/gateway.h"
#include "../../src/request.h"
#include "../../src/response.h"
#include "../../src/tuple.h"

TEST_MODULE(gateway);

/******************************************************************************
 *
 * Fixture.
 *
 ******************************************************************************/

#define FIXTURE                                      \
	FIXTURE_CLUSTER;                             \
	struct gateway gateway;                      \
	struct buffer payload; /* Request payload */ \
	struct cursor cursor;                        \
	struct buffer buffer;                        \
	struct handle req;                           \
	struct context context;

#define SETUP                                                             \
	int rc;                                                           \
	SETUP_CLUSTER;                                                    \
	gateway__init(&f->gateway, CLUSTER_LOGGER(0), CLUSTER_OPTIONS(0), \
		      CLUSTER_REGISTRY(0), CLUSTER_RAFT(0));              \
	rc = buffer__init(&f->payload);                                   \
	munit_assert_int(rc, ==, 0);                                      \
	rc = buffer__init(&f->buffer);                                    \
	munit_assert_int(rc, ==, 0);                                      \
	f->req.data = &f->context;                                        \
	f->context.invoked = false;                                       \
	f->context.status = -1;                                           \
	f->context.type = -1;

#define TEAR_DOWN                    \
	buffer__close(&f->buffer);   \
	buffer__close(&f->payload);  \
	gateway__close(&f->gateway); \
	TEAR_DOWN_CLUSTER;

/* Context for the fixture's handle request */
struct context
{
	bool invoked;
	int status;
	int type;
};

static void fixture_handle_cb(struct handle *req, int status, int type)
{
	struct context *c = req->data;
	c->invoked = true;
	c->status = status;
	c->type = type;
}

/******************************************************************************
 *
 * Helper macros.
 *
 ******************************************************************************/

/* Allocate the payload buffer, encode a request of the given lower case name
 * and initialize the fixture cursor. */
#define ENCODE(REQUEST, LOWER)                                  \
	{                                                       \
		size_t n2 = request_##LOWER##__sizeof(REQUEST); \
		void *cursor;                                   \
		buffer__reset(&f->payload);                     \
		cursor = buffer__advance(&f->payload, n2);      \
		munit_assert_ptr_not_null(cursor);              \
		request_##LOWER##__encode(REQUEST, &cursor);    \
	}

/* Encode N parameters with the given values */
#define ENCODE_PARAMS(N, VALUES)                                              \
	{                                                                     \
		struct tuple_encoder encoder;                                 \
		int i2;                                                       \
		int rc2;                                                      \
		rc2 = tuple_encoder__init(&encoder, N, TUPLE__PARAMS,         \
					  &f->payload);                       \
		munit_assert_int(rc2, ==, 0);                                 \
		for (i2 = 0; i2 < N; i2++) {                                  \
			rc2 = tuple_encoder__next(&encoder, &((VALUES)[i2])); \
			munit_assert_int(rc2, ==, 0);                         \
		}                                                             \
	}

/* Decode a response of the given lower/upper case name using the buffer that
 * was written by the gateway. */
#define DECODE(RESPONSE, LOWER)                                         \
	{                                                               \
		int rc2;                                                \
		rc2 = response_##LOWER##__decode(&f->cursor, RESPONSE); \
		munit_assert_int(rc2, ==, 0);                           \
	}

/* Decode a row with N columns filling the given values. */
#define DECODE_ROW(N, VALUES)                                                 \
	{                                                                     \
		struct tuple_decoder decoder;                                 \
		int i2;                                                       \
		int rc2;                                                      \
		rc2 = tuple_decoder__init(&decoder, N, &f->cursor);           \
		munit_assert_int(rc2, ==, 0);                                 \
		for (i2 = 0; i2 < N; i2++) {                                  \
			rc2 = tuple_decoder__next(&decoder, &((VALUES)[i2])); \
			munit_assert_int(rc2, ==, 0);                         \
		}                                                             \
	}

/* Handle a request of the given type and check that no error occurs. */
#define HANDLE(TYPE)                                                     \
	{                                                                \
		int rc2;                                                 \
		f->cursor.p = buffer__cursor(&f->payload, 0);            \
		f->cursor.cap = buffer__offset(&f->payload);             \
		buffer__reset(&f->buffer);                               \
		rc2 = gateway__handle(&f->gateway, &f->req,              \
				      DQLITE_REQUEST_##TYPE, &f->cursor, \
				      &f->buffer, fixture_handle_cb);    \
		munit_assert_int(rc2, ==, 0);                            \
	}

/* Open a leader connection the the "test" database */
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
		ASSERT_CALLBACK(0, STMT);       \
		DECODE(&stmt, stmt);            \
		stmt_id = stmt.id;              \
	}

/* Prepare and exec a statement. */
#define EXEC(SQL)                                 \
	{                                         \
		struct request_exec exec;         \
		uint64_t stmt_id;                 \
		unsigned i;                       \
		PREPARE(SQL);                     \
		exec.db_id = 0;                   \
		exec.stmt_id = stmt_id;           \
		ENCODE(&exec, exec);              \
		HANDLE(EXEC);                     \
		for (i = 0; i < 15; i++) {        \
			CLUSTER_STEP;             \
			if (f->context.invoked) { \
				break;            \
			}                         \
		}                                 \
		ASSERT_CALLBACK(0, RESULT);       \
	}

/******************************************************************************
 *
 * Assertions.
 *
 ******************************************************************************/

/* Assert that the handle callback has been invoked with the given status and
 * response type. Also, initialize the fixture's cursor to read the response
 * buffer. */
#define ASSERT_CALLBACK(STATUS, UPPER)                                  \
	munit_assert_true(f->context.invoked);                          \
	munit_assert_int(f->context.status, ==, STATUS);                \
	munit_assert_int(f->context.type, ==, DQLITE_RESPONSE_##UPPER); \
	f->cursor.p = buffer__cursor(&f->buffer, 0);                    \
	f->cursor.cap = buffer__offset(&f->buffer);                     \
	buffer__reset(&f->buffer);                                      \
	f->context.invoked = false;

/* Assert that the failure response generated by the gateway matches the given
 * details. */
#define ASSERT_FAILURE(CODE, MESSAGE)                                 \
	{                                                             \
		struct response_failure failure;                      \
		int rc2;                                              \
		rc2 = response_failure__decode(&f->cursor, &failure); \
		munit_assert_int(rc2, ==, 0);                         \
		munit_assert_int(failure.code, ==, CODE);             \
		munit_assert_string_equal(failure.message, MESSAGE);  \
	}

/******************************************************************************
 *
 * leader
 *
 ******************************************************************************/

struct leader_fixture
{
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

struct open_fixture
{
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
	ASSERT_CALLBACK(0, FAILURE);
	ASSERT_FAILURE(SQLITE_BUSY,
		       "a database for this connection is already open");
	return MUNIT_OK;
}

/******************************************************************************
 *
 * prepare
 *
 ******************************************************************************/

struct prepare_fixture
{
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
	ENCODE(&f->request, prepare);
	HANDLE(PREPARE);
	ASSERT_CALLBACK(0, STMT);
	DECODE(&f->response, stmt);
	munit_assert_int(f->response.id, ==, 0);
	return MUNIT_OK;
}

/******************************************************************************
 *
 * exec
 *
 ******************************************************************************/

struct exec_fixture
{
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
	CLUSTER_APPLIED(3);
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
	ENCODE_PARAMS(1, &value);
	HANDLE(EXEC);
	CLUSTER_APPLIED(4);
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
	char buf[8] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'};
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
	ENCODE_PARAMS(1, &value);
	HANDLE(EXEC);
	CLUSTER_APPLIED(4);
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

	uint64__decode(&f->cursor, &n);
	munit_assert_int(n, ==, 1);
	text__decode(&f->cursor, &column);
	munit_assert_string_equal(column, "data");
	DECODE_ROW(1, &value);
	munit_assert_int(value.type, ==, SQLITE_BLOB);
	munit_assert_int(value.blob.len, ==, sizeof buf);
	munit_assert_int(value.blob.base[0], ==, 'a');
	munit_assert_int(value.blob.base[7], ==, 'h');

	return MUNIT_OK;
}

/******************************************************************************
 *
 * query
 *
 ******************************************************************************/

struct query_fixture
{
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
	uint64__decode(&f->cursor, &n);
	munit_assert_int(n, ==, 1);
	text__decode(&f->cursor, &column);
	munit_assert_string_equal(column, "n");
	DECODE(&f->response, rows);
	munit_assert_ulong(f->response.eof, ==, DQLITE_RESPONSE_ROWS_DONE);

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

	uint64__decode(&f->cursor, &n);
	munit_assert_int(n, ==, 1);
	text__decode(&f->cursor, &column);
	munit_assert_string_equal(column, "n");
	DECODE_ROW(1, &value);
	munit_assert_int(value.type, ==, SQLITE_INTEGER);
	munit_assert_int(value.integer, ==, 666);
	DECODE(&f->response, rows);
	munit_assert_ulong(f->response.eof, ==, DQLITE_RESPONSE_ROWS_DONE);

	return MUNIT_OK;
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
	(void)params;
	EXEC("BEGIN");
	for (i = 0; i < 500; i++) {
		EXEC("INSERT INTO test(n) VALUES(123)");
	}
	EXEC("COMMIT");

	PREPARE("SELECT n FROM test");
	f->request.db_id = 0;
	f->request.stmt_id = stmt_id;
	ENCODE(&f->request, query);
	HANDLE(QUERY);
	ASSERT_CALLBACK(0, ROWS);

	uint64__decode(&f->cursor, &n);
	munit_assert_int(n, ==, 1);
	text__decode(&f->cursor, &column);
	munit_assert_string_equal(column, "n");

	for (i = 0; i < 255; i++) {
		DECODE_ROW(1, &value);
		munit_assert_int(value.type, ==, SQLITE_INTEGER);
		munit_assert_int(value.integer, ==, 123);
	}

	DECODE(&f->response, rows);
	munit_assert_ulong(f->response.eof, ==, DQLITE_RESPONSE_ROWS_PART);

	gateway__resume(&f->gateway);

	ASSERT_CALLBACK(0, ROWS);

	uint64__decode(&f->cursor, &n);
	munit_assert_int(n, ==, 1);
	text__decode(&f->cursor, &column);
	munit_assert_string_equal(column, "n");

	for (i = 0; i < 245; i++) {
		DECODE_ROW(1, &value);
		munit_assert_int(value.type, ==, SQLITE_INTEGER);
		munit_assert_int(value.integer, ==, 123);
	}

	DECODE(&f->response, rows);
	munit_assert_ulong(f->response.eof, ==, DQLITE_RESPONSE_ROWS_DONE);

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
	ENCODE_PARAMS(2, values);

	HANDLE(QUERY);
	ASSERT_CALLBACK(0, ROWS);
	return MUNIT_OK;
}

/******************************************************************************
 *
 * finalize
 *
 ******************************************************************************/

struct finalize_fixture
{
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

struct exec_sql_fixture
{
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
	CLUSTER_APPLIED(4);
	ASSERT_CALLBACK(0, RESULT);
	return MUNIT_OK;
}

/******************************************************************************
 *
 * query_sql
 *
 ******************************************************************************/

struct query_sql_fixture
{
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
	ENCODE_PARAMS(2, values);

	HANDLE(QUERY_SQL);
	ASSERT_CALLBACK(0, ROWS);
	return MUNIT_OK;
}
