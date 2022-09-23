#include "../lib/cluster.h"
#include "../lib/runner.h"

#include "../../src/gateway.h"
#include "../../src/protocol.h"
#include "../../src/request.h"
#include "../../src/response.h"

TEST_MODULE(concurrency);

/******************************************************************************
 *
 * Fixture.
 *
 ******************************************************************************/

#define N_GATEWAYS 2

/* Context for a gateway handle request */
struct context
{
	bool invoked;
	int status;
	int type;
};

/* Standalone leader database connection */
struct connection
{
	struct gateway gateway;
	struct buffer request;  /* Request payload */
	struct buffer response; /* Response payload */
	struct handle handle;   /* Async handle request */
	struct context context;
};

#define FIXTURE          \
	FIXTURE_CLUSTER; \
	struct connection connections[N_GATEWAYS]

#define SETUP                                                        \
	unsigned i;                                                  \
	int rc;                                                      \
	SETUP_CLUSTER(V2);					     \
	CLUSTER_ELECT(0);                                            \
	for (i = 0; i < N_GATEWAYS; i++) {                           \
		struct connection *c = &f->connections[i];           \
		struct request_open open;                            \
		struct response_db db;                               \
		gateway__init(&c->gateway, CLUSTER_CONFIG(0),        \
			      CLUSTER_REGISTRY(0), CLUSTER_RAFT(0)); \
		c->handle.data = &c->context;                        \
		rc = buffer__init(&c->request);                      \
		munit_assert_int(rc, ==, 0);                         \
		rc = buffer__init(&c->response);                     \
		munit_assert_int(rc, ==, 0);                         \
		open.filename = "test";                              \
		open.vfs = "";                                       \
		ENCODE(c, &open, open);                              \
		HANDLE(c, OPEN);                                     \
		ASSERT_CALLBACK(c, 0, DB);                           \
		DECODE(c, &db, db);                                  \
		munit_assert_int(db.id, ==, 0);                      \
	}

#define TEAR_DOWN                                          \
	unsigned i;                                        \
	for (i = 0; i < N_GATEWAYS; i++) {                 \
		struct connection *c = &f->connections[i]; \
		buffer__close(&c->request);                \
		buffer__close(&c->response);               \
		gateway__close(&c->gateway);               \
	}                                                  \
	TEAR_DOWN_CLUSTER;

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

/* Reset the request buffer of the given connection and encode a request of the
 * given lower case name. */
#define ENCODE(C, REQUEST, LOWER)                               \
	{                                                       \
		size_t n2 = request_##LOWER##__sizeof(REQUEST); \
		void *cursor;                                   \
		buffer__reset(&C->request);                     \
		cursor = buffer__advance(&C->request, n2);      \
		munit_assert_ptr_not_null(cursor);              \
		request_##LOWER##__encode(REQUEST, &cursor);    \
	}

/* Decode a response of the given lower/upper case name using the response
 * buffer of the given connection. */
#define DECODE(C, RESPONSE, LOWER)                                   \
	{                                                            \
		struct cursor cursor;                                \
		int rc2;                                             \
		cursor.p = buffer__cursor(&C->response, 0);          \
		cursor.cap = buffer__offset(&C->response);           \
		rc2 = response_##LOWER##__decode(&cursor, RESPONSE); \
		munit_assert_int(rc2, ==, 0);                        \
	}

/* Submit a request of the given type to the given connection and check that no
 * error occurs. */
#define HANDLE(C, TYPE)                                                 \
	{                                                               \
		int rc2;                                                \
		C->handle.cursor.p = buffer__cursor(&C->request, 0);    \
		C->handle.cursor.cap = buffer__offset(&C->request);     \
		buffer__reset(&C->response);                            \
		rc2 = gateway__handle(&C->gateway, &C->handle,          \
				      DQLITE_REQUEST_##TYPE, 0,         \
				      &C->response, fixture_handle_cb); \
		munit_assert_int(rc2, ==, 0);                           \
	}

/* Prepare a statement on the given connection. The ID will be saved in
 * the STMT_ID pointer. */
#define PREPARE(C, SQL, STMT_ID)                \
	{                                       \
		struct request_prepare prepare; \
		struct response_stmt stmt;      \
		prepare.db_id = 0;              \
		prepare.sql = SQL;              \
		ENCODE(C, &prepare, prepare);   \
		HANDLE(C, PREPARE);             \
		WAIT(C);                        \
		ASSERT_CALLBACK(C, 0, STMT);    \
		DECODE(C, &stmt, stmt);         \
		*(STMT_ID) = stmt.id;           \
	}

/* Submit a request to exec a statement. */
#define EXEC(C, STMT_ID)                  \
	{                                 \
		struct request_exec exec; \
		exec.db_id = 0;           \
		exec.stmt_id = STMT_ID;   \
		ENCODE(C, &exec, exec);   \
		HANDLE(C, EXEC);          \
	}

/* Submit a query request. */
#define QUERY(C, STMT_ID)                   \
	{                                   \
		struct request_query query; \
		query.db_id = 0;            \
		query.stmt_id = STMT_ID;    \
		ENCODE(C, &query, query);   \
		HANDLE(C, QUERY);           \
	}

/* Wait for the gateway of the given connection to finish handling a request. */
#define WAIT(C)                                        \
	{                                              \
		unsigned _i;                           \
		for (_i = 0; _i < 50; _i++) {          \
			CLUSTER_STEP;                  \
			if (C->context.invoked) {      \
				break;                 \
			}                              \
		}                                      \
		munit_assert_true(C->context.invoked); \
	}

/******************************************************************************
 *
 * Assertions.
 *
 ******************************************************************************/

/* Assert that the handle callback of the given connection has been invoked with
 * the given status and response type.. */
#define ASSERT_CALLBACK(C, STATUS, UPPER)                               \
	munit_assert_true(C->context.invoked);                          \
	munit_assert_int(C->context.status, ==, STATUS);                \
	munit_assert_int(C->context.type, ==, DQLITE_RESPONSE_##UPPER); \
	C->context.invoked = false

/* Assert that the failure response generated by the gateway of the given
 * connection matches the given details. */
#define ASSERT_FAILURE(C, CODE, MESSAGE)                             \
	{                                                            \
		struct response_failure failure;                     \
		DECODE(C, &failure, failure);                        \
		munit_assert_int(failure.code, ==, CODE);            \
		munit_assert_string_equal(failure.message, MESSAGE); \
	}

/******************************************************************************
 *
 * Concurrent exec requests
 *
 ******************************************************************************/

struct exec_fixture
{
	FIXTURE;
	struct connection *c1;
	struct connection *c2;
	unsigned stmt_id1;
	unsigned stmt_id2;
};

TEST_SUITE(exec);
TEST_SETUP(exec)
{
	struct exec_fixture *f = munit_malloc(sizeof *f);
	SETUP;
	f->c1 = &f->connections[0];
	f->c2 = &f->connections[1];
	return f;
}
TEST_TEAR_DOWN(exec)
{
	struct exec_fixture *f = data;
	TEAR_DOWN;
	free(f);
}

/* If another leader connection has submitted an Open request and is waiting for
 * it to complete, SQLITE_BUSY is returned. */
TEST_CASE(exec, open, NULL)
{
	struct exec_fixture *f = data;
	(void)params;

	PREPARE(f->c1, "CREATE TABLE test1 (n INT)", &f->stmt_id1);
	PREPARE(f->c2, "CREATE TABLE test2 (n INT)", &f->stmt_id2);

	EXEC(f->c1, f->stmt_id1);
	EXEC(f->c2, f->stmt_id2);
	WAIT(f->c2);
	ASSERT_CALLBACK(f->c2, 0, FAILURE);
	ASSERT_FAILURE(f->c2, SQLITE_BUSY, "database is locked");
	WAIT(f->c1);
	ASSERT_CALLBACK(f->c1, 0, RESULT);

	return MUNIT_OK;
}

/* If an exec request is already in progress on another leader connection,
 * SQLITE_BUSY is returned. */
TEST_CASE(exec, tx, NULL)
{
	struct exec_fixture *f = data;
	(void)params;

	/* Create a test table using connection 0 */
	PREPARE(f->c1, "CREATE TABLE test (n INT)", &f->stmt_id1);
	EXEC(f->c1, f->stmt_id1);
	WAIT(f->c1);
	ASSERT_CALLBACK(f->c1, 0, RESULT);
	PREPARE(f->c1, "INSERT INTO test(n) VALUES(1)", &f->stmt_id1);
	PREPARE(f->c2, "INSERT INTO test(n) VALUES(1)", &f->stmt_id2);

	EXEC(f->c1, f->stmt_id1);
	EXEC(f->c2, f->stmt_id2);
	WAIT(f->c2);
	ASSERT_CALLBACK(f->c2, 0, FAILURE);
	ASSERT_FAILURE(f->c2, SQLITE_BUSY, "database is locked");
	WAIT(f->c1);
	ASSERT_CALLBACK(f->c1, 0, RESULT);
	return MUNIT_OK;
}

/******************************************************************************
 *
 * Concurrent query requests
 *
 ******************************************************************************/

struct query_fixture
{
	FIXTURE;
	struct connection *c1;
	struct connection *c2;
	unsigned stmt_id1;
	unsigned stmt_id2;
};

TEST_SUITE(query);
TEST_SETUP(query)
{
	struct exec_fixture *f = munit_malloc(sizeof *f);
	SETUP;
	f->c1 = &f->connections[0];
	f->c2 = &f->connections[1];
	PREPARE(f->c1, "CREATE TABLE test (n INT)", &f->stmt_id1);
	EXEC(f->c1, f->stmt_id1);
	WAIT(f->c1);
	return f;
}
TEST_TEAR_DOWN(query)
{
	struct exec_fixture *f = data;
	TEAR_DOWN;
	free(f);
}

/* Handle a query request while there is a transaction in progress. */
TEST_CASE(query, tx, NULL)
{
	struct exec_fixture *f = data;
	(void)params;
	PREPARE(f->c1, "INSERT INTO test VALUES(1)", &f->stmt_id1);
	PREPARE(f->c2, "SELECT n FROM test", &f->stmt_id2);
	EXEC(f->c1, f->stmt_id1);
	QUERY(f->c2, f->stmt_id2);
	WAIT(f->c1);
	WAIT(f->c2);
	ASSERT_CALLBACK(f->c1, 0, RESULT);
	ASSERT_CALLBACK(f->c2, 0, ROWS);
	return MUNIT_OK;
}
