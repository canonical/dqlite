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

#define FIXTURE                              \
	FIXTURE_CLUSTER;                     \
	struct gateway gateway;              \
	void *payload; /* Request payload */ \
	struct cursor cursor;                \
	struct buffer buffer;                \
	struct handle req;                   \
	struct context context;

#define SETUP                                                             \
	int rc;                                                           \
	SETUP_CLUSTER;                                                    \
	gateway__init(&f->gateway, CLUSTER_LOGGER(0), CLUSTER_OPTIONS(0), \
		      CLUSTER_REGISTRY(0), CLUSTER_RAFT(0));              \
	rc = buffer__init(&f->buffer);                                    \
	munit_assert_int(rc, ==, 0);                                      \
	f->payload = NULL;                                                \
	f->req.data = &f->context;                                        \
	f->context.invoked = false;                                       \
	f->context.status = -1;                                           \
	f->context.type = -1;

#define TEAR_DOWN                    \
	if (f->payload != NULL) {    \
		free(f->payload);    \
	}                            \
	buffer__close(&f->buffer);   \
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
		if (f->payload != NULL) {                       \
			free(f->payload);                       \
		}                                               \
		f->payload = munit_malloc(n2);                  \
		cursor = f->payload;                            \
		request_##LOWER##__encode(REQUEST, &cursor);    \
		f->cursor.p = f->payload;                       \
		f->cursor.cap = n2;                             \
	}

/* Decode a response of the given lower/upper case name using the buffer that
 * was written by the gateway. */
#define DECODE(RESPONSE, LOWER, UPPER)                               \
	{                                                            \
		struct cursor cursor;                                \
		int rc2;                                             \
		cursor.p = buffer__cursor(&f->buffer, 0);            \
		cursor.cap = buffer__offset(&f->buffer);             \
		munit_assert_int(f->context.type, ==,                \
				 DQLITE_RESPONSE_##UPPER);           \
		rc2 = response_##LOWER##__decode(&cursor, RESPONSE); \
		munit_assert_int(rc2, ==, 0);                        \
	}

/* Handle a request of the given type and check that no error occurs. */
#define HANDLE(TYPE)                                                     \
	{                                                                \
		int rc2;                                                 \
		buffer__reset(&f->buffer);                               \
		rc2 = gateway__handle(&f->gateway, &f->req,              \
				      DQLITE_REQUEST_##TYPE, &f->cursor, \
				      &f->buffer, fixture_handle_cb);    \
		munit_assert_int(rc2, ==, 0);                            \
	}

/* Create a statement. The ID will be saved in stmt_id. */
#define PREPARE(SQL)                            \
	{                                       \
		struct request_prepare prepare; \
		struct response_stmt stmt;      \
		prepare.db_id = 0;              \
		prepare.sql = SQL;              \
		ENCODE(&prepare, prepare);      \
		HANDLE(PREPARE);                \
		ASSERT_STATUS(0);               \
		DECODE(&stmt, stmt, STMT);      \
		stmt_id = stmt.id;              \
	}

/******************************************************************************
 *
 * Assertions.
 *
 ******************************************************************************/

/* Assert that the handle callback has been invoked with the given status */
#define ASSERT_STATUS(STATUS)                  \
	munit_assert_true(f->context.invoked); \
	munit_assert_int(f->context.status, ==, STATUS)

/* Assert that the gateway has generated a failure response */
#define ASSERT_FAILURE(CODE, MESSAGE)                                \
	{                                                            \
		struct response_failure failure;                     \
		struct cursor cursor;                                \
		int rc2;                                             \
		cursor.p = buffer__cursor(&f->buffer, 0);            \
		cursor.cap = buffer__offset(&f->buffer);             \
		munit_assert_int(f->context.type, ==,                \
				 DQLITE_RESPONSE_FAILURE);           \
		rc2 = response_failure__decode(&cursor, &failure);   \
		munit_assert_int(rc2, ==, 0);                        \
		munit_assert_int(failure.code, ==, CODE);            \
		munit_assert_string_equal(failure.message, MESSAGE); \
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
	ASSERT_STATUS(0);
	DECODE(&f->response, server, SERVER);
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
	ASSERT_STATUS(0);
	DECODE(&f->response, server, SERVER);
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
	ASSERT_STATUS(0);
	DECODE(&f->response, server, SERVER);
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
	ASSERT_STATUS(0);
	DECODE(&f->response, db, DB);
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
	ASSERT_STATUS(0);
	ENCODE(&f->request, open);
	HANDLE(OPEN);
	ASSERT_STATUS(0);
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
	struct request_open open;
	SETUP;
	open.filename = "test";
	open.vfs = "";
	ENCODE(&open, open);
	HANDLE(OPEN);
	ASSERT_STATUS(0);
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
	f->request.sql = "CREATE TABLE test (INT n)";
	ENCODE(&f->request, prepare);
	HANDLE(PREPARE);
	ASSERT_STATUS(0);
	DECODE(&f->response, stmt, STMT);
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
	struct request_open open;
	SETUP;
	open.filename = "test";
	open.vfs = "";
	ENCODE(&open, open);
	HANDLE(OPEN);
	ASSERT_STATUS(0);
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
	PREPARE("CREATE TABLE test (INT n)");
	f->request.db_id = 0;
	f->request.stmt_id = stmt_id;
	ENCODE(&f->request, exec);
	HANDLE(EXEC);
	CLUSTER_APPLIED(3);
	ASSERT_STATUS(0);
	DECODE(&f->response, result, RESULT);
	munit_assert_int(f->response.last_insert_id, ==, 0);
	munit_assert_int(f->response.rows_affected, ==, 0);
	return MUNIT_OK;
}

/* Successfully execute a statement with a one parameter. */
TEST_CASE(exec, one_param, NULL)
{
	struct exec_fixture *f = data;
	uint64_t stmt_id;
	(void)params;
	CLUSTER_ELECT(0);

	/* Create the test table */
	PREPARE("CREATE TABLE test (INT n)");
	f->request.db_id = 0;
	f->request.stmt_id = stmt_id;
	ENCODE(&f->request, exec);
	HANDLE(EXEC);
	CLUSTER_APPLIED(3);

	/* Insert a row with one parameter */
	ASSERT_STATUS(0);
	PREPARE("INSERT INTO test VALUES (1)");

	f->request.stmt_id = stmt_id;
	ENCODE(&f->request, exec);

	struct tuple_encoder encoder;
	int rc2;
	rc2 = tuple_encoder__init(&encoder, 1, TUPLE__PARAMS, &f->buffer);
	munit_assert_int(rc2, ==, 0);
	struct value value;
	value.type = SQLITE_INTEGER;
	value.integer = 7;
	rc2 = tuple_encoder__next(&encoder, &value);
	munit_assert_int(rc2, ==, 0);

	HANDLE(EXEC);
	CLUSTER_APPLIED(4);
	ASSERT_STATUS(0);

	return MUNIT_OK;
}
