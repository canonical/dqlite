#include "../lib/heap.h"
#include "../lib/logger.h"
#include "../lib/options.h"
#include "../lib/registry.h"
#include "../lib/runner.h"
#include "../lib/sqlite.h"
#include "../lib/vfs.h"
#ifdef DQLITE_EXPERIMENTAL
#include "../lib/raft.h"
#include "../lib/replication.h"
#endif /* DQLITE_EXPERIMENTAL */

#include "../replication.h"

#include "../../src/format.h"
#include "../../src/gateway.h"

#include "../cluster.h"

TEST_MODULE(gateway);

/******************************************************************************
 *
 * Helpers
 *
 ******************************************************************************/

struct fixture
{
	FIXTURE_LOGGER;
	FIXTURE_VFS;
	FIXTURE_OPTIONS;
#ifdef DQLITE_EXPERIMENTAL
	FIXTURE_REGISTRY;
	FIXTURE_RAFT;
	FIXTURE_REPLICATION;
#else
	FIXTURE_STUB_REPLICATION;
#endif /* DQLITE_EXPERIMENTAL */
	dqlite_cluster *cluster;
	struct gateway *gateway;
	struct request *request;
	struct response *response;
};

/* Gateway flush callback, saving the response on the fixture. */
static void fixture_flush_cb(void *arg, struct response *response)
{
	struct fixture *f = arg;

	munit_assert_ptr_not_null(f);

	f->response = response;
}

/* Send a valid open request and return the database ID */
static void __open(struct fixture *f, uint32_t *db_id)
{
	int err;

	f->request->type = DQLITE_REQUEST_OPEN;
	f->request->open.name = "test.db";
	f->request->open.flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	f->request->open.vfs = f->replication.zName;

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_DB);

	*db_id = f->response->db.id;

	gateway__flushed(f->gateway, f->response);
}

/* Send a prepare request and return the statement ID */
static void __prepare(struct fixture *f,
		      uint32_t db_id,
		      const char *sql,
		      uint32_t *stmt_id)
{
	int err;

	f->request->type = DQLITE_REQUEST_PREPARE;
	f->request->prepare.db_id = db_id;
	f->request->prepare.sql = sql;

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_STMT);

	*stmt_id = f->response->stmt.id;

	gateway__flushed(f->gateway, f->response);
}

/* Send a simple exec request with no parameters. */
static void __exec(struct fixture *f, uint32_t db_id, uint32_t stmt_id)
{
	int err;

	f->request->type = DQLITE_REQUEST_EXEC;
	f->request->exec.db_id = db_id;
	f->request->exec.stmt_id = stmt_id;

	f->request->message.words = 1;
	f->request->message.offset1 = 8;

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

#ifdef DQLITE_EXPERIMENTAL
	if (f->gateway->leader->db->follower == NULL) {
		/* Wait for the open command */
		RAFT_COMMIT;
	}
	RAFT_COMMIT;
#endif

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_RESULT);

	gateway__flushed(f->gateway, f->response);
}

/******************************************************************************
 *
 * Setup and tear down
 *
 ******************************************************************************/

static void *setup(const MunitParameter params[], void *user_data)
{
	struct fixture *f = munit_malloc(sizeof *f);
	struct gateway__cbs callbacks;
	SETUP_HEAP;
	SETUP_SQLITE;
	SETUP_LOGGER;
	SETUP_OPTIONS;
	SETUP_VFS;
#ifdef DQLITE_EXPERIMENTAL
	SETUP_REGISTRY;
	SETUP_RAFT;
	RAFT_BECOME_LEADER;
	SETUP_REPLICATION;
#else
	SETUP_STUB_REPLICATION;
#endif /* DQLITE_EXPERIMENTAL */

	callbacks.ctx = f;
	callbacks.xFlush = fixture_flush_cb;

	f->cluster = test_cluster();
	f->gateway = munit_malloc(sizeof *f->gateway);
	gateway__init(f->gateway, &callbacks, f->cluster, &f->logger,
		      &f->options);
#ifdef DQLITE_EXPERIMENTAL
	f->gateway->registry = &f->registry;
#endif /* DQLITE_EXPERIMENTAL */

	f->request = munit_malloc(sizeof *f->request);

	request_init(f->request);

	return f;
}

static void tear_down(void *data)
{
	struct fixture *f = data;

	request_close(f->request);
	gateway__close(f->gateway);
	test_cluster_close(f->cluster);
#ifdef DQLITE_EXPERIMENTAL
	TEAR_DOWN_REPLICATION;
	TEAR_DOWN_REGISTRY;
	TEAR_DOWN_RAFT;
#else
	TEAR_DOWN_STUB_REPLICATION;
#endif /* DQLITE_EXPERIMENTAL */
	TEAR_DOWN_OPTIONS;
	TEAR_DOWN_VFS;
	TEAR_DOWN_SQLITE;
	TEAR_DOWN_HEAP;
	TEAR_DOWN_LOGGER;
	free(f->gateway);
	free(f->request);
	free(f);
}

/******************************************************************************
 *
 * gateway__handle
 *
 ******************************************************************************/

TEST_SUITE(handle);
TEST_SETUP(handle, setup);
TEST_TEAR_DOWN(handle, tear_down);

/* Handle a leader request. */
TEST_CASE(handle, leader, NULL)
{
	struct fixture *f = data;
	int err;

	(void)params;

	f->request->type = DQLITE_REQUEST_LEADER;

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_SERVER);

	munit_assert_string_equal(f->response->server.address, "127.0.0.1:666");

	/* Notify the gateway that the response has been flushed. This is just
	 * to release any associated memory. */
	gateway__flushed(f->gateway, f->response);

	return MUNIT_OK;
}

/* Handle a client request. */
TEST_CASE(handle, client, NULL)
{
	struct fixture *f = data;
	int err;

	(void)params;

	f->request->type = DQLITE_REQUEST_CLIENT;
	f->request->client.id = 123;

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_WELCOME);

	munit_assert_int(f->response->welcome.heartbeat_timeout, ==, 15000);

	return MUNIT_OK;
}

/* Handle a heartbeat request. */
TEST_CASE(handle, heartbeat, NULL)
{
	struct fixture *f = data;
	int err;

	(void)params;

	f->request->type = DQLITE_REQUEST_HEARTBEAT;
	f->request->heartbeat.timestamp = 12345;

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_SERVERS);

	munit_assert_string_equal(f->response->servers.servers[0].address,
				  "1.2.3.4:666");
	munit_assert_string_equal(f->response->servers.servers[1].address,
				  "5.6.7.8:666");
	munit_assert_ptr_equal(f->response->servers.servers[2].address, NULL);

	/* Notify the gateway that the response has been flushed. This is just
	 * to release any associated memory. */
	gateway__flushed(f->gateway, f->response);

	return MUNIT_OK;
}

#ifndef DQLITE_EXPERIMENTAL

/* If the xServers method of the cluster implementation returns an error, it's
 * propagated to the client. */
TEST_CASE(handle, heartbeat_error, NULL)
{
	struct fixture *f = data;
	int err;

	(void)params;

	f->request->type = DQLITE_REQUEST_HEARTBEAT;
	f->request->heartbeat.timestamp = 12345;

	test_cluster_servers_rc(SQLITE_IOERR_NOT_LEADER);

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);

	munit_assert_int(f->response->failure.code, ==,
			 SQLITE_IOERR_NOT_LEADER);
	munit_assert_string_equal(f->response->failure.message,
				  "failed to get cluster servers");

	return MUNIT_OK;
}

/* If an error occurs while opening a database, it's included in the
 * response. */
TEST_CASE(handle, open_error, NULL)
{
	struct fixture *f = data;
	int err;

	(void)params;

	f->request->type = DQLITE_REQUEST_OPEN;
	f->request->open.name = "test.db";
	f->request->open.flags = SQLITE_OPEN_CREATE;
	f->request->open.vfs = f->replication.zName;

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);

	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_MISUSE);
	munit_assert_string_equal(f->response->failure.message,
				  "bad parameter or other API misuse");

	return MUNIT_OK;
}

static char *test_open_oom_delay[] = {"0", NULL};
static char *test_open_oom_repeat[] = {"1", NULL};

static MunitParameterEnum test_open_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, test_open_oom_delay},
    {TEST_HEAP_FAULT_REPEAT, test_open_oom_repeat},
    {NULL, NULL},
};

/* Out of memory failure modes for the open request. */
TEST_CASE(handle, open_oom, test_open_oom_params)
{
	struct fixture *f = data;
	int rc;

	(void)params;

	test_heap_fault_enable();

	f->request->type = DQLITE_REQUEST_OPEN;
	f->request->open.name = "test.db";
	f->request->open.flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	f->request->open.vfs = f->replication.zName;

	rc = gateway__handle(f->gateway, f->request);
	munit_assert_int(rc, ==, 0);

	munit_assert_ptr_not_null(f->response);

	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_NOMEM);

	return MUNIT_OK;
}

#endif /* !DQLITE_EXPERIMENTAL */

/* Handle an oper request. */
TEST_CASE(handle, open, NULL)
{
	struct fixture *f = data;
	int err;

	(void)params;

	f->request->type = DQLITE_REQUEST_OPEN;
	f->request->open.name = "test.db";
	f->request->open.flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	f->request->open.vfs = f->replication.zName;

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);

	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_DB);
	munit_assert_int(f->response->db.id, ==, 0);

	return MUNIT_OK;
}

/* Attempting to open two databases on the same gateway results in an error. */
TEST_CASE(handle, open_twice, NULL)
{
	struct fixture *f = data;
	int err;

	(void)params;

	f->request->type = DQLITE_REQUEST_OPEN;
	f->request->open.name = "test.db";
	f->request->open.flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	f->request->open.vfs = f->replication.zName;

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	gateway__flushed(f->gateway, f->response);

	f->request->open.name = "test2.db";

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);

	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_BUSY);
	munit_assert_string_equal(
	    f->response->failure.message,
	    "a database for this connection is already open");

	return MUNIT_OK;
}

/* If no registered db matches the provided ID, the request fails. */
TEST_CASE(handle, prepare_bad_db, NULL)
{
	struct fixture *f = data;
	int err;

	(void)params;

	f->request->type = DQLITE_REQUEST_PREPARE;
	f->request->prepare.db_id = 123;
	f->request->prepare.sql = "SELECT 1";

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);

	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_NOTFOUND);
	munit_assert_string_equal(f->response->failure.message,
				  "no db with id 123");

	return MUNIT_OK;
}

/* If the provided SQL statement is invalid, the request fails. */
TEST_CASE(handle, prepare_bad_sql, NULL)
{
	struct fixture *f = data;
	uint32_t db_id;
	int err;

	(void)params;

	__open(f, &db_id);

	f->request->type = DQLITE_REQUEST_PREPARE;
	f->request->prepare.db_id = db_id;
	f->request->prepare.sql = "FOO bar";

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);

	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_ERROR);
	munit_assert_string_equal(f->response->failure.message,
				  "near \"FOO\": syntax error");

	return MUNIT_OK;
}

/* Handle a prepare request. */
TEST_CASE(handle, prepare, NULL)
{
	struct fixture *f = data;
	uint32_t db_id;
	int err;

	(void)params;

	__open(f, &db_id);

	f->request->type = DQLITE_REQUEST_PREPARE;
	f->request->prepare.db_id = db_id;
	f->request->prepare.sql = "SELECT 1";

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);

	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_STMT);
	munit_assert_int(f->response->stmt.id, ==, 0);

	return MUNIT_OK;
}

/* Handle an exec request. */
TEST_CASE(handle, exec, NULL)
{
	struct fixture *f = data;
	uint32_t db_id;
	uint32_t stmt_id;
	int err;

	(void)params;

	__open(f, &db_id);
	__prepare(f, db_id, "CREATE TABLE test (n INT)", &stmt_id);

	f->request->type = DQLITE_REQUEST_EXEC;
	f->request->exec.db_id = db_id;
	f->request->exec.stmt_id = stmt_id;

	f->request->message.words = 1;
	f->request->message.offset1 = 8;

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

#ifdef DQLITE_EXPERIMENTAL
	RAFT_COMMIT;
	RAFT_COMMIT;
#endif /* !DQLITE_EXPERIMENTAL */

	munit_assert_ptr_not_null(f->response);

	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_RESULT);

	return MUNIT_OK;
}

/* Handle an exec request with parameters. */
TEST_CASE(handle, exec_params, NULL)
{
	struct fixture *f = data;
	uint32_t db_id;
	uint32_t stmt_id;
	int err;

	(void)params;

	__open(f, &db_id);

	__prepare(f, db_id, "CREATE TABLE test (n INT)", &stmt_id);
	__exec(f, db_id, stmt_id);

	__prepare(f, db_id, "INSERT INTO test VALUES(?)", &stmt_id);

	f->request->type = DQLITE_REQUEST_EXEC;
	f->request->exec.db_id = db_id;
	f->request->exec.stmt_id = stmt_id;

	f->request->message.words = 3;
	f->request->message.offset1 = 8;

	message__body_put_uint8(&f->request->message, 1); /* N of params */
	message__body_put_uint8(&f->request->message, SQLITE_INTEGER);

	f->request->message.offset1 = 16; /* skip padding bytes */

	message__body_put_int64(&f->request->message, 1); /* param value */

	f->request->message.offset1 = 8; /* rewind */

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

#ifdef DQLITE_EXPERIMENTAL
	RAFT_COMMIT;
#endif /* !DQLITE_EXPERIMENTAL */

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_RESULT);

	return MUNIT_OK;
}

/* If the given statement ID is invalid, an error is returned. */
TEST_CASE(handle, exec_bad_stmt_id, NULL)
{
	struct fixture *f = data;
	uint32_t db_id;
	int err;

	(void)params;

	__open(f, &db_id);

	f->request->type = DQLITE_REQUEST_EXEC;
	f->request->exec.db_id = db_id;
	f->request->exec.stmt_id = 666;

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_NOTFOUND);

	munit_assert_string_equal(f->response->failure.message,
				  "no stmt with id 666");

	return MUNIT_OK;
}

/* If the given bindings are invalid, an error is returned. */
TEST_CASE(handle, exec_bad_params, NULL)
{
	struct fixture *f = data;
	uint32_t db_id;
	uint32_t stmt_id;
	int err;

	(void)params;

	__open(f, &db_id);
	__prepare(f, db_id, "CREATE TABLE test (n INT)", &stmt_id);

	f->request->type = DQLITE_REQUEST_EXEC;
	f->request->exec.db_id = db_id;
	f->request->exec.stmt_id = stmt_id;

	/* Add a parameter even if the query has none. */
	f->request->message.words = 3;
	f->request->message.offset1 = 8;

	message__body_put_uint8(&f->request->message, 1); /* N of params */
	message__body_put_uint8(&f->request->message, SQLITE_INTEGER);

	f->request->message.offset1 = 16; /* skip padding bytes */

	message__body_put_int64(&f->request->message, 1); /* param value */

	f->request->message.offset1 = 8; /* rewind */

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_RANGE);

	munit_assert_string_equal(f->response->failure.message,
				  "column index out of range");

	return MUNIT_OK;
}

#ifndef DQLITE_EXPERIMENTAL

/* If the execution of the statement fails, an error is returned. */
TEST_CASE(handle, exec_fail, NULL)
{
	struct fixture *f = data;
	uint32_t db_id;
	uint32_t stmt_id;
	int err;

	(void)params;

	__open(f, &db_id);

	__prepare(f, db_id, "CREATE TABLE test (n INT, UNIQUE (n))", &stmt_id);
	__exec(f, db_id, stmt_id);

	__prepare(f, db_id, "INSERT INTO test VALUES(1)", &stmt_id);
	__exec(f, db_id, stmt_id);

	__prepare(f, db_id, "INSERT INTO test VALUES(1)", &stmt_id);

	f->request->type = DQLITE_REQUEST_EXEC;
	f->request->exec.db_id = db_id;
	f->request->exec.stmt_id = stmt_id;

	f->request->message.words = 1;
	f->request->message.offset1 = 8;

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);

	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==,
			 SQLITE_CONSTRAINT_UNIQUE);

	munit_assert_string_equal(f->response->failure.message,
				  "UNIQUE constraint failed: test.n");

	return MUNIT_OK;
}

#endif /* !DQLITE_EXPERIMENTAL */

/* Handle a query request. */
TEST_CASE(handle, query, NULL)
{
	struct fixture *f = data;
	uint32_t db_id;
	uint32_t stmt_id;
	int err;

	(void)params;

	__open(f, &db_id);

	__prepare(f, db_id, "CREATE TABLE foo (n INT)", &stmt_id);
	__exec(f, db_id, stmt_id);

	__prepare(f, db_id, "INSERT INTO foo(n) VALUES(-12)", &stmt_id);
	__exec(f, db_id, stmt_id);

	__prepare(f, db_id, "SELECT n FROM foo", &stmt_id);

	f->request->type = DQLITE_REQUEST_QUERY;
	f->request->query.db_id = db_id;
	f->request->query.stmt_id = stmt_id;

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_ROWS);

	/* Four words were written, one with the column count, one with the
	 * column name, one with the row header and one with the row column */
	munit_assert_int(f->response->message.offset1, ==, 32);

	return MUNIT_OK;
}

/* If the given bindings are invalid, an error is returned. */
TEST_CASE(handle, query_bad_params, NULL)
{
	struct fixture *f = data;
	uint32_t db_id;
	uint32_t stmt_id;
	int err;

	(void)params;

	__open(f, &db_id);

	__prepare(f, db_id, "CREATE TABLE foo (n INT)", &stmt_id);
	__exec(f, db_id, stmt_id);

	__prepare(f, db_id, "SELECT n FROM foo", &stmt_id);

	f->request->type = DQLITE_REQUEST_QUERY;
	f->request->exec.db_id = db_id;
	f->request->exec.stmt_id = stmt_id;

	/* Add a parameter even if the query has none. */
	f->request->message.words = 3;
	f->request->message.offset1 = 8;

	message__body_put_uint8(&f->request->message, 1); /* N of params */
	message__body_put_uint8(&f->request->message, SQLITE_INTEGER);

	f->request->message.offset1 = 16; /* skip padding bytes */

	message__body_put_int64(&f->request->message, 1); /* param value */

	f->request->message.offset1 = 8; /* rewind */

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_RANGE);

	munit_assert_string_equal(f->response->failure.message,
				  "column index out of range");

	return MUNIT_OK;
}

/* Handle a finalize request. */
TEST_CASE(handle, finalize, NULL)
{
	struct fixture *f = data;
	uint32_t db_id;
	uint32_t stmt_id;
	int err;

	(void)params;

	__open(f, &db_id);

	__prepare(f, db_id, "CREATE TABLE foo (n INT)", &stmt_id);

	f->request->type = DQLITE_REQUEST_FINALIZE;
	f->request->finalize.db_id = db_id;
	f->request->finalize.stmt_id = stmt_id;

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	return MUNIT_OK;
}

/* Handle an exec sql request. */
TEST_CASE(handle, exec_sql, NULL)
{
	struct fixture *f = data;
	uint32_t db_id;
	uint32_t stmt_id;
	int err;

	(void)params;

	__open(f, &db_id);

	__prepare(f, db_id, "CREATE TABLE foo (n INT, t TEXT, f FLOAT)",
		  &stmt_id);
	__exec(f, db_id, stmt_id);

	f->request->type = DQLITE_REQUEST_EXEC_SQL;
	f->request->exec_sql.db_id = db_id;
	f->request->exec_sql.sql = "INSERT INTO foo(n,t,f) VALUES(?,?,?)";

	f->request->message.words = 5;
	f->request->message.offset1 = 8;

	/* N of params and parm types */
	message__body_put_uint8(&f->request->message, 3);
	message__body_put_uint8(&f->request->message, SQLITE_INTEGER);
	message__body_put_uint8(&f->request->message, SQLITE_TEXT);
	message__body_put_uint8(&f->request->message, SQLITE_NULL);

	f->request->message.offset1 = 16; /* skip padding bytes */

	/* param values */
	message__body_put_int64(&f->request->message, 1);
	message__body_put_text(&f->request->message, "hello");
	message__body_put_int64(&f->request->message, 0);

	f->request->message.offset1 = 8; /* rewind */

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

#ifdef DQLITE_EXPERIMENTAL
	RAFT_COMMIT;
#endif

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_RESULT);

	munit_assert_int(f->response->result.last_insert_id, ==, 1);
	munit_assert_int(f->response->result.rows_affected, ==, 1);

	return MUNIT_OK;
}

/* Handle an exec sql request with multiple statements. */
TEST_CASE(handle, exec_sql_multi, NULL)
{
	struct fixture *f = data;
	uint32_t db_id;
	uint32_t stmt_id;
	int err;

	(void)params;

	__open(f, &db_id);

	f->request->type = DQLITE_REQUEST_EXEC_SQL;
	f->request->exec_sql.db_id = db_id;
	f->request->exec_sql.sql =
	    "CREATE TABLE foo (n INT); CREATE TABLE bar (t TEXT)";

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

#ifdef DQLITE_EXPERIMENTAL
	RAFT_COMMIT;
	RAFT_COMMIT;
	RAFT_COMMIT;
#endif

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_RESULT);

	gateway__flushed(f->gateway, f->response);

	/* Both tables where created. */
	__prepare(f, db_id, "SELECT n FROM foo", &stmt_id);
	__prepare(f, db_id, "SELECT t FROM bar", &stmt_id);

	return MUNIT_OK;
}

/* If the given SQL text is invalid, an error is returned. */
TEST_CASE(handle, exec_sql_bad_sql, NULL)
{
	struct fixture *f = data;
	uint32_t db_id;
	int err;

	(void)params;

	__open(f, &db_id);

	f->request->type = DQLITE_REQUEST_EXEC_SQL;
	f->request->exec_sql.db_id = db_id;
	f->request->exec_sql.sql = "FOO bar";

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_ERROR);

	munit_assert_string_equal(f->response->failure.message,
				  "near \"FOO\": syntax error");

	return MUNIT_OK;
}

/* If the given bindings are invalid, an error is returned. */
TEST_CASE(handle, exec_sql_bad_params, NULL)
{
	struct fixture *f = data;
	uint32_t db_id;
	int err;

	(void)params;

	__open(f, &db_id);

	f->request->type = DQLITE_REQUEST_EXEC_SQL;
	f->request->exec_sql.db_id = db_id;
	f->request->exec_sql.sql = "CREATE TABLE test (n INT)";

	/* Add a parameter even if the query has none. */
	f->request->message.words = 3;
	f->request->message.offset1 = 8;

	message__body_put_uint8(&f->request->message, 1); /* N of params */
	message__body_put_uint8(&f->request->message, SQLITE_INTEGER);

	f->request->message.offset1 = 16; /* skip padding bytes */

	message__body_put_int64(&f->request->message, 1); /* param value */

	f->request->message.offset1 = 8; /* rewind */

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_RANGE);

	munit_assert_string_equal(f->response->failure.message,
				  "column index out of range");

	return MUNIT_OK;
}

#ifndef DQLITE_EXPERIMENTAL

/* If the execution of the statement fails, an error is returned. */
TEST_CASE(handle, exec_sql_error, NULL)
{
	struct fixture *f = data;
	uint32_t db_id;
	uint32_t stmt_id;
	int err;

	(void)params;

	__open(f, &db_id);

	__prepare(f, db_id, "CREATE TABLE foo (n INT, UNIQUE(n))", &stmt_id);
	__exec(f, db_id, stmt_id);

	__prepare(f, db_id, "INSERT INTO foo(n) VALUES(1)", &stmt_id);
	__exec(f, db_id, stmt_id);

	f->request->type = DQLITE_REQUEST_EXEC_SQL;
	f->request->exec_sql.db_id = db_id;
	f->request->exec_sql.sql = "INSERT INTO foo(n) VALUES(1)";

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==,
			 SQLITE_CONSTRAINT_UNIQUE);

	munit_assert_string_equal(f->response->failure.message,
				  "UNIQUE constraint failed: foo.n");

	return MUNIT_OK;
}

#endif /* !DQLITE_EXPERIMENTAL */

/* Handle a query sql request. */
TEST_CASE(handle, query_sql, NULL)
{
	struct fixture *f = data;
	uint32_t db_id;
	uint32_t stmt_id;
	int err;
	uint64_t column_count;
	const char *column_name;
	uint64_t header;
	int64_t n;

	(void)params;

	__open(f, &db_id);

	__prepare(f, db_id, "CREATE TABLE foo (n INT)", &stmt_id);
	__exec(f, db_id, stmt_id);

	__prepare(f, db_id, "INSERT INTO foo(n) VALUES(-12)", &stmt_id);
	__exec(f, db_id, stmt_id);

	f->request->type = DQLITE_REQUEST_QUERY_SQL;
	f->request->query_sql.db_id = db_id;
	f->request->query_sql.sql = "SELECT n FROM foo";

	f->request->message.words = 1;
	f->request->message.offset1 = 8;

	f->response->message.offset1 = 0;

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_ROWS);

	/* Four words were written, one with the column count, one with the
	 * column name, one with the row header and one with the row column */
	munit_assert_int(f->response->message.offset1, ==, 32);

	f->response->message.words = 4;
	f->response->message.offset1 = 0;

	/* Read the column count */
	message__body_get_uint64(&f->response->message, &column_count);
	munit_assert_int(column_count, ==, 1);

	/* Read the column name */
	message__body_get_text(&f->response->message, &column_name);
	munit_assert_string_equal(column_name, "n");

	/* Read the header */
	message__body_get_uint64(&f->response->message, &header);

	munit_assert_int(header, ==, SQLITE_INTEGER);

	/* Read the value */
	message__body_get_int64(&f->response->message, &n);
	munit_assert_int(n, ==, -12);

	return MUNIT_OK;
}

/* If the given SQL text is invalid, an error is returned. */
TEST_CASE(handle, query_sql_bad_sql, NULL)
{
	struct fixture *f = data;
	uint32_t db_id;
	int err;

	(void)params;

	__open(f, &db_id);

	f->request->type = DQLITE_REQUEST_QUERY_SQL;
	f->request->query_sql.db_id = db_id;
	f->request->query_sql.sql = "FOO bar";

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_ERROR);

	munit_assert_string_equal(f->response->failure.message,
				  "near \"FOO\": syntax error");

	return MUNIT_OK;
}

/* If the given bindings are invalid, an error is returned. */
TEST_CASE(handle, query_sql_bad_params, NULL)
{
	struct fixture *f = data;
	uint32_t db_id;
	int err;

	(void)params;

	__open(f, &db_id);

	f->request->type = DQLITE_REQUEST_QUERY_SQL;
	f->request->query_sql.db_id = db_id;
	f->request->query_sql.sql = "SELECT 1";

	/* Add a parameter even if the query has none. */
	f->request->message.words = 3;
	f->request->message.offset1 = 8;

	message__body_put_uint8(&f->request->message, 1); /* N of params */
	message__body_put_uint8(&f->request->message, SQLITE_INTEGER);

	f->request->message.offset1 = 16; /* skip padding bytes */

	message__body_put_int64(&f->request->message, 1); /* param value */

	f->request->message.offset1 = 8; /* rewind */

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_RANGE);

	munit_assert_string_equal(f->response->failure.message,
				  "column index out of range");

	return MUNIT_OK;
}

/* If the given request type is invalid, an error is returned. */
TEST_CASE(handle, invalid_request_type, NULL)
{
	struct fixture *f = data;
	int err;

	(void)params;

	f->request->type = 128;

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_ERROR);

	munit_assert_string_equal(f->response->failure.message,
				  "invalid request type 128");

	return MUNIT_OK;
}

/* If a second request is pushed before the first has completed , an error is
 * returned. */
TEST_CASE(handle, max_requests, NULL)
{
	struct fixture *f = data;
	uint32_t db_id;
	int err;

	(void)params;

	__open(f, &db_id);

	f->request->type = DQLITE_REQUEST_PREPARE;
	f->request->prepare.db_id = db_id;
	f->request->prepare.sql = "SELECT 1";

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, DQLITE_PROTO);

	munit_assert_string_equal(f->gateway->error,
				  "concurrent request limit exceeded");

	return MUNIT_OK;
}

#ifndef DQLITE_EXPERIMENTAL

/* If the number of frames in the WAL reaches the configured threshold, a
 * checkpoint is triggered. */
TEST_CASE(handle, checkpoint, NULL)
{
	struct fixture *f = data;
	sqlite3_file *file = munit_malloc(f->vfs->szOsFile);
	uint32_t db_id;
	uint32_t stmt_id;
	int err;
	int rc;
	int flags;
	sqlite3_int64 size;

	(void)params;

	f->options.checkpoint_threshold = 1;

	__open(f, &db_id);
	__prepare(f, db_id, "BEGIN", &stmt_id);
	__exec(f, db_id, stmt_id);

	f->request->type = DQLITE_REQUEST_EXEC_SQL;
	f->request->exec_sql.db_id = db_id;
	f->request->exec_sql.sql = "CREATE TABLE test (n INT)";

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	gateway__flushed(f->gateway, f->response);

	__prepare(f, db_id, "COMMIT", &stmt_id);
	__exec(f, db_id, stmt_id);

	/* The WAL file got truncated. */
	flags = SQLITE_OPEN_READONLY | SQLITE_OPEN_WAL;
	rc = f->vfs->xOpen(f->vfs, "test.db-wal", file, flags, &flags);
	munit_assert_int(rc, ==, 0);

	rc = file->pMethods->xFileSize(file, &size);
	munit_assert_int(rc, ==, 0);

	munit_assert_int(size, ==, 0);

	free(file);

	return MUNIT_OK;
}

/* If the number of frames in the WAL reaches the configured threshold, but a
 * read transaction holding a shared lock on the WAL is in progress, no
 * checkpoint is triggered. */
TEST_CASE(handle, checkpoint_busy, NULL)
{
	struct fixture *f = data;
	sqlite3_file *file = munit_malloc(f->vfs->szOsFile);
	uint32_t db1_id;
	struct db_ db2;
	struct stmt *stmt2;
	uint64_t last_insert_id;
	uint64_t rows_affected;
	uint32_t stmt_id;
	int err;
	int rc;
	int flags;
	sqlite3_int64 size;

	(void)params;

	__open(f, &db1_id);
	__prepare(f, db1_id, "BEGIN", &stmt_id);
	__exec(f, db1_id, stmt_id);

	f->request->type = DQLITE_REQUEST_EXEC_SQL;
	f->request->exec_sql.db_id = db1_id;
	f->request->exec_sql.sql =
	    "CREATE TABLE test (n INT); INSERT INTO test VALUES(1)";

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	gateway__flushed(f->gateway, f->response);

	__prepare(f, db1_id, "COMMIT", &stmt_id);
	__exec(f, db1_id, stmt_id);

	/* Manually open a new connection to the same database and start a read
	 * transaction. */
	db__init_(&db2);
	flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	rc = db__open(&db2, "test.db", flags, f->options.vfs,
		      f->gateway->options->page_size,
		      f->gateway->options->replication);
	munit_assert_int(rc, ==, 0);

	rc = db__prepare(&db2, "BEGIN", &stmt2);
	munit_assert_int(rc, ==, 0);

	rc = stmt__exec(stmt2, &last_insert_id, &rows_affected);
	munit_assert_int(rc, ==, 0);

	rc = db__prepare(&db2, "SELECT * FROM test", &stmt2);
	munit_assert_int(rc, ==, 0);

	rc = stmt__exec(stmt2, &last_insert_id, &rows_affected);
	munit_assert_int(rc, ==, SQLITE_ROW);

	/* Lower the checkpoint threshold. */
	f->gateway->options->checkpoint_threshold = 1;

	/* Execute a new write transaction on the first connection. */
	__prepare(f, db1_id, "BEGIN", &stmt_id);
	__exec(f, db1_id, stmt_id);

	f->request->type = DQLITE_REQUEST_EXEC_SQL;
	f->request->exec_sql.db_id = db1_id;
	f->request->exec_sql.sql = "INSERT INTO test VALUES(1)";

	err = gateway__handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	gateway__flushed(f->gateway, f->response);

	__prepare(f, db1_id, "COMMIT", &stmt_id);
	__exec(f, db1_id, stmt_id);

	/* The WAL file did not get truncated. */
	flags = SQLITE_OPEN_READONLY | SQLITE_OPEN_WAL;
	rc = f->vfs->xOpen(f->vfs, "test.db-wal", file, flags, &flags);
	munit_assert_int(rc, ==, 0);

	rc = file->pMethods->xFileSize(file, &size);
	munit_assert_int(rc, ==, 0);

	munit_assert_int(format__wal_calc_pages(4096, size), ==, 3);

	db__close_(&db2);

	free(file);

	return MUNIT_OK;
}

/* Interrupt a query request that does not need its statement to be finalized */
TEST_CASE(handle, interrupt, NULL)
{
	struct fixture *f = data;
	uint32_t db_id;
	uint32_t stmt_id;
	int i;
	int rc;
	int ctx;

	(void)params;

	f->gateway->options->checkpoint_threshold = 1;

	__open(f, &db_id);

	__prepare(f, db_id, "BEGIN", &stmt_id);
	__exec(f, db_id, stmt_id);

	__prepare(f, db_id, "CREATE TABLE test (n INT)", &stmt_id);
	__exec(f, db_id, stmt_id);

	for (i = 0; i < 256; i++) {
		__prepare(f, db_id, "INSERT INTO test(n) VALUES(1)", &stmt_id);
		__exec(f, db_id, stmt_id);
	}

	__prepare(f, db_id, "COMMIT", &stmt_id);
	__exec(f, db_id, stmt_id);

	__prepare(f, db_id, "SELECT n FROM test", &stmt_id);

	f->request->type = DQLITE_REQUEST_QUERY;
	f->request->query.db_id = db_id;
	f->request->query.stmt_id = stmt_id;

	f->request->message.words = 2;
	f->request->message.offset1 = 16;

	f->response->message.offset1 = 0;

	rc = gateway__handle(f->gateway, f->request);
	munit_assert_int(rc, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_ROWS);

	gateway__flushed(f->gateway, f->response);

	f->request->type = DQLITE_REQUEST_INTERRUPT;
	f->request->interrupt.db_id = db_id;

	f->request->message.words = 1;
	f->request->message.offset1 = 8;

	f->response->message.offset1 = 0;

	rc = gateway__handle(f->gateway, f->request);
	munit_assert_int(rc, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_EMPTY);

	gateway__flushed(f->gateway, f->response);

	/* The next context index for a database request is 0, meaning that no
	 * pending database request is left. */
	ctx = gateway__ctx_for(f->gateway, DQLITE_REQUEST_EXEC_SQL);
	munit_assert_int(ctx, ==, 0);

	return MUNIT_OK;
}

/* Interrupt a query request that needs its statement to be finalized. */
TEST_CASE(handle, interrupt_finalize, NULL)
{
	struct fixture *f = data;
	uint32_t db_id;
	uint32_t stmt_id;
	int i;
	int ctx;
	int rc;

	(void)params;

	f->gateway->options->checkpoint_threshold = 1;

	__open(f, &db_id);

	__prepare(f, db_id, "BEGIN", &stmt_id);
	__exec(f, db_id, stmt_id);

	__prepare(f, db_id, "CREATE TABLE test (n INT)", &stmt_id);
	__exec(f, db_id, stmt_id);

	for (i = 0; i < 256; i++) {
		__prepare(f, db_id, "INSERT INTO test(n) VALUES(1)", &stmt_id);
		__exec(f, db_id, stmt_id);
	}

	__prepare(f, db_id, "COMMIT", &stmt_id);
	__exec(f, db_id, stmt_id);

	f->request->type = DQLITE_REQUEST_QUERY_SQL;
	f->request->query_sql.db_id = db_id;
	f->request->query_sql.sql = "SELECT n FROM test";

	f->request->message.words = 1;
	f->request->message.offset1 = 8;

	f->response->message.offset1 = 0;

	rc = gateway__handle(f->gateway, f->request);
	munit_assert_int(rc, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_ROWS);

	gateway__flushed(f->gateway, f->response);

	f->request->type = DQLITE_REQUEST_INTERRUPT;
	f->request->interrupt.db_id = db_id;

	f->request->message.words = 1;
	f->request->message.offset1 = 8;

	f->response->message.offset1 = 0;

	rc = gateway__handle(f->gateway, f->request);
	munit_assert_int(rc, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_EMPTY);

	gateway__flushed(f->gateway, f->response);

	/* The next context index for a database request is 0, meaning that no
	 * pending database request is left. */
	ctx = gateway__ctx_for(f->gateway, DQLITE_REQUEST_EXEC_SQL);
	munit_assert_int(ctx, ==, 0);

	return MUNIT_OK;
}

/* An empty response is returned if there is no request in to interrupt. */
TEST_CASE(handle, interrupt_no_request, NULL)
{
	struct fixture *f = data;
	uint32_t db_id;
	int rc;

	(void)params;

	f->gateway->options->checkpoint_threshold = 1;

	__open(f, &db_id);

	f->request->type = DQLITE_REQUEST_INTERRUPT;
	f->request->interrupt.db_id = db_id;

	f->request->message.words = 1;
	f->request->message.offset1 = 8;

	f->response->message.offset1 = 0;

	rc = gateway__handle(f->gateway, f->request);
	munit_assert_int(rc, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_EMPTY);

	return MUNIT_OK;
}

/* An empty response is returned if the current request can't be interrupted. */
TEST_CASE(handle, interrupt_bad_request, NULL)
{
	struct fixture *f = data;
	uint32_t db_id;
	int rc;

	(void)params;

	f->gateway->options->checkpoint_threshold = 1;

	__open(f, &db_id);

	f->request->type = DQLITE_REQUEST_PREPARE;
	f->request->prepare.db_id = db_id;
	f->request->prepare.sql = "SELECT 1";

	rc = gateway__handle(f->gateway, f->request);
	munit_assert_int(rc, ==, 0);

	f->request->type = DQLITE_REQUEST_INTERRUPT;
	f->request->interrupt.db_id = db_id;

	f->request->message.words = 1;
	f->request->message.offset1 = 8;

	f->response->message.offset1 = 0;

	rc = gateway__handle(f->gateway, f->request);
	munit_assert_int(rc, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_EMPTY);

	return MUNIT_OK;
}

#endif /* !DQLITE_EXPERIMENTAL */
