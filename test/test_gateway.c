#include <sqlite3.h>

#include "../include/dqlite.h"

#include "../src/gateway.h"
#include "../src/options.h"
#include "../src/response.h"
#include "../src/format.h"

#include "cluster.h"
#include "replication.h"

#include "case.h"
#include "log.h"
#include "mem.h"

/******************************************************************************
 *
 * Helpers
 *
 ******************************************************************************/

struct fixture {
	sqlite3_wal_replication *replication;
	sqlite3_vfs *            vfs;
	struct dqlite__options * options;
	struct dqlite__gateway * gateway;
	struct dqlite__request * request;
	struct dqlite__response *response;
};

/* Gateway flush callback, saving the response on the fixture. */
static void fixture_flush_cb(void *arg, struct dqlite__response *response)
{
	struct fixture *f = arg;

	munit_assert_ptr_not_null(f);

	f->response = response;
}

/* Send a valid open request and return the database ID */
static void __open(struct fixture *f, uint32_t *db_id)
{
	int err;

	f->request->type       = DQLITE_REQUEST_OPEN;
	f->request->open.name  = "test.db";
	f->request->open.flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	f->request->open.vfs   = f->replication->zName;

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_DB);

	*db_id = f->response->db.id;

	dqlite__gateway_flushed(f->gateway, f->response);
}

/* Send a prepare request and return the statement ID */
static void __prepare(struct fixture *f,
                      uint32_t        db_id,
                      const char *    sql,
                      uint32_t *      stmt_id)
{
	int err;

	f->request->type          = DQLITE_REQUEST_PREPARE;
	f->request->prepare.db_id = db_id;
	f->request->prepare.sql   = sql;

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_STMT);

	*stmt_id = f->response->stmt.id;

	dqlite__gateway_flushed(f->gateway, f->response);
}

/* Send a simple exec request with no parameters. */
static void __exec(struct fixture *f, uint32_t db_id, uint32_t stmt_id)
{
	int err;

	f->request->type         = DQLITE_REQUEST_EXEC;
	f->request->exec.db_id   = db_id;
	f->request->exec.stmt_id = stmt_id;

	f->request->message.words   = 1;
	f->request->message.offset1 = 8;

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_RESULT);

	dqlite__gateway_flushed(f->gateway, f->response);
}

/******************************************************************************
 *
 * Setup and tear down
 *
 ******************************************************************************/

static void *setup(const MunitParameter params[], void *user_data)
{
	struct fixture *           f;
	struct dqlite__gateway_cbs callbacks;
	dqlite_logger *            logger = test_logger();
	int                        rc;

	test_case_setup(params, user_data);

	f = munit_malloc(sizeof *f);

	callbacks.ctx    = f;
	callbacks.xFlush = fixture_flush_cb;

	f->replication = test_replication();

	rc = sqlite3_wal_replication_register(f->replication, 0);
	munit_assert_int(rc, ==, SQLITE_OK);

	f->vfs = dqlite_vfs_create(f->replication->zName, logger);
	munit_assert_ptr_not_null(f->vfs);

	sqlite3_vfs_register(f->vfs, 0);

	f->options = munit_malloc(sizeof *f->options);

	dqlite__options_defaults(f->options);

	f->options->vfs             = "test";
	f->options->wal_replication = "test";

	f->gateway = munit_malloc(sizeof *f->gateway);
	dqlite__gateway_init(
	    f->gateway, &callbacks, test_cluster(), test_logger(), f->options);

#ifdef DQLITE_EXPERIMENTAL
	rc = dqlite__gateway_start(f->gateway, 0);
	munit_assert_int(rc, ==, SQLITE_OK);
#endif /* DQLITE_EXPERIMENTAL */

	f->request = munit_malloc(sizeof *f->request);

	dqlite__request_init(f->request);

	return f;
}

static void tear_down(void *data)
{
	struct fixture *f = data;

	sqlite3_vfs_unregister(f->vfs);

	dqlite__request_close(f->request);
	dqlite__gateway_close(f->gateway);
	dqlite_vfs_destroy(f->vfs);
	sqlite3_wal_replication_unregister(f->replication);

	test_case_tear_down(data);
}

/******************************************************************************
 *
 * dqlite__gateway_handle
 *
 ******************************************************************************/

/* Handle a leader request. */
static MunitResult test_leader(const MunitParameter params[], void *data)
{
	struct fixture *f = data;
	int             err;

	(void)params;

	f->request->type = DQLITE_REQUEST_LEADER;

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_SERVER);

	munit_assert_string_equal(f->response->server.address, "127.0.0.1:666");

	/* Notify the gateway that the response has been flushed. This is just
	 * to release any associated memory. */
	dqlite__gateway_flushed(f->gateway, f->response);

	return MUNIT_OK;
}

/* Handle a client request. */
static MunitResult test_client(const MunitParameter params[], void *data)
{
	struct fixture *f = data;
	int             err;

	(void)params;

	f->request->type      = DQLITE_REQUEST_CLIENT;
	f->request->client.id = 123;

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_WELCOME);

	munit_assert_int(f->response->welcome.heartbeat_timeout, ==, 15000);

	return MUNIT_OK;
}

/* Handle a heartbeat request. */
static MunitResult test_heartbeat(const MunitParameter params[], void *data)
{
	struct fixture *f = data;
	int             err;

	(void)params;

	f->request->type                = DQLITE_REQUEST_HEARTBEAT;
	f->request->heartbeat.timestamp = 12345;

	err = dqlite__gateway_handle(f->gateway, f->request);
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
	dqlite__gateway_flushed(f->gateway, f->response);

	return MUNIT_OK;
}

/* If the xServers method of the cluster implementation returns an error, it's
 * propagated to the client. */
static MunitResult test_heartbeat_error(const MunitParameter params[],
                                        void *               data)
{
	struct fixture *f = data;
	int             err;

	(void)params;

	f->request->type                = DQLITE_REQUEST_HEARTBEAT;
	f->request->heartbeat.timestamp = 12345;

	test_cluster_servers_rc(SQLITE_IOERR_NOT_LEADER);

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);

	munit_assert_int(
	    f->response->failure.code, ==, SQLITE_IOERR_NOT_LEADER);
	munit_assert_string_equal(f->response->failure.message,
	                          "failed to get cluster servers");

	return MUNIT_OK;
}

/* If an error occurs while opening a database, it's included in the
 * response. */
static MunitResult test_open_error(const MunitParameter params[], void *data)
{
	struct fixture *f = data;
	int             err;

	(void)params;

	f->request->type       = DQLITE_REQUEST_OPEN;
	f->request->open.name  = "test.db";
	f->request->open.flags = SQLITE_OPEN_CREATE;
	f->request->open.vfs   = f->replication->zName;

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);

	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_MISUSE);
	munit_assert_string_equal(f->response->failure.message,
	                          "bad parameter or other API misuse");

	return MUNIT_OK;
}

static char *test_open_oom_delay[]  = {"0", NULL};
static char *test_open_oom_repeat[] = {"1", NULL};

static MunitParameterEnum test_open_oom_params[] = {
    {TEST_MEM_FAULT_DELAY_PARAM, test_open_oom_delay},
    {TEST_MEM_FAULT_REPEAT_PARAM, test_open_oom_repeat},
    {NULL, NULL},
};

/* Out of memory failure modes for the open request. */
static MunitResult test_open_oom(const MunitParameter params[], void *data)
{
	struct fixture *f = data;
	int             rc;

	(void)params;

	test_mem_fault_enable();

	f->request->type       = DQLITE_REQUEST_OPEN;
	f->request->open.name  = "test.db";
	f->request->open.flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	f->request->open.vfs   = f->replication->zName;

	rc = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(rc, ==, 0);

	munit_assert_ptr_not_null(f->response);

	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_NOMEM);

	return MUNIT_OK;
}

/* Handle an oper request. */
static MunitResult test_open(const MunitParameter params[], void *data)
{
	struct fixture *f = data;
	int             err;

	(void)params;

	f->request->type       = DQLITE_REQUEST_OPEN;
	f->request->open.name  = "test.db";
	f->request->open.flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	f->request->open.vfs   = f->replication->zName;

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);

	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_DB);
	munit_assert_int(f->response->db.id, ==, 0);

	return MUNIT_OK;
}

/* Attempting to open two databases on the same gateway results in an error. */
static MunitResult test_open_twice(const MunitParameter params[], void *data)
{
	struct fixture *f = data;
	int             err;

	(void)params;

	f->request->type       = DQLITE_REQUEST_OPEN;
	f->request->open.name  = "test.db";
	f->request->open.flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	f->request->open.vfs   = f->replication->zName;

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	dqlite__gateway_flushed(f->gateway, f->response);

	f->request->open.name = "test2.db";

	err = dqlite__gateway_handle(f->gateway, f->request);
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
static MunitResult test_prepare_bad_db(const MunitParameter params[],
                                       void *               data)
{
	struct fixture *f = data;
	int             err;

	(void)params;

	f->request->type          = DQLITE_REQUEST_PREPARE;
	f->request->prepare.db_id = 123;
	f->request->prepare.sql   = "SELECT 1";

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);

	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_NOTFOUND);
	munit_assert_string_equal(f->response->failure.message,
	                          "no db with id 123");

	return MUNIT_OK;
}

/* If the provided SQL statement is invalid, the request fails. */
static MunitResult test_prepare_bad_sql(const MunitParameter params[],
                                        void *               data)
{
	struct fixture *f = data;
	uint32_t        db_id;
	int             err;

	(void)params;

	__open(f, &db_id);

	f->request->type          = DQLITE_REQUEST_PREPARE;
	f->request->prepare.db_id = db_id;
	f->request->prepare.sql   = "FOO bar";

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);

	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_ERROR);
	munit_assert_string_equal(f->response->failure.message,
	                          "near \"FOO\": syntax error");

	return MUNIT_OK;
}

/* Handle a prepare request. */
static MunitResult test_prepare(const MunitParameter params[], void *data)
{
	struct fixture *f = data;
	uint32_t        db_id;
	int             err;

	(void)params;

	__open(f, &db_id);

	f->request->type          = DQLITE_REQUEST_PREPARE;
	f->request->prepare.db_id = db_id;
	f->request->prepare.sql   = "SELECT 1";

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);

	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_STMT);
	munit_assert_int(f->response->stmt.id, ==, 0);

	return MUNIT_OK;
}

/* Handle an exec request. */
static MunitResult test_exec(const MunitParameter params[], void *data)
{
	struct fixture *f = data;
	uint32_t        db_id;
	uint32_t        stmt_id;
	int             err;

	(void)params;

	__open(f, &db_id);
	__prepare(f, db_id, "CREATE TABLE test (n INT)", &stmt_id);

	f->request->type         = DQLITE_REQUEST_EXEC;
	f->request->exec.db_id   = db_id;
	f->request->exec.stmt_id = stmt_id;

	f->request->message.words   = 1;
	f->request->message.offset1 = 8;

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);

	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_RESULT);

	return MUNIT_OK;
}

/* Handle an exec request with parameters. */
static MunitResult test_exec_params(const MunitParameter params[], void *data)
{
	struct fixture *f = data;
	uint32_t        db_id;
	uint32_t        stmt_id;
	int             err;

	(void)params;

	__open(f, &db_id);

	__prepare(f, db_id, "CREATE TABLE test (n INT)", &stmt_id);
	__exec(f, db_id, stmt_id);

	__prepare(f, db_id, "INSERT INTO test VALUES(?)", &stmt_id);

	f->request->type         = DQLITE_REQUEST_EXEC;
	f->request->exec.db_id   = db_id;
	f->request->exec.stmt_id = stmt_id;

	f->request->message.words   = 3;
	f->request->message.offset1 = 8;

	dqlite__message_body_put_uint8(&f->request->message,
	                               1); /* N of params */
	dqlite__message_body_put_uint8(&f->request->message, SQLITE_INTEGER);

	f->request->message.offset1 = 16; /* skip padding bytes */

	dqlite__message_body_put_int64(&f->request->message,
	                               1); /* param value */

	f->request->message.offset1 = 8; /* rewind */

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_RESULT);

	return MUNIT_OK;
}

/* If the given statement ID is invalid, an error is returned. */
static MunitResult test_exec_bad_stmt_id(const MunitParameter params[],
                                         void *               data)
{
	struct fixture *f = data;
	uint32_t        db_id;
	int             err;

	(void)params;

	__open(f, &db_id);

	f->request->type         = DQLITE_REQUEST_EXEC;
	f->request->exec.db_id   = db_id;
	f->request->exec.stmt_id = 666;

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_NOTFOUND);

	munit_assert_string_equal(f->response->failure.message,
	                          "no stmt with id 666");

	return MUNIT_OK;
}

/* If the given bindings are invalid, an error is returned. */
static MunitResult test_exec_bad_params(const MunitParameter params[],
                                        void *               data)
{
	struct fixture *f = data;
	uint32_t        db_id;
	uint32_t        stmt_id;
	int             err;

	(void)params;

	__open(f, &db_id);
	__prepare(f, db_id, "CREATE TABLE test (n INT)", &stmt_id);

	f->request->type         = DQLITE_REQUEST_EXEC;
	f->request->exec.db_id   = db_id;
	f->request->exec.stmt_id = stmt_id;

	/* Add a parameter even if the query has none. */
	f->request->message.words   = 3;
	f->request->message.offset1 = 8;

	dqlite__message_body_put_uint8(&f->request->message,
	                               1); /* N of params */
	dqlite__message_body_put_uint8(&f->request->message, SQLITE_INTEGER);

	f->request->message.offset1 = 16; /* skip padding bytes */

	dqlite__message_body_put_int64(&f->request->message,
	                               1); /* param value */

	f->request->message.offset1 = 8; /* rewind */

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_RANGE);

	munit_assert_string_equal(f->response->failure.message,
	                          "column index out of range");

	return MUNIT_OK;
}

/* If the execution of the statement fails, an error is returned. */
static MunitResult test_exec_fail(const MunitParameter params[], void *data)
{
	struct fixture *f = data;
	uint32_t        db_id;
	uint32_t        stmt_id;
	int             err;

	(void)params;

	__open(f, &db_id);

	__prepare(f, db_id, "CREATE TABLE test (n INT, UNIQUE (n))", &stmt_id);
	__exec(f, db_id, stmt_id);

	__prepare(f, db_id, "INSERT INTO test VALUES(1)", &stmt_id);
	__exec(f, db_id, stmt_id);

	__prepare(f, db_id, "INSERT INTO test VALUES(1)", &stmt_id);

	f->request->type         = DQLITE_REQUEST_EXEC;
	f->request->exec.db_id   = db_id;
	f->request->exec.stmt_id = stmt_id;

	f->request->message.words   = 1;
	f->request->message.offset1 = 8;

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);

	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(
	    f->response->failure.code, ==, SQLITE_CONSTRAINT_UNIQUE);

	munit_assert_string_equal(f->response->failure.message,
	                          "UNIQUE constraint failed: test.n");

	return MUNIT_OK;
}

/* Handle a query request. */
static MunitResult test_query(const MunitParameter params[], void *data)
{
	struct fixture *f = data;
	uint32_t        db_id;
	uint32_t        stmt_id;
	int             err;

	(void)params;

	__open(f, &db_id);

	__prepare(f, db_id, "CREATE TABLE foo (n INT)", &stmt_id);
	__exec(f, db_id, stmt_id);

	__prepare(f, db_id, "INSERT INTO foo(n) VALUES(-12)", &stmt_id);
	__exec(f, db_id, stmt_id);

	__prepare(f, db_id, "SELECT n FROM foo", &stmt_id);

	f->request->type          = DQLITE_REQUEST_QUERY;
	f->request->query.db_id   = db_id;
	f->request->query.stmt_id = stmt_id;

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_ROWS);

	/* Four words were written, one with the column count, one with the
	 * column name, one with the row header and one with the row column */
	munit_assert_int(f->response->message.offset1, ==, 32);

	return MUNIT_OK;
}

/* If the given bindings are invalid, an error is returned. */
static MunitResult test_query_bad_params(const MunitParameter params[],
                                         void *               data)
{
	struct fixture *f = data;
	uint32_t        db_id;
	uint32_t        stmt_id;
	int             err;

	(void)params;

	__open(f, &db_id);

	__prepare(f, db_id, "CREATE TABLE foo (n INT)", &stmt_id);
	__exec(f, db_id, stmt_id);

	__prepare(f, db_id, "SELECT n FROM foo", &stmt_id);

	f->request->type         = DQLITE_REQUEST_QUERY;
	f->request->exec.db_id   = db_id;
	f->request->exec.stmt_id = stmt_id;

	/* Add a parameter even if the query has none. */
	f->request->message.words   = 3;
	f->request->message.offset1 = 8;

	dqlite__message_body_put_uint8(&f->request->message,
	                               1); /* N of params */
	dqlite__message_body_put_uint8(&f->request->message, SQLITE_INTEGER);

	f->request->message.offset1 = 16; /* skip padding bytes */

	dqlite__message_body_put_int64(&f->request->message,
	                               1); /* param value */

	f->request->message.offset1 = 8; /* rewind */

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_RANGE);

	munit_assert_string_equal(f->response->failure.message,
	                          "column index out of range");

	return MUNIT_OK;
}

/* Handle a finalize request. */
static MunitResult test_finalize(const MunitParameter params[], void *data)
{
	struct fixture *f = data;
	uint32_t        db_id;
	uint32_t        stmt_id;
	int             err;

	(void)params;

	__open(f, &db_id);

	__prepare(f, db_id, "CREATE TABLE foo (n INT)", &stmt_id);

	f->request->type             = DQLITE_REQUEST_FINALIZE;
	f->request->finalize.db_id   = db_id;
	f->request->finalize.stmt_id = stmt_id;

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	return MUNIT_OK;
}

/* Handle an exec sql request. */
static MunitResult test_exec_sql(const MunitParameter params[], void *data)
{
	struct fixture *f = data;
	uint32_t        db_id;
	uint32_t        stmt_id;
	int             err;

	(void)params;

	__open(f, &db_id);

	__prepare(
	    f, db_id, "CREATE TABLE foo (n INT, t TEXT, f FLOAT)", &stmt_id);
	__exec(f, db_id, stmt_id);

	f->request->type           = DQLITE_REQUEST_EXEC_SQL;
	f->request->exec_sql.db_id = db_id;
	f->request->exec_sql.sql   = "INSERT INTO foo(n,t,f) VALUES(?,?,?)";

	f->request->message.words   = 5;
	f->request->message.offset1 = 8;

	/* N of params and parm types */
	dqlite__message_body_put_uint8(&f->request->message, 3);
	dqlite__message_body_put_uint8(&f->request->message, SQLITE_INTEGER);
	dqlite__message_body_put_uint8(&f->request->message, SQLITE_TEXT);
	dqlite__message_body_put_uint8(&f->request->message, SQLITE_NULL);

	f->request->message.offset1 = 16; /* skip padding bytes */

	/* param values */
	dqlite__message_body_put_int64(&f->request->message, 1);
	dqlite__message_body_put_text(&f->request->message, "hello");
	dqlite__message_body_put_int64(&f->request->message, 0);

	f->request->message.offset1 = 8; /* rewind */

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_RESULT);

	munit_assert_int(f->response->result.last_insert_id, ==, 1);
	munit_assert_int(f->response->result.rows_affected, ==, 1);

	return MUNIT_OK;
}

/* Handle an exec sql request with multiple statements. */
static MunitResult test_exec_sql_multi(const MunitParameter params[],
                                       void *               data)
{
	struct fixture *f = data;
	uint32_t        db_id;
	uint32_t        stmt_id;
	int             err;

	(void)params;

	__open(f, &db_id);

	f->request->type           = DQLITE_REQUEST_EXEC_SQL;
	f->request->exec_sql.db_id = db_id;
	f->request->exec_sql.sql =
	    "CREATE TABLE foo (n INT); CREATE TABLE bar (t TEXT)";

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_RESULT);

	dqlite__gateway_flushed(f->gateway, f->response);

	/* Both tables where created. */
	__prepare(f, db_id, "SELECT n FROM foo", &stmt_id);
	__prepare(f, db_id, "SELECT t FROM bar", &stmt_id);

	return MUNIT_OK;
}

/* If the given SQL text is invalid, an error is returned. */
static MunitResult test_exec_sql_bad_sql(const MunitParameter params[],
                                         void *               data)
{
	struct fixture *f = data;
	uint32_t        db_id;
	int             err;

	(void)params;

	__open(f, &db_id);

	f->request->type           = DQLITE_REQUEST_EXEC_SQL;
	f->request->exec_sql.db_id = db_id;
	f->request->exec_sql.sql   = "FOO bar";

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_ERROR);

	munit_assert_string_equal(f->response->failure.message,
	                          "near \"FOO\": syntax error");

	return MUNIT_OK;
}

/* If the given bindings are invalid, an error is returned. */
static MunitResult test_exec_sql_bad_params(const MunitParameter params[],
                                            void *               data)
{
	struct fixture *f = data;
	uint32_t        db_id;
	int             err;

	(void)params;

	__open(f, &db_id);

	f->request->type           = DQLITE_REQUEST_EXEC_SQL;
	f->request->exec_sql.db_id = db_id;
	f->request->exec_sql.sql   = "CREATE TABLE test (n INT)";

	/* Add a parameter even if the query has none. */
	f->request->message.words   = 3;
	f->request->message.offset1 = 8;

	dqlite__message_body_put_uint8(&f->request->message,
	                               1); /* N of params */
	dqlite__message_body_put_uint8(&f->request->message, SQLITE_INTEGER);

	f->request->message.offset1 = 16; /* skip padding bytes */

	dqlite__message_body_put_int64(&f->request->message,
	                               1); /* param value */

	f->request->message.offset1 = 8; /* rewind */

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_RANGE);

	munit_assert_string_equal(f->response->failure.message,
	                          "column index out of range");

	return MUNIT_OK;
}

/* If the execution of the statement fails, an error is returned. */
static MunitResult test_exec_sql_error(const MunitParameter params[],
                                       void *               data)
{
	struct fixture *f = data;
	uint32_t        db_id;
	uint32_t        stmt_id;
	int             err;

	(void)params;

	__open(f, &db_id);

	__prepare(f, db_id, "CREATE TABLE foo (n INT, UNIQUE(n))", &stmt_id);
	__exec(f, db_id, stmt_id);

	__prepare(f, db_id, "INSERT INTO foo(n) VALUES(1)", &stmt_id);
	__exec(f, db_id, stmt_id);

	f->request->type           = DQLITE_REQUEST_EXEC_SQL;
	f->request->exec_sql.db_id = db_id;
	f->request->exec_sql.sql   = "INSERT INTO foo(n) VALUES(1)";

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(
	    f->response->failure.code, ==, SQLITE_CONSTRAINT_UNIQUE);

	munit_assert_string_equal(f->response->failure.message,
	                          "UNIQUE constraint failed: foo.n");

	return MUNIT_OK;
}

/* Handle a query sql request. */
static MunitResult test_query_sql(const MunitParameter params[], void *data)
{
	struct fixture *f = data;
	uint32_t        db_id;
	uint32_t        stmt_id;
	int             err;
	uint64_t        column_count;
	const char *    column_name;
	uint64_t        header;
	int64_t         n;

	(void)params;

	__open(f, &db_id);

	__prepare(f, db_id, "CREATE TABLE foo (n INT)", &stmt_id);
	__exec(f, db_id, stmt_id);

	__prepare(f, db_id, "INSERT INTO foo(n) VALUES(-12)", &stmt_id);
	__exec(f, db_id, stmt_id);

	f->request->type            = DQLITE_REQUEST_QUERY_SQL;
	f->request->query_sql.db_id = db_id;
	f->request->query_sql.sql   = "SELECT n FROM foo";

	f->request->message.words   = 1;
	f->request->message.offset1 = 8;

	f->response->message.offset1 = 0;

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_ROWS);

	/* Four words were written, one with the column count, one with the
	 * column name, one with the row header and one with the row column */
	munit_assert_int(f->response->message.offset1, ==, 32);

	f->response->message.words   = 4;
	f->response->message.offset1 = 0;

	/* Read the column count */
	dqlite__message_body_get_uint64(&f->response->message, &column_count);
	munit_assert_int(column_count, ==, 1);

	/* Read the column name */
	dqlite__message_body_get_text(&f->response->message, &column_name);
	munit_assert_string_equal(column_name, "n");

	/* Read the header */
	dqlite__message_body_get_uint64(&f->response->message, &header);

	munit_assert_int(header, ==, SQLITE_INTEGER);

	/* Read the value */
	dqlite__message_body_get_int64(&f->response->message, &n);
	munit_assert_int(n, ==, -12);

	return MUNIT_OK;
}

/* If the given SQL text is invalid, an error is returned. */
static MunitResult test_query_sql_bad_sql(const MunitParameter params[],
                                          void *               data)
{
	struct fixture *f = data;
	uint32_t        db_id;
	int             err;

	(void)params;

	__open(f, &db_id);

	f->request->type            = DQLITE_REQUEST_QUERY_SQL;
	f->request->query_sql.db_id = db_id;
	f->request->query_sql.sql   = "FOO bar";

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_ERROR);

	munit_assert_string_equal(f->response->failure.message,
	                          "near \"FOO\": syntax error");

	return MUNIT_OK;
}

/* If the given bindings are invalid, an error is returned. */
static MunitResult test_query_sql_bad_params(const MunitParameter params[],
                                             void *               data)
{
	struct fixture *f = data;
	uint32_t        db_id;
	int             err;

	(void)params;

	__open(f, &db_id);

	f->request->type            = DQLITE_REQUEST_QUERY_SQL;
	f->request->query_sql.db_id = db_id;
	f->request->query_sql.sql   = "SELECT 1";

	/* Add a parameter even if the query has none. */
	f->request->message.words   = 3;
	f->request->message.offset1 = 8;

	dqlite__message_body_put_uint8(&f->request->message,
	                               1); /* N of params */
	dqlite__message_body_put_uint8(&f->request->message, SQLITE_INTEGER);

	f->request->message.offset1 = 16; /* skip padding bytes */

	dqlite__message_body_put_int64(&f->request->message,
	                               1); /* param value */

	f->request->message.offset1 = 8; /* rewind */

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_RANGE);

	munit_assert_string_equal(f->response->failure.message,
	                          "column index out of range");

	return MUNIT_OK;
}

/* If the given request type is invalid, an error is returned. */
static MunitResult test_invalid_request_type(const MunitParameter params[],
                                             void *               data)
{
	struct fixture *f = data;
	int             err;

	(void)params;

	f->request->type = 128;

	err = dqlite__gateway_handle(f->gateway, f->request);
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
static MunitResult test_max_requests(const MunitParameter params[], void *data)
{
	struct fixture *f = data;
	uint32_t        db_id;
	int             err;

	(void)params;

	__open(f, &db_id);

	f->request->type          = DQLITE_REQUEST_PREPARE;
	f->request->prepare.db_id = db_id;
	f->request->prepare.sql   = "SELECT 1";

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, DQLITE_PROTO);

	munit_assert_string_equal(f->gateway->error,
	                          "concurrent request limit exceeded");

	return MUNIT_OK;
}

/* If the number of frames in the WAL reaches the configured threshold, a
 * checkpoint is triggered. */
static MunitResult test_checkpoint(const MunitParameter params[], void *data)
{
	struct fixture *f    = data;
	sqlite3_file *  file = munit_malloc(f->vfs->szOsFile);
	uint32_t        db_id;
	uint32_t        stmt_id;
	int             err;
	int             rc;
	int             flags;
	sqlite3_int64   size;

	(void)params;

	f->gateway->options->checkpoint_threshold = 1;

	__open(f, &db_id);
	__prepare(f, db_id, "BEGIN", &stmt_id);
	__exec(f, db_id, stmt_id);

	f->request->type           = DQLITE_REQUEST_EXEC_SQL;
	f->request->exec_sql.db_id = db_id;
	f->request->exec_sql.sql   = "CREATE TABLE test (n INT)";

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	dqlite__gateway_flushed(f->gateway, f->response);

	__prepare(f, db_id, "COMMIT", &stmt_id);
	__exec(f, db_id, stmt_id);

	/* The WAL file got truncated. */
	flags = SQLITE_OPEN_READONLY | SQLITE_OPEN_WAL;
	rc    = f->vfs->xOpen(f->vfs, "test.db-wal", file, flags, &flags);
	munit_assert_int(rc, ==, 0);

	rc = file->pMethods->xFileSize(file, &size);
	munit_assert_int(rc, ==, 0);

	munit_assert_int(size, ==, 0);

	return MUNIT_OK;
}

/* If the number of frames in the WAL reaches the configured threshold, but a
 * read transaction holding a shared lock on the WAL is in progress, no
 * checkpoint is triggered. */
static MunitResult test_checkpoint_busy(const MunitParameter params[],
                                        void *               data)
{
	struct fixture *     f    = data;
	sqlite3_file *       file = munit_malloc(f->vfs->szOsFile);
	uint32_t             db1_id;
	struct dqlite__db    db2;
	struct dqlite__stmt *stmt2;
	uint64_t             last_insert_id;
	uint64_t             rows_affected;
	uint32_t             stmt_id;
	int                  err;
	int                  rc;
	int                  flags;
	sqlite3_int64        size;

	(void)params;

	__open(f, &db1_id);
	__prepare(f, db1_id, "BEGIN", &stmt_id);
	__exec(f, db1_id, stmt_id);

	f->request->type           = DQLITE_REQUEST_EXEC_SQL;
	f->request->exec_sql.db_id = db1_id;
	f->request->exec_sql.sql =
	    "CREATE TABLE test (n INT); INSERT INTO test VALUES(1)";

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	dqlite__gateway_flushed(f->gateway, f->response);

	__prepare(f, db1_id, "COMMIT", &stmt_id);
	__exec(f, db1_id, stmt_id);

	/* Manually open a new connection to the same database and start a read
	 * transaction. */
	dqlite__db_init(&db2);
	flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	rc    = dqlite__db_open(&db2,
                             "test.db",
                             flags,
                             f->gateway->options->vfs,
                             f->gateway->options->page_size,
                             f->gateway->options->wal_replication);
	munit_assert_int(rc, ==, 0);

	rc = dqlite__db_prepare(&db2, "BEGIN", &stmt2);
	munit_assert_int(rc, ==, 0);

	rc = dqlite__stmt_exec(stmt2, &last_insert_id, &rows_affected);
	munit_assert_int(rc, ==, 0);

	rc = dqlite__db_prepare(&db2, "SELECT * FROM test", &stmt2);
	munit_assert_int(rc, ==, 0);

	rc = dqlite__stmt_exec(stmt2, &last_insert_id, &rows_affected);
	munit_assert_int(rc, ==, SQLITE_ROW);

	/* Lower the checkpoint threshold. */
	f->gateway->options->checkpoint_threshold = 1;

	/* Execute a new write transaction on the first connection. */
	__prepare(f, db1_id, "BEGIN", &stmt_id);
	__exec(f, db1_id, stmt_id);

	f->request->type           = DQLITE_REQUEST_EXEC_SQL;
	f->request->exec_sql.db_id = db1_id;
	f->request->exec_sql.sql   = "INSERT INTO test VALUES(1)";

	err = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(err, ==, 0);

	dqlite__gateway_flushed(f->gateway, f->response);

	__prepare(f, db1_id, "COMMIT", &stmt_id);
	__exec(f, db1_id, stmt_id);

	/* The WAL file did not get truncated. */
	flags = SQLITE_OPEN_READONLY | SQLITE_OPEN_WAL;
	rc    = f->vfs->xOpen(f->vfs, "test.db-wal", file, flags, &flags);
	munit_assert_int(rc, ==, 0);

	rc = file->pMethods->xFileSize(file, &size);
	munit_assert_int(rc, ==, 0);

	munit_assert_int(dqlite__format_wal_calc_pages(4096, size), ==, 3);

	dqlite__db_close(&db2);

	return MUNIT_OK;
}

/* Interrupt a query request that does not need its statement to be finalized */
static MunitResult test_interrupt(const MunitParameter params[], void *data)
{
	struct fixture *f = data;
	uint32_t        db_id;
	uint32_t        stmt_id;
	int             i;
	int             rc;
	int             ctx;

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

	f->request->type          = DQLITE_REQUEST_QUERY;
	f->request->query.db_id   = db_id;
	f->request->query.stmt_id = stmt_id;

	f->request->message.words   = 2;
	f->request->message.offset1 = 16;

	f->response->message.offset1 = 0;

	rc = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(rc, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_ROWS);

	dqlite__gateway_flushed(f->gateway, f->response);

	f->request->type            = DQLITE_REQUEST_INTERRUPT;
	f->request->interrupt.db_id = db_id;

	f->request->message.words   = 1;
	f->request->message.offset1 = 8;

	f->response->message.offset1 = 0;

	rc = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(rc, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_EMPTY);

	dqlite__gateway_flushed(f->gateway, f->response);

	/* The next context index for a database request is 0, meaning that no
	 * pending database request is left. */
	ctx = dqlite__gateway_ctx_for(f->gateway, DQLITE_REQUEST_EXEC_SQL);
	munit_assert_int(ctx, ==, 0);

	return MUNIT_OK;
}

/* Interrupt a query request that needs its statement to be finalized. */
static MunitResult test_interrupt_finalize(const MunitParameter params[],
                                           void *               data)
{
	struct fixture *f = data;
	uint32_t        db_id;
	uint32_t        stmt_id;
	int             i;
	int             ctx;
	int             rc;

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

	f->request->type            = DQLITE_REQUEST_QUERY_SQL;
	f->request->query_sql.db_id = db_id;
	f->request->query_sql.sql   = "SELECT n FROM test";

	f->request->message.words   = 1;
	f->request->message.offset1 = 8;

	f->response->message.offset1 = 0;

	rc = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(rc, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_ROWS);

	dqlite__gateway_flushed(f->gateway, f->response);

	f->request->type            = DQLITE_REQUEST_INTERRUPT;
	f->request->interrupt.db_id = db_id;

	f->request->message.words   = 1;
	f->request->message.offset1 = 8;

	f->response->message.offset1 = 0;

	rc = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(rc, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_EMPTY);

	dqlite__gateway_flushed(f->gateway, f->response);

	/* The next context index for a database request is 0, meaning that no
	 * pending database request is left. */
	ctx = dqlite__gateway_ctx_for(f->gateway, DQLITE_REQUEST_EXEC_SQL);
	munit_assert_int(ctx, ==, 0);

	return MUNIT_OK;
}

/* An empty response is returned if there is no request in to interrupt. */
static MunitResult test_interrupt_no_request(const MunitParameter params[],
                                             void *               data)
{
	struct fixture *f = data;
	uint32_t        db_id;
	int             rc;

	(void)params;

	f->gateway->options->checkpoint_threshold = 1;

	__open(f, &db_id);

	f->request->type            = DQLITE_REQUEST_INTERRUPT;
	f->request->interrupt.db_id = db_id;

	f->request->message.words   = 1;
	f->request->message.offset1 = 8;

	f->response->message.offset1 = 0;

	rc = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(rc, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_EMPTY);

	return MUNIT_OK;
}

/* An empty response is returned if the current request can't be interrupted. */
static MunitResult test_interrupt_bad_request(const MunitParameter params[],
                                              void *               data)
{
	struct fixture *f = data;
	uint32_t        db_id;
	int             rc;

	(void)params;

	f->gateway->options->checkpoint_threshold = 1;

	__open(f, &db_id);

	f->request->type          = DQLITE_REQUEST_PREPARE;
	f->request->prepare.db_id = db_id;
	f->request->prepare.sql   = "SELECT 1";

	rc = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(rc, ==, 0);

	f->request->type            = DQLITE_REQUEST_INTERRUPT;
	f->request->interrupt.db_id = db_id;

	f->request->message.words   = 1;
	f->request->message.offset1 = 8;

	f->response->message.offset1 = 0;

	rc = dqlite__gateway_handle(f->gateway, f->request);
	munit_assert_int(rc, ==, 0);

	munit_assert_ptr_not_null(f->response);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_EMPTY);

	return MUNIT_OK;
}

static MunitTest dqlite__gateway_handle_tests[] = {
    {"/leader", test_leader, setup, tear_down, 0, NULL},
    {"/client", test_client, setup, tear_down, 0, NULL},
    {"/heartbeat", test_heartbeat, setup, tear_down, 0, NULL},
    {"/heartbeat/error", test_heartbeat_error, setup, tear_down, 0, NULL},
    {"/open/error", test_open_error, setup, tear_down, 0, NULL},
    {"/open/oom", test_open_oom, setup, tear_down, 0, test_open_oom_params},
    {"/open", test_open, setup, tear_down, 0, NULL},
    {"/open/twice", test_open_twice, setup, tear_down, 0, NULL},
    {"/prepare/bad-db", test_prepare_bad_db, setup, tear_down, 0, NULL},
    {"/prepare/bad-sql", test_prepare_bad_sql, setup, tear_down, 0, NULL},
    {"/prepare", test_prepare, setup, tear_down, 0, NULL},
    {"/exec", test_exec, setup, tear_down, 0, NULL},
    {"/exec/params", test_exec_params, setup, tear_down, 0, NULL},
    {"/exec/bad-stmt-id", test_exec_bad_stmt_id, setup, tear_down, 0, NULL},
    {"/exec/bad-params", test_exec_bad_params, setup, tear_down, 0, NULL},
    {"/exec/fail", test_exec_fail, setup, tear_down, 0, NULL},
    {"/query", test_query, setup, tear_down, 0, NULL},
    {"/query/bad-params", test_query_bad_params, setup, tear_down, 0, NULL},
    {"/finalize", test_finalize, setup, tear_down, 0, NULL},
    {"/exec-sql", test_exec_sql, setup, tear_down, 0, NULL},
    {"/exec-sql/multi", test_exec_sql_multi, setup, tear_down, 0, NULL},
    {"/exec-sql/bad-sql", test_exec_sql_bad_sql, setup, tear_down, 0, NULL},
    {"/exec-sql/bad-params",
     test_exec_sql_bad_params,
     setup,
     tear_down,
     0,
     NULL},
    {"/exec-sql/error", test_exec_sql_error, setup, tear_down, 0, NULL},
    {"/query-sql", test_query_sql, setup, tear_down, 0, NULL},
    {"/query-sql/bad-sql", test_query_sql_bad_sql, setup, tear_down, 0, NULL},
    {"/query-sql/bad-params",
     test_query_sql_bad_params,
     setup,
     tear_down,
     0,
     NULL},
    {"/invalid-request-type",
     test_invalid_request_type,
     setup,
     tear_down,
     0,
     NULL},
    {"/max-requests", test_max_requests, setup, tear_down, 0, NULL},
    {"/checkpoint", test_checkpoint, setup, tear_down, 0, NULL},
    {"/checkpoint-busy", test_checkpoint_busy, setup, tear_down, 0, NULL},
    {"/interrupt", test_interrupt, setup, tear_down, 0, NULL},
    {"/interrupt/finalize", test_interrupt_finalize, setup, tear_down, 0, NULL},
    {"/interrupt/no-request",
     test_interrupt_no_request,
     setup,
     tear_down,
     0,
     NULL},
    {"/interrupt/bad-request",
     test_interrupt_bad_request,
     setup,
     tear_down,
     0,
     NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * Suite
 *
 ******************************************************************************/

MunitSuite dqlite__gateway_suites[] = {
    {"_handle", dqlite__gateway_handle_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE},
    {NULL, NULL, NULL, 0, MUNIT_SUITE_OPTION_NONE},
};
