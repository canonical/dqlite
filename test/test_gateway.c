#include <sqlite3.h>

#include "../include/dqlite.h"
#include "../src/gateway.h"
#include "../src/options.h"
#include "../src/response.h"

#include "cluster.h"
#include "replication.h"

#include "leak.h"
#include "munit.h"

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

/* Send a valid open request and return the database ID */
static void fixture_open(struct fixture *f, uint32_t *db_id) {
	int err;

	f->request->type       = DQLITE_REQUEST_OPEN;
	f->request->open.name  = "test.db";
	f->request->open.flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	f->request->open.vfs   = f->replication->zName;

	err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_equal(f->response, NULL);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_DB);

	*db_id = f->response->db.id;

	dqlite__gateway_finish(f->gateway, f->response);
}

/* Send a prepare request and return the statement ID */
static void fixture_prepare(struct fixture *f,
                            uint32_t        db_id,
                            const char *    sql,
                            uint32_t *      stmt_id) {
	int err;

	f->request->type          = DQLITE_REQUEST_PREPARE;
	f->request->prepare.db_id = db_id;
	f->request->prepare.sql   = sql;

	err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_equal(f->response, NULL);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_STMT);

	*stmt_id = f->response->stmt.id;

	dqlite__gateway_finish(f->gateway, f->response);
}

/* Send a simple exec request with no parameters. */
static void fixture_exec(struct fixture *f, uint32_t db_id, uint32_t stmt_id) {
	int err;

	f->request->type         = DQLITE_REQUEST_EXEC;
	f->request->exec.db_id   = db_id;
	f->request->exec.stmt_id = stmt_id;

	f->request->message.words   = 1;
	f->request->message.offset1 = 8;

	err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_equal(f->response, NULL);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_RESULT);

	dqlite__gateway_finish(f->gateway, f->response);
}

/******************************************************************************
 *
 * Setup and tear down
 *
 ******************************************************************************/

static void *setup(const MunitParameter params[], void *user_data) {
	struct fixture *f;
	int             rc;

	(void)params;
	(void)user_data;

	/* The replication code relies on mutexes being disabled */
	rc = sqlite3_config(SQLITE_CONFIG_SINGLETHREAD);
	munit_assert_int(rc, ==, SQLITE_OK);

	f = munit_malloc(sizeof *f);

	f->replication = test_replication();

	rc = sqlite3_wal_replication_register(f->replication, 0);
	munit_assert_int(rc, ==, SQLITE_OK);

	f->vfs = dqlite_vfs_create(f->replication->zName);
	munit_assert_ptr_not_null(f->vfs);

	sqlite3_vfs_register(f->vfs, 0);

	f->options = munit_malloc(sizeof *f->options);

	dqlite__options_defaults(f->options);

	f->options->vfs             = "test";
	f->options->wal_replication = "test";

	f->gateway = munit_malloc(sizeof *f->gateway);
	dqlite__gateway_init(f->gateway, test_cluster(), f->options);

	f->request = munit_malloc(sizeof *f->request);
	dqlite__request_init(f->request);

	return f;
}

static void tear_down(void *data) {
	struct fixture *f = data;

	sqlite3_vfs_unregister(f->vfs);

	dqlite__request_close(f->request);
	dqlite__gateway_close(f->gateway);
	dqlite_vfs_destroy(f->vfs);
	sqlite3_wal_replication_unregister(f->replication);

	test_assert_no_leaks();
}

/******************************************************************************
 *
 * Tests for dqlite__gateway_handle
 *
 ******************************************************************************/

/* Handle a leader request. */
static MunitResult test_leader(const MunitParameter params[], void *data) {
	struct fixture *f = data;
	int             err;

	(void)params;

	f->request->type = DQLITE_REQUEST_LEADER;

	err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_equal(f->response, NULL);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_SERVER);

	munit_assert_string_equal(f->response->server.address, "127.0.0.1:666");

	/* Invoke the reset callback, as it would be if the response was then
	 * being encoded. */
	f->response->xReset(f->response);

	return MUNIT_OK;
}

/* Handle a client request. */
static MunitResult test_client(const MunitParameter params[], void *data) {
	struct fixture *f = data;
	int             err;

	(void)params;

	f->request->type      = DQLITE_REQUEST_CLIENT;
	f->request->client.id = 123;

	err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_equal(f->response, NULL);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_WELCOME);

	munit_assert_int(f->response->welcome.heartbeat_timeout, ==, 15000);

	return MUNIT_OK;
}

/* Handle a heartbeat request. */
static MunitResult test_heartbeat(const MunitParameter params[], void *data) {
	struct fixture *f = data;
	int             err;

	(void)params;

	f->request->type                = DQLITE_REQUEST_HEARTBEAT;
	f->request->heartbeat.timestamp = 12345;

	err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_equal(f->response, NULL);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_SERVERS);

	munit_assert_string_equal(f->response->servers.servers[0].address,
	                          "1.2.3.4:666");
	munit_assert_string_equal(f->response->servers.servers[1].address,
	                          "5.6.7.8:666");
	munit_assert_ptr_equal(f->response->servers.servers[2].address, NULL);

	/* Invoke the reset callback, as it would be if the response was then
	 * being encoded. */
	f->response->xReset(f->response);

	return MUNIT_OK;
}

/* If the xServers method of the cluster implementation returns an error, it's
 * propagated to the client. */
static MunitResult test_heartbeat_error(const MunitParameter params[], void *data) {
	struct fixture *f = data;
	int             err;

	(void)params;

	f->request->type                = DQLITE_REQUEST_HEARTBEAT;
	f->request->heartbeat.timestamp = 12345;

	test_cluster_servers_rc(SQLITE_IOERR_NOT_LEADER);

	err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_equal(f->response, NULL);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);

	munit_assert_int(f->response->failure.code, ==, SQLITE_IOERR_NOT_LEADER);
	munit_assert_string_equal(f->response->failure.message,
	                          "failed to get cluster servers");

	return MUNIT_OK;
}

/* If an error occurs while opening a database, it's included in the
 * response. */
static MunitResult test_open_error(const MunitParameter params[], void *data) {
	struct fixture *f = data;
	int             err;

	(void)params;

	f->request->type       = DQLITE_REQUEST_OPEN;
	f->request->open.name  = "test.db";
	f->request->open.flags = SQLITE_OPEN_CREATE;
	f->request->open.vfs   = f->replication->zName;

	err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_equal(f->response, NULL);

	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_MISUSE);
	munit_assert_string_equal(f->response->failure.message,
	                          "bad parameter or other API misuse");

	return MUNIT_OK;
}

/* Handle an oper request. */
static MunitResult test_open(const MunitParameter params[], void *data) {
	struct fixture *f = data;
	int             err;

	(void)params;

	f->request->type       = DQLITE_REQUEST_OPEN;
	f->request->open.name  = "test.db";
	f->request->open.flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	f->request->open.vfs   = f->replication->zName;

	err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_equal(f->response, NULL);

	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_DB);
	munit_assert_int(f->response->db.id, ==, 0);

	return MUNIT_OK;
}

/* If no registered db matches the provided ID, the request fails. */
static MunitResult test_prepare_bad_db(const MunitParameter params[], void *data) {
	struct fixture *f = data;
	int             err;

	(void)params;

	f->request->type          = DQLITE_REQUEST_PREPARE;
	f->request->prepare.db_id = 123;
	f->request->prepare.sql   = "SELECT 1";

	err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_equal(f->response, NULL);

	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_NOTFOUND);
	munit_assert_string_equal(f->response->failure.message, "no db with id 123");

	return MUNIT_OK;
}

/* If the provided SQL statement is invalid, the request fails. */
static MunitResult test_prepare_bad_sql(const MunitParameter params[], void *data) {
	struct fixture *f = data;
	uint32_t        db_id;
	int             err;

	(void)params;

	fixture_open(f, &db_id);

	f->request->type          = DQLITE_REQUEST_PREPARE;
	f->request->prepare.db_id = db_id;
	f->request->prepare.sql   = "FOO bar";

	err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_equal(f->response, NULL);

	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_ERROR);
	munit_assert_string_equal(f->response->failure.message,
	                          "near \"FOO\": syntax error");

	return MUNIT_OK;
}

/* Handle a prepare request. */
static MunitResult test_prepare(const MunitParameter params[], void *data) {
	struct fixture *f = data;
	uint32_t        db_id;
	int             err;

	(void)params;

	fixture_open(f, &db_id);

	f->request->type          = DQLITE_REQUEST_PREPARE;
	f->request->prepare.db_id = db_id;
	f->request->prepare.sql   = "SELECT 1";

	err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_equal(f->response, NULL);

	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_STMT);
	munit_assert_int(f->response->stmt.id, ==, 0);

	return MUNIT_OK;
}

/* Handle an exec request. */
static MunitResult test_exec(const MunitParameter params[], void *data) {
	struct fixture *f = data;
	uint32_t        db_id;
	uint32_t        stmt_id;
	int             err;

	(void)params;

	fixture_open(f, &db_id);
	fixture_prepare(f, db_id, "CREATE TABLE test (n INT)", &stmt_id);

	f->request->type         = DQLITE_REQUEST_EXEC;
	f->request->exec.db_id   = db_id;
	f->request->exec.stmt_id = stmt_id;

	f->request->message.words   = 1;
	f->request->message.offset1 = 8;

	err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_equal(f->response, NULL);

	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_RESULT);

	return MUNIT_OK;
}

/* Handle an exec request with parameters. */
static MunitResult test_exec_params(const MunitParameter params[], void *data) {
	struct fixture *f = data;
	uint32_t        db_id;
	uint32_t        stmt_id;
	int             err;

	(void)params;

	fixture_open(f, &db_id);

	fixture_prepare(f, db_id, "CREATE TABLE test (n INT)", &stmt_id);
	fixture_exec(f, db_id, stmt_id);

	fixture_prepare(f, db_id, "INSERT INTO test VALUES(?)", &stmt_id);

	f->request->type         = DQLITE_REQUEST_EXEC;
	f->request->exec.db_id   = db_id;
	f->request->exec.stmt_id = stmt_id;

	f->request->message.words   = 3;
	f->request->message.offset1 = 8;

	dqlite__message_body_put_uint8(&f->request->message, 1); /* N of params */
	dqlite__message_body_put_uint8(&f->request->message, SQLITE_INTEGER);

	f->request->message.offset1 = 16; /* skip padding bytes */

	dqlite__message_body_put_int64(&f->request->message, 1); /* param value */

	f->request->message.offset1 = 8; /* rewind */

	err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_equal(f->response, NULL);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_RESULT);

	return MUNIT_OK;
}

/* If the given statement ID is invalid, an error is returned. */
static MunitResult test_exec_bad_stmt_id(const MunitParameter params[], void *data) {
	struct fixture *f = data;
	uint32_t        db_id;
	int             err;

	(void)params;

	fixture_open(f, &db_id);

	f->request->type         = DQLITE_REQUEST_EXEC;
	f->request->exec.db_id   = db_id;
	f->request->exec.stmt_id = 666;

	err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_equal(f->response, NULL);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_NOTFOUND);

	munit_assert_string_equal(f->response->failure.message,
	                          "no stmt with id 666");

	return MUNIT_OK;
}

/* If the given bindings are invalid, an error is returned. */
static MunitResult test_exec_bad_params(const MunitParameter params[], void *data) {
	struct fixture *f = data;
	uint32_t        db_id;
	uint32_t        stmt_id;
	int             err;

	(void)params;

	fixture_open(f, &db_id);
	fixture_prepare(f, db_id, "CREATE TABLE test (n INT)", &stmt_id);

	f->request->type         = DQLITE_REQUEST_EXEC;
	f->request->exec.db_id   = db_id;
	f->request->exec.stmt_id = stmt_id;

	/* Add a parameter even if the query has none. */
	f->request->message.words   = 3;
	f->request->message.offset1 = 8;

	dqlite__message_body_put_uint8(&f->request->message, 1); /* N of params */
	dqlite__message_body_put_uint8(&f->request->message, SQLITE_INTEGER);

	f->request->message.offset1 = 16; /* skip padding bytes */

	dqlite__message_body_put_int64(&f->request->message, 1); /* param value */

	f->request->message.offset1 = 8; /* rewind */

	err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_equal(f->response, NULL);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_RANGE);

	munit_assert_string_equal(f->response->failure.message,
	                          "column index out of range");

	return MUNIT_OK;
}

/* If the execution of the statement fails, an error is returned. */
static MunitResult test_exec_fail(const MunitParameter params[], void *data) {
	struct fixture *f = data;
	uint32_t        db_id;
	uint32_t        stmt_id;
	int             err;

	(void)params;

	fixture_open(f, &db_id);

	fixture_prepare(f, db_id, "CREATE TABLE test (n INT, UNIQUE (n))", &stmt_id);
	fixture_exec(f, db_id, stmt_id);

	fixture_prepare(f, db_id, "INSERT INTO test VALUES(1)", &stmt_id);
	fixture_exec(f, db_id, stmt_id);

	fixture_prepare(f, db_id, "INSERT INTO test VALUES(1)", &stmt_id);

	f->request->type         = DQLITE_REQUEST_EXEC;
	f->request->exec.db_id   = db_id;
	f->request->exec.stmt_id = stmt_id;

	f->request->message.words   = 1;
	f->request->message.offset1 = 8;

	err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_equal(f->response, NULL);

	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_CONSTRAINT_UNIQUE);

	munit_assert_string_equal(f->response->failure.message,
	                          "UNIQUE constraint failed: test.n");

	return MUNIT_OK;
}

/* Handle a query request. */
static MunitResult test_query(const MunitParameter params[], void *data) {
	struct fixture *f = data;
	uint32_t        db_id;
	uint32_t        stmt_id;
	int             err;

	(void)params;

	fixture_open(f, &db_id);

	fixture_prepare(f, db_id, "CREATE TABLE foo (n INT)", &stmt_id);
	fixture_exec(f, db_id, stmt_id);

	fixture_prepare(f, db_id, "INSERT INTO foo(n) VALUES(-12)", &stmt_id);
	fixture_exec(f, db_id, stmt_id);

	fixture_prepare(f, db_id, "SELECT n FROM foo", &stmt_id);

	f->request->type          = DQLITE_REQUEST_QUERY;
	f->request->query.db_id   = db_id;
	f->request->query.stmt_id = stmt_id;

	err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_equal(f->response, NULL);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_ROWS);

	/* Four words were written, one with the column count, one with the
	 * column name, one with the row header and one with the row column */
	munit_assert_int(f->response->message.offset1, ==, 32);

	return MUNIT_OK;
}

/* If the given bindings are invalid, an error is returned. */
static MunitResult test_query_bad_params(const MunitParameter params[], void *data) {
	struct fixture *f = data;
	uint32_t        db_id;
	uint32_t        stmt_id;
	int             err;

	(void)params;

	fixture_open(f, &db_id);

	fixture_prepare(f, db_id, "CREATE TABLE foo (n INT)", &stmt_id);
	fixture_exec(f, db_id, stmt_id);

	fixture_prepare(f, db_id, "SELECT n FROM foo", &stmt_id);

	f->request->type         = DQLITE_REQUEST_QUERY;
	f->request->exec.db_id   = db_id;
	f->request->exec.stmt_id = stmt_id;

	/* Add a parameter even if the query has none. */
	f->request->message.words   = 3;
	f->request->message.offset1 = 8;

	dqlite__message_body_put_uint8(&f->request->message, 1); /* N of params */
	dqlite__message_body_put_uint8(&f->request->message, SQLITE_INTEGER);

	f->request->message.offset1 = 16; /* skip padding bytes */

	dqlite__message_body_put_int64(&f->request->message, 1); /* param value */

	f->request->message.offset1 = 8; /* rewind */

	err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_equal(f->response, NULL);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_RANGE);

	munit_assert_string_equal(f->response->failure.message,
	                          "column index out of range");

	return MUNIT_OK;
}

/* Handle a finalize request. */
static MunitResult test_finalize(const MunitParameter params[], void *data) {
	struct fixture *f = data;
	uint32_t        db_id;
	uint32_t        stmt_id;
	int             err;

	(void)params;

	fixture_open(f, &db_id);

	fixture_prepare(f, db_id, "CREATE TABLE foo (n INT)", &stmt_id);

	f->request->type             = DQLITE_REQUEST_FINALIZE;
	f->request->finalize.db_id   = db_id;
	f->request->finalize.stmt_id = stmt_id;

	err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
	munit_assert_int(err, ==, 0);

	return MUNIT_OK;
}

/* Handle an exec sql request. */
static MunitResult test_exec_sql(const MunitParameter params[], void *data) {
	struct fixture *f = data;
	uint32_t        db_id;
	uint32_t        stmt_id;
	int             err;

	(void)params;

	fixture_open(f, &db_id);

	fixture_prepare(
	    f, db_id, "CREATE TABLE foo (n INT, t TEXT, f FLOAT)", &stmt_id);
	fixture_exec(f, db_id, stmt_id);

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

	err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_equal(f->response, NULL);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_RESULT);

	munit_assert_int(f->response->result.last_insert_id, ==, 1);
	munit_assert_int(f->response->result.rows_affected, ==, 1);

	return MUNIT_OK;
}

/* Handle an exec sql request with multiple statements. */
static MunitResult test_exec_sql_multi(const MunitParameter params[], void *data) {
	struct fixture *f = data;
	uint32_t        db_id;
	uint32_t        stmt_id;
	int             err;

	(void)params;

	fixture_open(f, &db_id);

	f->request->type           = DQLITE_REQUEST_EXEC_SQL;
	f->request->exec_sql.db_id = db_id;
	f->request->exec_sql.sql =
	    "CREATE TABLE foo (n INT); CREATE TABLE bar (t TEXT)";

	err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_equal(f->response, NULL);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_RESULT);

	/* Both tables where created. */
	fixture_prepare(f, db_id, "SELECT n FROM foo", &stmt_id);
	fixture_prepare(f, db_id, "SELECT t FROM bar", &stmt_id);

	return MUNIT_OK;
}

/* If the given SQL text is invalid, an error is returned. */
static MunitResult test_exec_sql_bad_sql(const MunitParameter params[], void *data) {
	struct fixture *f = data;
	uint32_t        db_id;
	int             err;

	(void)params;

	fixture_open(f, &db_id);

	f->request->type           = DQLITE_REQUEST_EXEC_SQL;
	f->request->exec_sql.db_id = db_id;
	f->request->exec_sql.sql   = "FOO bar";

	err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_equal(f->response, NULL);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_ERROR);

	munit_assert_string_equal(f->response->failure.message,
	                          "near \"FOO\": syntax error");

	return MUNIT_OK;
}

/* If the given bindings are invalid, an error is returned. */
static MunitResult test_exec_sql_bad_params(const MunitParameter params[],
                                            void *               data) {
	struct fixture *f = data;
	uint32_t        db_id;
	int             err;

	(void)params;

	fixture_open(f, &db_id);

	f->request->type           = DQLITE_REQUEST_EXEC_SQL;
	f->request->exec_sql.db_id = db_id;
	f->request->exec_sql.sql   = "CREATE TABLE test (n INT)";

	/* Add a parameter even if the query has none. */
	f->request->message.words   = 3;
	f->request->message.offset1 = 8;

	dqlite__message_body_put_uint8(&f->request->message, 1); /* N of params */
	dqlite__message_body_put_uint8(&f->request->message, SQLITE_INTEGER);

	f->request->message.offset1 = 16; /* skip padding bytes */

	dqlite__message_body_put_int64(&f->request->message, 1); /* param value */

	f->request->message.offset1 = 8; /* rewind */

	err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_equal(f->response, NULL);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_RANGE);

	munit_assert_string_equal(f->response->failure.message,
	                          "column index out of range");

	return MUNIT_OK;
}

/* If the execution of the statement fails, an error is returned. */
static MunitResult test_exec_sql_error(const MunitParameter params[], void *data) {
	struct fixture *f = data;
	uint32_t        db_id;
	uint32_t        stmt_id;
	int             err;

	(void)params;

	fixture_open(f, &db_id);

	fixture_prepare(f, db_id, "CREATE TABLE foo (n INT, UNIQUE(n))", &stmt_id);
	fixture_exec(f, db_id, stmt_id);

	fixture_prepare(f, db_id, "INSERT INTO foo(n) VALUES(1)", &stmt_id);
	fixture_exec(f, db_id, stmt_id);

	f->request->type           = DQLITE_REQUEST_EXEC_SQL;
	f->request->exec_sql.db_id = db_id;
	f->request->exec_sql.sql   = "INSERT INTO foo(n) VALUES(1)";

	err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_equal(f->response, NULL);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_CONSTRAINT_UNIQUE);

	munit_assert_string_equal(f->response->failure.message,
	                          "UNIQUE constraint failed: foo.n");

	return MUNIT_OK;
}

/* Handle a query sql request. */
static MunitResult test_query_sql(const MunitParameter params[], void *data) {
	struct fixture *f = data;
	uint32_t        db_id;
	uint32_t        stmt_id;
	int             err;
	uint64_t        column_count;
	const char *    column_name;
	uint64_t        header;
	int64_t         n;

	(void)params;

	fixture_open(f, &db_id);

	fixture_prepare(f, db_id, "CREATE TABLE foo (n INT)", &stmt_id);
	fixture_exec(f, db_id, stmt_id);

	fixture_prepare(f, db_id, "INSERT INTO foo(n) VALUES(-12)", &stmt_id);
	fixture_exec(f, db_id, stmt_id);

	f->request->type            = DQLITE_REQUEST_QUERY_SQL;
	f->request->query_sql.db_id = db_id;
	f->request->query_sql.sql   = "SELECT n FROM foo";

	f->request->message.words   = 1;
	f->request->message.offset1 = 8;

	f->response->message.offset1 = 0;

	err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_equal(f->response, NULL);
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
                                          void *               data) {
	struct fixture *f = data;
	uint32_t        db_id;
	int             err;

	(void)params;

	fixture_open(f, &db_id);

	f->request->type            = DQLITE_REQUEST_QUERY_SQL;
	f->request->query_sql.db_id = db_id;
	f->request->query_sql.sql   = "FOO bar";

	err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_equal(f->response, NULL);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_ERROR);

	munit_assert_string_equal(f->response->failure.message,
	                          "near \"FOO\": syntax error");

	return MUNIT_OK;
}

/* If the given bindings are invalid, an error is returned. */
static MunitResult test_query_sql_bad_params(const MunitParameter params[],
                                             void *               data) {
	struct fixture *f = data;
	uint32_t        db_id;
	int             err;

	(void)params;

	fixture_open(f, &db_id);

	f->request->type            = DQLITE_REQUEST_QUERY_SQL;
	f->request->query_sql.db_id = db_id;
	f->request->query_sql.sql   = "SELECT 1";

	/* Add a parameter even if the query has none. */
	f->request->message.words   = 3;
	f->request->message.offset1 = 8;

	dqlite__message_body_put_uint8(&f->request->message, 1); /* N of params */
	dqlite__message_body_put_uint8(&f->request->message, SQLITE_INTEGER);

	f->request->message.offset1 = 16; /* skip padding bytes */

	dqlite__message_body_put_int64(&f->request->message, 1); /* param value */

	f->request->message.offset1 = 8; /* rewind */

	err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_equal(f->response, NULL);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_RANGE);

	munit_assert_string_equal(f->response->failure.message,
	                          "column index out of range");

	return MUNIT_OK;
}

/* If the given request type is invalid, an error is returned. */
static MunitResult test_invalid_request_type(const MunitParameter params[],
                                             void *               data) {
	struct fixture *f = data;
	int             err;

	(void)params;

	f->request->type = 128;

	err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_not_equal(f->response, NULL);
	munit_assert_int(f->response->type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response->failure.code, ==, SQLITE_ERROR);

	munit_assert_string_equal(f->response->failure.message,
	                          "invalid request type 128");

	return MUNIT_OK;
}

/* If the maximum number of concurrent requests is reached, an error is returned. */
static MunitResult test_max_requests(const MunitParameter params[], void *data) {
	struct fixture *f = data;
	int             err;
	int             i;

	(void)params;

	for (i = 0; i < DQLITE__GATEWAY_MAX_REQUESTS; i++) {
		f->request->type = DQLITE_REQUEST_CLIENT;

		err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
		munit_assert_int(err, ==, 0);
	}

	f->request->type = DQLITE_REQUEST_CLIENT;

	err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
	munit_assert_int(err, ==, DQLITE_PROTO);

	munit_assert_string_equal(f->gateway->error,
	                          "concurrent request limit exceeded");

	return MUNIT_OK;
}

/* If the number of frames in the WAL reaches the configured threshold, a checkpoint
 * is triggered. */
static MunitResult test_checkpoint(const MunitParameter params[], void *data) {
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

	fixture_open(f, &db_id);
	fixture_prepare(f, db_id, "BEGIN", &stmt_id);
	fixture_exec(f, db_id, stmt_id);

	f->request->type           = DQLITE_REQUEST_EXEC_SQL;
	f->request->exec_sql.db_id = db_id;
	f->request->exec_sql.sql   = "CREATE TABLE test (n INT)";

	err = dqlite__gateway_handle(f->gateway, f->request, &f->response);
	munit_assert_int(err, ==, 0);

	fixture_prepare(f, db_id, "COMMIT", &stmt_id);
	fixture_exec(f, db_id, stmt_id);

	/* The WAL file got truncated. */
	flags = SQLITE_OPEN_READONLY | SQLITE_OPEN_WAL;
	rc    = f->vfs->xOpen(f->vfs, "test.db-wal", file, flags, &flags);
	munit_assert_int(rc, ==, 0);

	rc = file->pMethods->xFileSize(file, &size);
	munit_assert_int(rc, ==, 0);

	munit_assert_int(size, ==, 0);

	return MUNIT_OK;
}

static MunitTest dqlite__gateway_handle_tests[] = {
    {"/leader", test_leader, setup, tear_down, 0, NULL},
    {"/client", test_client, setup, tear_down, 0, NULL},
    {"/heartbeat", test_heartbeat, setup, tear_down, 0, NULL},
    {"/heartbeat/error", test_heartbeat_error, setup, tear_down, 0, NULL},
    {"/open/error", test_open_error, setup, tear_down, 0, NULL},
    {"/open", test_open, setup, tear_down, 0, NULL},
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
    {"/exec-sql/bad-params", test_exec_sql_bad_params, setup, tear_down, 0, NULL},
    {"/exec-sql/error", test_exec_sql_error, setup, tear_down, 0, NULL},
    {"/query-sql", test_query_sql, setup, tear_down, 0, NULL},
    {"/query-sql/bad-sql", test_query_sql_bad_sql, setup, tear_down, 0, NULL},
    {"/query-sql/bad-params", test_query_sql_bad_params, setup, tear_down, 0, NULL},
    {"/invalid-request-type", test_invalid_request_type, setup, tear_down, 0, NULL},
    {"/max-requests", test_max_requests, setup, tear_down, 0, NULL},
    {"/checkpoint", test_checkpoint, setup, tear_down, 0, NULL},
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
