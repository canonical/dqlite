#include <assert.h>

#include "../include/dqlite.h"

#include "client.h"
#include "leak.h"
#include "munit.h"
#include "server.h"

/******************************************************************************
 *
 * Helpers
 *
 ******************************************************************************/

struct fixture {
	test_server *       server;
	struct test_client *client;
};

/******************************************************************************
 *
 * Setup and tear down
 *
 ******************************************************************************/

static void *setup(const MunitParameter params[], void *user_data) {
	struct fixture *f;
	const char *    errmsg;
	int             err;

	(void)params;
	(void)user_data;

	err = dqlite_init(&errmsg);
	munit_assert_int(err, ==, 0);

	f = munit_malloc(sizeof *f);

	f->server = test_server_start();
	munit_assert_ptr_not_equal(f->server, NULL);

	err = test_server_connect(f->server, &f->client);
	munit_assert_int(err, ==, 0);

	return f;
}

static void tear_down(void *data) {
	struct fixture *f = data;
	int             err;

	test_client_close(f->client);

	err = test_server_stop(f->server);
	munit_assert_int(err, ==, 0);

	test_assert_no_leaks();
}

/******************************************************************************
 *
 * Tests
 *
 ******************************************************************************/

static MunitResult test_exec_and_query(const MunitParameter params[], void *data) {
	struct fixture *          f = data;
	char *                    leader;
	uint64_t                  heartbeat;
	uint32_t                  db_id;
	uint32_t                  stmt_id;
	struct test_client_result result;
	struct test_client_rows   rows;

	(void)params;

	/* Initialize the connection and open a database. */
	test_client_handshake(f->client);
	test_client_leader(f->client, &leader);
	test_client_client(f->client, &heartbeat);
	test_client_open(f->client, "test.db", &db_id);
	munit_assert_int(db_id, ==, 0);

	/* Create a test table. */
	test_client_prepare(f->client, db_id, "CREATE TABLE test (n INT)", &stmt_id);
	test_client_exec(f->client, db_id, stmt_id, &result);
	test_client_finalize(f->client, db_id, stmt_id);

	/* Insert a row in the test table. */
	test_client_prepare(
	    f->client, db_id, "INSERT INTO test VALUES(123)", &stmt_id);

	munit_assert_int(stmt_id, ==, 0);

	test_client_exec(f->client, db_id, stmt_id, &result);

	munit_assert_int(result.last_insert_id, ==, 1);
	munit_assert_int(result.rows_affected, ==, 1);

	test_client_finalize(f->client, db_id, stmt_id);

	/* Select rows from the test table. */
	test_client_prepare(f->client, db_id, "SELECT n FROM test", &stmt_id);

	munit_assert_int(stmt_id, ==, 0);

	test_client_query(f->client, db_id, stmt_id, &rows);

	munit_assert_int(rows.column_count, ==, 1);
	munit_assert_string_equal(rows.column_names[0], "n");

	munit_assert_ptr_not_null(rows.next);
	munit_assert_int(rows.next->types[0], ==, SQLITE_INTEGER);
	munit_assert_int(*(int64_t *)rows.next->values[0], ==, 123);

	test_client_finalize(f->client, db_id, stmt_id);

	return MUNIT_OK;
}

static MunitTest dqlite__integration_tests[] = {
    {"/exec-and-query", test_exec_and_query, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * Suite
 *
 ******************************************************************************/

MunitSuite dqlite__integration_suites[] = {
    {"", dqlite__integration_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE},
    {NULL, NULL, NULL, 0, 0},
};
