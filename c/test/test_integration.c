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

static MunitResult test_dqlite(const MunitParameter params[], void *data) {
	struct fixture *f = data;
	int             err;
	char *          leader;
	uint64_t        heartbeat;
	uint32_t        db_id;
	uint32_t        stmt_id;

	(void)params;

	err = test_client_handshake(f->client);
	munit_assert_int(err, ==, 0);

	err = test_client_leader(f->client, &leader);
	munit_assert_int(err, ==, 0);

	err = test_client_client(f->client, &heartbeat);
	munit_assert_int(err, ==, 0);

	err = test_client_open(f->client, "test.db", &db_id);
	munit_assert_int(err, ==, 0);

	munit_assert_int(db_id, ==, 0);

	err = test_client_prepare(
	    f->client, db_id, "CREATE TABLE test (n INT)", &stmt_id);
	munit_assert_int(err, ==, 0);

	munit_assert_int(stmt_id, ==, 0);

	err = test_client_exec(f->client, db_id, stmt_id);
	munit_assert_int(err, ==, 0);

	err = test_client_finalize(f->client, db_id, stmt_id);
	munit_assert_int(err, ==, 0);

	err = test_client_prepare(
	    f->client, db_id, "INSERT INTO test VALUES(123)", &stmt_id);
	munit_assert_int(err, ==, 0);

	munit_assert_int(stmt_id, ==, 0);

	err = test_client_exec(f->client, db_id, stmt_id);
	munit_assert_int(err, ==, 0);

	err = test_client_finalize(f->client, db_id, stmt_id);
	munit_assert_int(err, ==, 0);

	err = test_client_prepare(f->client, db_id, "SELECT n FROM test", &stmt_id);
	munit_assert_int(err, ==, 0);

	munit_assert_int(stmt_id, ==, 0);

	err = test_client_query(f->client, db_id, stmt_id);
	munit_assert_int(err, ==, 0);

	err = test_client_finalize(f->client, db_id, stmt_id);
	munit_assert_int(err, ==, 0);

	return MUNIT_OK;
}

static MunitTest dqlite__integration_tests[] = {
    {"dqlite", test_dqlite, setup, tear_down, 0, NULL},
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
