#include "../lib/client_protocol.h"
#include "../lib/fs.h"
#include "../lib/heap.h"
#include "../lib/runner.h"
#include "../lib/server.h"
#include "../lib/sqlite.h"

#include "include/dqlite.h"
#include "src/server.h"
#include "src/transport.h"

/******************************************************************************
 *
 * C client
 *
 ******************************************************************************/

SUITE(client);

#define N_SERVERS 3

struct fixture {
	char *dirs[N_SERVERS];
	dqlite_server *servers[N_SERVERS];
};

static void start_each_server(struct fixture *f)
{
	const char *addrs[] = { "127.0.0.1:8880", "127.0.0.1:8881" };
	int rv;

	rv = dqlite_server_set_address(f->servers[0], "127.0.0.1:8880");
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_set_auto_bootstrap(f->servers[0], true);
	munit_assert_int(rv, ==, 0);
	f->servers[0]->refresh_period = 100;
	rv = dqlite_server_start(f->servers[0]);
	munit_assert_int(rv, ==, 0);

	rv = dqlite_server_set_address(f->servers[1], "127.0.0.1:8881");
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_set_auto_join(f->servers[1], addrs, 1);
	munit_assert_int(rv, ==, 0);
	f->servers[1]->refresh_period = 100;
	rv = dqlite_server_start(f->servers[1]);
	munit_assert_int(rv, ==, 0);

	rv = dqlite_server_set_address(f->servers[2], "127.0.0.1:8882");
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_set_auto_join(f->servers[2], addrs, 2);
	munit_assert_int(rv, ==, 0);
	f->servers[2]->refresh_period = 100;
	rv = dqlite_server_start(f->servers[2]);
	munit_assert_int(rv, ==, 0);
}

static void stop_each_server(struct fixture *f)
{
	int rv;

	rv = dqlite_server_stop(f->servers[2]);
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_stop(f->servers[1]);
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_stop(f->servers[0]);
	munit_assert_int(rv, ==, 0);
}

static void *setup(const MunitParameter params[], void *user_data)
{
	(void)params;
	(void)user_data;
	struct fixture *f = munit_malloc(sizeof *f);
	unsigned i;
	int rv;

	for (i = 0; i < N_SERVERS; i += 1) {
		f->dirs[i] = test_dir_setup();
		rv = dqlite_server_create(f->dirs[i], &f->servers[i]);
		munit_assert_int(rv, ==, 0);
	}

	start_each_server(f);
	return f;
}

static void teardown(void *data)
{
	struct fixture *f = data;
	unsigned i;

	stop_each_server(f);
	for (i = 0; i < N_SERVERS; i += 1) {
		dqlite_server_destroy(f->servers[i]);
		test_dir_tear_down(f->dirs[i]);
	}
	free(f);
}

TEST(client, openclose, setup, teardown, 0, NULL)
{
	dqlite *db;
	int rv;
	struct fixture *f = data;

	rv = dqlite_open(f->servers[0], "test", &db, 0);
	munit_assert_int(rv, ==, SQLITE_OK);
	rv = dqlite_close(db);
	munit_assert_int(rv, ==, SQLITE_OK);

	rv = dqlite_open(f->servers[0], "test", &db, 0);
	munit_assert_int(rv, ==, SQLITE_OK);
	rv = dqlite_close(db);
	munit_assert_int(rv, ==, SQLITE_OK);

	return MUNIT_OK;
}

TEST(client, prepare, setup, teardown, 0, NULL)
{
	dqlite *db;
	dqlite_stmt *stmt;
	int rv;
	struct fixture *f = data;

	rv = dqlite_open(f->servers[0], "test", &db, 0);
	munit_assert_int(rv, ==, SQLITE_OK);

	// Regular statement.
	rv = dqlite_prepare(
	    db, "CREATE TABLE pairs (k TEXT, v INTEGER, f FLOAT, b BLOB)", -1,
	    &stmt, NULL);
	munit_assert_int(rv, ==, SQLITE_OK);
	rv = dqlite_finalize(stmt);
	munit_assert_int(rv, ==, SQLITE_OK);

	// TODO Edge case: sql_len = 0.
	rv = dqlite_prepare(
	    db, "CREATE TABLE pairs (k TEXT, v INTEGER, f FLOAT, b BLOB)", -1,
	    &stmt, NULL);
	munit_assert_int(rv, ==, SQLITE_OK);
	rv = dqlite_finalize(stmt);
	munit_assert_int(rv, ==, SQLITE_OK);

	rv = dqlite_close(db);
	munit_assert_int(rv, ==, SQLITE_OK);

	return MUNIT_OK;
}

TEST(client, prepare_reconnect, setup, teardown, 0, NULL)
{
	dqlite *db;
	dqlite_stmt *stmt;
	int rv;
	struct fixture *f = data;

	rv = dqlite_open(f->servers[0], "test", &db, 0);
	munit_assert_int(rv, ==, SQLITE_OK);

	// Regular statement.
	rv = dqlite_prepare(
	    db, "CREATE TABLE pairs (k TEXT, v INTEGER, f FLOAT, b BLOB)", -1,
	    &stmt, NULL);
	munit_assert_int(rv, ==, SQLITE_OK);
	rv = dqlite_finalize(stmt);
	munit_assert_int(rv, ==, SQLITE_OK);

	rv = dqlite_close(db);
	munit_assert_int(rv, ==, SQLITE_OK);

	return MUNIT_OK;
}
