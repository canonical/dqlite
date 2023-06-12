#include "../../include/dqlite.h"
#include "../../src/server.h"
#include "../lib/fs.h"
#include "../lib/munit.h"
#include "../lib/runner.h"

#include <stdio.h>
#include <time.h>

SUITE(server);

#define N_SERVERS 3

struct fixture
{
	char *dirs[N_SERVERS];
	dqlite_server *servers[N_SERVERS];
};

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

	return f;
}

static void teardown(void *data)
{
	struct fixture *f = data;
	unsigned i;

	for (i = 0; i < N_SERVERS; i += 1) {
		dqlite_server_destroy(f->servers[i]);
		test_dir_tear_down(f->dirs[i]);
	}
	free(f);
}

#define PREPARE_FILE(i, name, ...)                              \
	do {                                                    \
		char path[100];                                 \
		snprintf(path, 100, "%s/%s", f->dirs[i], name); \
		FILE *fp = fopen(path, "w+");                   \
		fprintf(fp, __VA_ARGS__);                       \
		fclose(fp);                                     \
	} while (0)

#define NODE(x) x

#define NODE0_ID "3297041220608546238"

void start_each_server(struct fixture *f)
{
	const char *addrs[] = {"127.0.0.1:8880", "127.0.0.1:8881"};
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

void stop_each_server(struct fixture *f)
{
	int rv;

	rv = dqlite_server_stop(f->servers[2]);
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_stop(f->servers[1]);
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_stop(f->servers[0]);
	munit_assert_int(rv, ==, 0);
}

TEST(server, restart_follower, setup, teardown, 0, NULL)
{
	struct fixture *f = data;
	struct timespec ts = {0};
	int rv;

	/* Between operations we sleep for 200 milliseconds, twice
	 * the configured refresh period, so that the refresh task
	 * has a chance to be triggered. */
	ts.tv_nsec = 200 * 1000 * 1000;

	start_each_server(f);

	nanosleep(&ts, NULL);

	rv = dqlite_server_stop(f->servers[1]);
	munit_assert_int(rv, ==, 0);

	nanosleep(&ts, NULL);

	rv = dqlite_server_start(f->servers[1]);
	munit_assert_int(rv, ==, 0);

	nanosleep(&ts, NULL);

	stop_each_server(f);

	return MUNIT_OK;
}

TEST(server, restart_leader, setup, teardown, 0, NULL)
{
	struct fixture *f = data;
	struct timespec ts = {0};
	int rv;

	/* Between operations we sleep for 200 milliseconds, twice
	 * the configured refresh period, so that the refresh task
	 * has a chance to be triggered. */
	ts.tv_nsec = 200 * 1000 * 1000;

	start_each_server(f);

	nanosleep(&ts, NULL);

	rv = dqlite_server_stop(f->servers[0]);
	munit_assert_int(rv, ==, 0);

	nanosleep(&ts, NULL);

	rv = dqlite_server_start(f->servers[0]);
	munit_assert_int(rv, ==, 0);

	nanosleep(&ts, NULL);

	stop_each_server(f);

	return MUNIT_OK;
}

TEST(server, bad_info_file, setup, teardown, 0, NULL)
{
	struct fixture *f = data;
	int rv;

	PREPARE_FILE(NODE(0), "server-info", "blah");

	rv = dqlite_server_set_address(f->servers[0], "127.0.0.1:8880");
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_set_auto_bootstrap(f->servers[0], true);
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_start(f->servers[0]);
	munit_assert_int(rv, !=, 0);

	return MUNIT_OK;
}

TEST(server, bad_node_store, setup, teardown, 0, NULL)
{
	struct fixture *f = data;
	int rv;

	PREPARE_FILE(NODE(0), "server-info",
		     "v1\n127.0.0.1:8880\n" NODE0_ID "\n");
	PREPARE_FILE(NODE(0), "node-store", "blah");

	rv = dqlite_server_set_address(f->servers[0], "127.0.0.1:8880");
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_set_auto_bootstrap(f->servers[0], true);
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_start(f->servers[0]);
	munit_assert_int(rv, !=, 0);

	return MUNIT_OK;
}

TEST(server, node_store_but_no_info, setup, teardown, 0, NULL)
{
	struct fixture *f = data;
	int rv;

	PREPARE_FILE(NODE(0), "node-store",
		     "v1\n127.0.0.1:8880\n" NODE0_ID "\nvoter\n");

	rv = dqlite_server_set_address(f->servers[0], "127.0.0.1:8880");
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_set_auto_bootstrap(f->servers[0], true);
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_start(f->servers[0]);
	munit_assert_int(rv, !=, 0);

	return MUNIT_OK;
}

TEST(server, missing_bootstrap, setup, teardown, 0, NULL)
{
	struct fixture *f = data;
	const char *addrs[] = {"127.0.0.1:8880"};
	int rv;

	rv = dqlite_server_set_address(f->servers[1], "127.0.0.1:8881");
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_set_auto_join(f->servers[1], addrs, 1);
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_start(f->servers[1]);
	munit_assert_int(rv, !=, 0);

	return MUNIT_OK;
}

TEST(server, start_twice, setup, teardown, 0, NULL)
{
	struct fixture *f = data;
	int rv;

	rv = dqlite_server_set_address(f->servers[0], "127.0.0.1:8880");
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_set_auto_bootstrap(f->servers[0], true);
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_start(f->servers[0]);
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_start(f->servers[0]);
	munit_assert_int(rv, !=, 0);
	rv = dqlite_server_stop(f->servers[0]);
	munit_assert_int(rv, ==, 0);

	return MUNIT_OK;
}

TEST(server, stop_twice, setup, teardown, 0, NULL)
{
	struct fixture *f = data;
	int rv;

	rv = dqlite_server_set_address(f->servers[0], "127.0.0.1:8880");
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_set_auto_bootstrap(f->servers[0], true);
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_start(f->servers[0]);
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_stop(f->servers[0]);
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_stop(f->servers[0]);
	munit_assert_int(rv, !=, 0);

	return MUNIT_OK;
}
