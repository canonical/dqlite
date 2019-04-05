#include "../lib/fs.h"
#include "../lib/heap.h"
#include "../lib/runner.h"
#include "../lib/sqlite.h"
#include "../lib/thread.h"

#include "../../src/server.h"

TEST_MODULE(server);

/******************************************************************************
 *
 * Fixture
 *
 ******************************************************************************/

#define FIXTURE \
	FIXTURE_THREAD;\
	char *dir;\
	struct dqlite dqlite

#define SETUP \
	int rv;\
	SETUP_HEAP;\
	SETUP_SQLITE;\
	f->dir = test_dir_setup();\
	rv = dqlite__init(&f->dqlite, 1, "1", f->dir);\
	munit_assert_int(rv, ==, 0)

#define TEAR_DOWN \
	dqlite__close(&f->dqlite);\
	test_dir_tear_down(f->dir);\
	TEAR_DOWN_SQLITE;\
	TEAR_DOWN_HEAP

/******************************************************************************
 *
 * Helper macros.
 *
 ******************************************************************************/

static void *run(void *arg)
{
	struct dqlite *d = arg;
	int rc;
	rc = dqlite_run(d);
	if (rc) {
		return (void *)1;
	}
	return NULL;
}

/* Run the dqlite server in a thread */
#define START THREAD_START(run, &f->dqlite)

#define READY munit_assert_true(dqlite_ready(&f->dqlite))

#define STOP dqlite_stop(&f->dqlite); THREAD_JOIN

/******************************************************************************
 *
 * dqlite_run
 *
 ******************************************************************************/

struct run_fixture {
	FIXTURE;
};

TEST_SUITE(run);
TEST_SETUP(run) {
	struct run_fixture *f = munit_malloc(sizeof *f);
	SETUP;
	return f;
}
TEST_TEAR_DOWN(run) {
	struct run_fixture *f = data;
	TEAR_DOWN;
	free(f);
}

TEST_CASE(run, success, NULL)
{
	struct run_fixture *f = data;
	START;
	READY;
	STOP;
	(void)params;
	return MUNIT_OK;
}

#if 0

/******************************************************************************
 *
 * Setup and tear down
 *
 ******************************************************************************/

dqlite_cluster *cluster;

static void *setup(const MunitParameter params[], void *user_data) {
	dqlite *server;
	int            err;

	(void)params;
	(void)user_data;

	test_heap_setup(params, user_data);
	test_sqlite_setup(params);

	cluster = test_cluster();

	err = dqlite_create(cluster, &server);
	munit_assert_int(err, ==, 0);

	return server;
}

static void tear_down(void *data) {
	dqlite *server = data;

	dqlite_destroy(server);
	test_cluster_close(cluster);

	test_sqlite_tear_down();
	test_heap_tear_down(data);
}

/******************************************************************************
 *
 * Tests dqlite_config
 *
 ******************************************************************************/

TEST_SUITE(config);
TEST_SETUP(config, setup);
TEST_TEAR_DOWN(config, tear_down);

TEST_CASE(config, logger, NULL)
{
	dqlite *server = data;
	dqlite_logger *logger = test_logger();
	int            err;

	(void)params;

	err = dqlite_config(server, DQLITE_CONFIG_LOGGER, logger);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_equal(dqlite_logger(server), logger);

	free(logger);

	return MUNIT_OK;
}

TEST_CASE(config, heartbeat_timeout, NULL)
{
	dqlite *server  = data;
	int            timeout = 1000;
	int            err;

	(void)params;

	err =
	    dqlite_config(server, DQLITE_CONFIG_HEARTBEAT_TIMEOUT, &timeout);
	munit_assert_int(err, ==, 0);

	return MUNIT_OK;
}

TEST_CASE(config, page_size, NULL)
{
	dqlite *server = data;
	int            size   = 512;
	int            err;

	(void)params;

	err = dqlite_config(server, DQLITE_CONFIG_PAGE_SIZE, &size);
	munit_assert_int(err, ==, 0);

	return MUNIT_OK;
}

TEST_CASE(config, checkpoint_threshold, NULL)
{
	dqlite *server    = data;
	int            threshold = 1;
	int            err;

	(void)params;

	err = dqlite_config(
	    server, DQLITE_CONFIG_CHECKPOINT_THRESHOLD, &threshold);
	munit_assert_int(err, ==, 0);

	return MUNIT_OK;
}

#endif
