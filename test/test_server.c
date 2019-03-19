#include <pthread.h>

#include <sqlite3.h>

#include "../include/dqlite.h"

#include "client.h"
#include "cluster.h"
#include "log.h"
#include "server.h"
#include "./lib/runner.h"
#include "./lib/sqlite.h"
#include "./lib/heap.h"

#ifndef DQLITE_EXPERIMENTAL

TEST_MODULE(server);

/******************************************************************************
 *
 * Setup and tear down
 *
 ******************************************************************************/

dqlite_cluster *cluster;

static void *setup(const MunitParameter params[], void *user_data) {
	dqlite_server *server;
	int            err;

	(void)params;
	(void)user_data;

	test_heap_setup(params, user_data);
	test_sqlite_setup(params);

	cluster = test_cluster();

	err = dqlite_server_create(cluster, &server);
	munit_assert_int(err, ==, 0);

	return server;
}

static void tear_down(void *data) {
	dqlite_server *server = data;

	dqlite_server_destroy(server);
	test_cluster_close(cluster);

	test_sqlite_tear_down();
	test_heap_tear_down(data);
}

/******************************************************************************
 *
 * Tests dqlite_server_config
 *
 ******************************************************************************/

TEST_SUITE(config);
TEST_SETUP(config, setup);
TEST_TEAR_DOWN(config, tear_down);

TEST_CASE(config, logger, NULL)
{
	dqlite_server *server = data;
	dqlite_logger *logger = test_logger();
	int            err;

	(void)params;

	err = dqlite_server_config(server, DQLITE_CONFIG_LOGGER, logger);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_equal(dqlite_server_logger(server), logger);

	free(logger);

	return MUNIT_OK;
}

TEST_CASE(config, heartbeat_timeout, NULL)
{
	dqlite_server *server  = data;
	int            timeout = 1000;
	int            err;

	(void)params;

	err =
	    dqlite_server_config(server, DQLITE_CONFIG_HEARTBEAT_TIMEOUT, &timeout);
	munit_assert_int(err, ==, 0);

	return MUNIT_OK;
}

TEST_CASE(config, page_size, NULL)
{
	dqlite_server *server = data;
	int            size   = 512;
	int            err;

	(void)params;

	err = dqlite_server_config(server, DQLITE_CONFIG_PAGE_SIZE, &size);
	munit_assert_int(err, ==, 0);

	return MUNIT_OK;
}

TEST_CASE(config, checkpoint_threshold, NULL)
{
	dqlite_server *server    = data;
	int            threshold = 1;
	int            err;

	(void)params;

	err = dqlite_server_config(
	    server, DQLITE_CONFIG_CHECKPOINT_THRESHOLD, &threshold);
	munit_assert_int(err, ==, 0);

	return MUNIT_OK;
}

#endif /* DQLITE_EXPERIMENTAL */
