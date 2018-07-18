#include <assert.h>
#include <pthread.h>

#include <sqlite3.h>

#include "../include/dqlite.h"

#include "client.h"
#include "cluster.h"
#include "leak.h"
#include "log.h"
#include "munit.h"
#include "server.h"

/******************************************************************************
 *
 * Setup and tear down
 *
 ******************************************************************************/

static void *setup(const MunitParameter params[], void *user_data) {
	dqlite_server *server;
	int            err;

	(void)params;
	(void)user_data;

	server = dqlite_server_alloc();
	munit_assert_ptr_not_equal(server, NULL);

	err = dqlite_server_init(server, test_cluster());

	munit_assert_int(err, ==, 0);

	return server;
}

static void tear_down(void *data) {
	dqlite_server *server = data;

	dqlite_server_close(server);
	dqlite_server_free(server);

	test_assert_no_leaks();
}

/******************************************************************************
 *
 * Tests dqlite_server_config
 *
 ******************************************************************************/

static MunitResult test_config_logger(const MunitParameter params[], void *data) {
	dqlite_server *server = data;
	dqlite_logger *logger = test_logger();
	int            err;

	(void)params;

	err = dqlite_server_config(server, DQLITE_CONFIG_LOGGER, logger);
	munit_assert_int(err, ==, 0);

	return MUNIT_OK;
}

static MunitTest dqlite_server_config_tests[] = {
    {"/logger", test_config_logger, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * Suite
 *
 ******************************************************************************/

MunitSuite dqlite__server_suites[] = {
    {"_config", dqlite_server_config_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE},
    {NULL, NULL, NULL, 0, 0},
};
