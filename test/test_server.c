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

	err = dqlite_server_create(test_cluster(), &server);
	munit_assert_int(err, ==, 0);

	return server;
}

static void tear_down(void *data) {
	dqlite_server *server = data;

	dqlite_server_destroy(server);

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

	munit_assert_ptr_equal(dqlite_server_logger(server), logger);

	return MUNIT_OK;
}

static MunitResult test_config_heartbeat_timeout(const MunitParameter params[],
                                                 void *               data) {
	dqlite_server *server  = data;
	int            timeout = 1000;
	int            err;

	(void)params;

	err =
	    dqlite_server_config(server, DQLITE_CONFIG_HEARTBEAT_TIMEOUT, &timeout);
	munit_assert_int(err, ==, 0);

	return MUNIT_OK;
}

static MunitResult test_config_page_size(const MunitParameter params[], void *data) {
	dqlite_server *server = data;
	int            size   = 512;
	int            err;

	(void)params;

	err = dqlite_server_config(server, DQLITE_CONFIG_PAGE_SIZE, &size);
	munit_assert_int(err, ==, 0);

	return MUNIT_OK;
}

static MunitResult test_config_checkpoint_threshold(const MunitParameter params[],
                                                    void *               data) {
	dqlite_server *server    = data;
	int            threshold = 1;
	int            err;

	(void)params;

	err = dqlite_server_config(
	    server, DQLITE_CONFIG_CHECKPOINT_THRESHOLD, &threshold);
	munit_assert_int(err, ==, 0);

	return MUNIT_OK;
}

static MunitTest dqlite_server_config_tests[] = {
    {"/logger", test_config_logger, setup, tear_down, 0, NULL},
    {"/heartbeat-timeout", test_config_heartbeat_timeout, setup, tear_down, 0, NULL},
    {"/page-size", test_config_page_size, setup, tear_down, 0, NULL},
    {"/checkpoint-threshold",
     test_config_checkpoint_threshold,
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

MunitSuite dqlite__server_suites[] = {
    {"_config", dqlite_server_config_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE},
    {NULL, NULL, NULL, 0, 0},
};
