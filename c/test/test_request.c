#include <stdint.h>

#include "../src/message.h"
#include "../src/request.h"

#include "leak.h"
#include "message.h"
#include "munit.h"

/******************************************************************************
 *
 * Setup and tear down
 *
 ******************************************************************************/

static void *setup(const MunitParameter params[], void *user_data) {
	struct dqlite__request *request;

	(void)params;
	(void)user_data;

	request = munit_malloc(sizeof *request);

	dqlite__request_init(request);

	return request;
}

static void tear_down(void *data) {
	struct dqlite__request *request = data;

	dqlite__request_close(request);

	test_assert_no_leaks();
}

/******************************************************************************
 *
 * Tests
 *
 ******************************************************************************/

static MunitResult test_leader(const MunitParameter params[], void *data) {
	struct dqlite__request *request = data;
	int                     err;

	(void)params;

	test_message_send_leader(0, &request->message);

	err = dqlite__request_decode(request);
	munit_assert_int(err, ==, 0);

	return MUNIT_OK;
}

static MunitResult test_client(const MunitParameter params[], void *data) {
	struct dqlite__request *request = data;
	int                     err;

	(void)params;

	test_message_send_client(123, &request->message);

	err = dqlite__request_decode(request);
	munit_assert_int(err, ==, 0);

	munit_assert_int(request->client.id, ==, 123);

	return MUNIT_OK;
}

static MunitResult test_heartbeat(const MunitParameter params[], void *data) {
	struct dqlite__request *request = data;
	int                     err;

	(void)params;

	test_message_send_heartbeat(666, &request->message);

	err = dqlite__request_decode(request);
	munit_assert_int(err, ==, 0);

	munit_assert_int(request->heartbeat.timestamp, ==, 666);

	return MUNIT_OK;
}

static MunitResult test_open(const MunitParameter params[], void *data) {
	struct dqlite__request *request = data;
	int                     err;

	(void)params;

	test_message_send_open("test.db", 123, "volatile", &request->message);

	err = dqlite__request_decode(request);
	munit_assert_int(err, ==, 0);

	munit_assert_string_equal(request->open.name, "test.db");
	munit_assert_int(request->open.flags, ==, 123);
	munit_assert_string_equal(request->open.vfs, "volatile");

	return MUNIT_OK;
}

static MunitTest dqlite__request_decode_tests[] = {
    {"/leader", test_leader, setup, tear_down, 0, NULL},
    {"/client", test_client, setup, tear_down, 0, NULL},
    {"/heartbeat", test_heartbeat, setup, tear_down, 0, NULL},
    {"/open", test_open, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * Suite
 *
 ******************************************************************************/

MunitSuite dqlite__request_suites[] = {
    {"_decode", dqlite__request_decode_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE},
    {NULL, NULL, NULL, 0, 0},
};
