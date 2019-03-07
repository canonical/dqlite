#include <stdint.h>

#include "../src/response.h"

#include "./lib/heap.h"
#include "./lib/message.h"
#include "./lib/runner.h"

TEST_MODULE(response);

/******************************************************************************
 *
 * Setup and tear down
 *
 ******************************************************************************/

static void *setup(const MunitParameter params[], void *user_data)
{
	struct response *response;
	test_heap_setup(params, user_data);
	response = munit_malloc(sizeof *response);
	response_init(response);
	return response;
}

static void tear_down(void *data)
{
	struct response *response = data;
	response_close(response);
	free(response);
	test_heap_tear_down(data);
}

/******************************************************************************
 *
 * Tests
 *
 ******************************************************************************/

TEST_SUITE(decode);
TEST_SETUP(decode, setup);
TEST_TEAR_DOWN(decode, tear_down);

TEST_CASE(decode, server, NULL)
{
	struct response *response = data;
	int err;

	(void)params;

	test_message_send_server("1.2.3.4:666", &response->message);

	err = response_decode(response);
	munit_assert_int(err, ==, 0);

	munit_assert_string_equal(response->server.address, "1.2.3.4:666");

	return MUNIT_OK;
}

TEST_CASE(decode, welcome, NULL)
{
	struct response *response = data;
	int err;

	(void)params;

	test_message_send_welcome(15000, &response->message);

	err = response_decode(response);
	munit_assert_int(err, ==, 0);

	munit_assert_int(response->welcome.heartbeat_timeout, ==, 15000);

	return MUNIT_OK;
}

/* static MunitResult test_servers(const MunitParameter params[], void *data) {
 */
/* 	struct response *response = data; */
/* 	int                      err; */

/* 	(void)params; */

/* 	text_t addresses[3] = {"1.2.3.4:666", "5.6.7.8:999", NULL}; */

/* 	test_message_send_servers(addresses, &response->message); */

/* 	err = response_decode(response); */
/* 	munit_assert_int(err, ==, 0); */

/* 	munit_assert_ptr_not_equal(response->servers.addresses, NULL); */

/* 	munit_assert_string_equal(response->servers.addresses[0],
 * "1.2.3.4:666"); */
/* 	munit_assert_string_equal(response->servers.addresses[1],
 * "5.6.7.8:999"); */
/* 	munit_assert_ptr_equal(response->servers.addresses[2], NULL); */

/* 	sqlite3_free(response->servers.addresses); */

/* 	return MUNIT_OK; */
/* } */

TEST_CASE(decode, db, NULL)
{
	struct response *response = data;
	int err;

	(void)params;

	test_message_send_db(123, 0 /* __pad__ */, &response->message);

	err = response_decode(response);
	munit_assert_int(err, ==, 0);

	munit_assert_int(response->db.id, ==, 123);

	return MUNIT_OK;
}
