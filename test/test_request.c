#include "../src/request.h"

#include "./lib/heap.h"
#include "./lib/message.h"
#include "./lib/runner.h"

TEST_MODULE(request);

/******************************************************************************
 *
 * Setup and tear down
 *
 ******************************************************************************/

static void *setup(const MunitParameter params[], void *user_data)
{
	struct request *request;
	test_heap_setup(params, user_data);
	request = munit_malloc(sizeof *request);
	request_init(request);
	return request;
}

static void tear_down(void *data)
{
	struct request *request = data;
	request_close(request);
	free(request);
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

TEST_CASE(decode, leader, NULL)
{
	struct request *request = data;
	int err;

	(void)params;

	test_message_send_leader(0, &request->message);

	err = request_decode(request);
	munit_assert_int(err, ==, 0);

	return MUNIT_OK;
}

TEST_CASE(decode, client, NULL)
{
	struct request *request = data;
	int err;

	(void)params;

	test_message_send_client(123, &request->message);

	err = request_decode(request);
	munit_assert_int(err, ==, 0);

	munit_assert_int(request->client.id, ==, 123);

	return MUNIT_OK;
}

TEST_CASE(decode, heartbeat, NULL)
{
	struct request *request = data;
	int err;

	(void)params;

	test_message_send_heartbeat(666, &request->message);

	err = request_decode(request);
	munit_assert_int(err, ==, 0);

	munit_assert_int(request->heartbeat.timestamp, ==, 666);

	return MUNIT_OK;
}

TEST_CASE(decode, open, NULL)
{
	struct request *request = data;
	int err;

	(void)params;

	test_message_send_open("test.db", 123, "volatile", &request->message);

	err = request_decode(request);
	munit_assert_int(err, ==, 0);

	munit_assert_string_equal(request->open.name, "test.db");
	munit_assert_int(request->open.flags, ==, 123);
	munit_assert_string_equal(request->open.vfs, "volatile");

	return MUNIT_OK;
}
