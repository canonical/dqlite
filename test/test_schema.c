#include "../src/error.h"
#include "../src/message.h"
#include "../src/schema.h"
#include "../src/lib/byte.h"

#include "./lib/message.h"
#include "./lib/runner.h"

TEST_MODULE(schema);

/******************************************************************************
 *
 * Helpers
 *
 ******************************************************************************/

#define TEST_SCHEMA_FOO(X, ...)                                                \
	X(uint64, id, __VA_ARGS__)                                             \
	X(text, name, __VA_ARGS__)

SCHEMA__DEFINE(test_foo, TEST_SCHEMA_FOO);
SCHEMA__IMPLEMENT(test_foo, TEST_SCHEMA_FOO);

#define TEST_SCHEMA_BAR(X, ...)                                                \
	X(uint64, i, __VA_ARGS__)                                              \
	X(uint64, j, __VA_ARGS__)

SCHEMA__DEFINE(test_bar, TEST_SCHEMA_BAR);
SCHEMA__IMPLEMENT(test_bar, TEST_SCHEMA_BAR);

/* Type codes */
#define TEST_FOO 0
#define TEST_BAR 1

#define TEST_SCHEMA_TYPES(X, ...)                                              \
	X(TEST_FOO, test_foo, foo, __VA_ARGS__)                                \
	X(TEST_BAR, test_bar, bar, __VA_ARGS__)

SCHEMA__HANDLER_DEFINE(test_handler, TEST_SCHEMA_TYPES);
SCHEMA__HANDLER_IMPLEMENT(test_handler, TEST_SCHEMA_TYPES);

/******************************************************************************
 *
 * Setup and tear down
 *
 ******************************************************************************/

static void *setup(const MunitParameter params[], void *user_data) {
	struct test_handler *handler;

	(void)params;
	(void)user_data;

	handler = munit_malloc(sizeof *handler);

	test_handler_init(handler);

	return handler;
}

static void tear_down(void *data) {
	struct test_handler *handler = data;

	test_handler_close(handler);
	free(handler);
}

/******************************************************************************
 *
 * Tests for the _encode method.
 *
 ******************************************************************************/

TEST_SUITE(encode);
TEST_SETUP(encode, setup);
TEST_TEAR_DOWN(encode, tear_down);

TEST_CASE(encode, two_uint64, NULL)
{
	struct test_handler *handler = data;
	int                  err;

	(void)params;

	handler->type  = TEST_BAR;
	handler->bar.i = 99;
	handler->bar.j = 17;

	err = test_handler_encode(handler);
	munit_assert_int(err, ==, 0);

	munit_assert_int(handler->message.type, ==, TEST_BAR);
	munit_assert_int(handler->message.offset1, ==, 16);

	munit_assert_int(byte__flip64(*(uint64_t *)handler->message.body1), ==, 99);
	munit_assert_int(byte__flip64(*(uint64_t *)(handler->message.body1 + 8)), ==, 17);

	return MUNIT_OK;
}

TEST_CASE(encode, uint64_and_text, NULL)
{
	struct test_handler *handler = data;
	int                  err;

	(void)params;

	handler->type     = TEST_FOO;
	handler->foo.id   = 123;
	handler->foo.name = "hello world!";

	err = test_handler_encode(handler);
	munit_assert_int(err, ==, 0);

	munit_assert_int(handler->message.type, ==, TEST_FOO);
	munit_assert_int(handler->message.offset1, ==, 24);

	munit_assert_int(byte__flip64(*(uint64_t *)handler->message.body1), ==, 123);
	munit_assert_string_equal((const char *)(handler->message.body1 + 8),
	                          "hello world!");
	return MUNIT_OK;
}

TEST_CASE(encode, unknown_type, NULL)
{
	struct test_handler *handler = data;
	int                  err;

	(void)params;

	handler->type = 255;

	err = test_handler_encode(handler);
	munit_assert_int(err, ==, DQLITE_PROTO);

	munit_assert_string_equal(handler->error, "unknown message type 255");

	return MUNIT_OK;
}

/******************************************************************************
 *
 * Tests for the _decode method.
 *
 ******************************************************************************/

TEST_SUITE(decode);
TEST_SETUP(decode, setup);
TEST_TEAR_DOWN(decode, tear_down);

TEST_CASE(decode, invalid_text, NULL)
{
	struct test_handler *handler = data;
	int                  err;

	(void)params;

	handler->message.type  = TEST_FOO;
	handler->message.words = 2;

	*(uint64_t *)handler->message.body1       = 123;
	*(uint64_t *)(handler->message.body1 + 8) = 0xffffffffffffffff;

	err = test_handler_decode(handler);
	munit_assert_int(err, ==, DQLITE_PARSE);

	munit_assert_string_equal(handler->error,
	                          "failed to decode 'foo': failed to get "
	                          "'name' field: no string found");

	return MUNIT_OK;
}

TEST_CASE(decode, unknown_type, NULL)
{
	struct test_handler *handler = data;
	int                  err;

	(void)params;

	handler->message.type  = 255;
	handler->message.words = 1;

	err = test_handler_decode(handler);
	munit_assert_int(err, ==, DQLITE_PROTO);

	munit_assert_string_equal(handler->error, "unknown message type 255");

	return MUNIT_OK;
}

TEST_CASE(decode, two_uint64, NULL)
{
	struct test_handler *      handler = data;
	static struct test_handler handler2;
	int                        err;

	(void)params;

	test_handler_init(&handler2);

	handler->type  = TEST_BAR;
	handler->bar.i = 99;
	handler->bar.j = 17;

	err = test_handler_encode(handler);
	munit_assert_int(err, ==, 0);

	munit_assert_int(handler->message.type, ==, TEST_BAR);

	test_message_send(&handler->message, &handler2.message);

	munit_assert_int(handler2.message.type, ==, TEST_BAR);

	err = test_handler_decode(&handler2);
	munit_assert_int(err, ==, 0);

	munit_assert_int(handler2.bar.i, ==, 99);
	munit_assert_int(handler2.bar.j, ==, 17);

	message__recv_reset(&handler2.message);
	message__send_reset(&handler->message);

	test_handler_close(&handler2);

	return MUNIT_OK;
}
