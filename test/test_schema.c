#include "../src/error.h"
#include "../src/message.h"
#include "../src/schema.h"
#include "../src/binary.h"

#include "leak.h"
#include "message.h"
#include "munit.h"

/******************************************************************************
 *
 * Helpers
 *
 ******************************************************************************/

#define TEST_SCHEMA_FOO(X, ...)                                                \
	X(uint64, id, __VA_ARGS__)                                             \
	X(text, name, __VA_ARGS__)

DQLITE__SCHEMA_DEFINE(test_foo, TEST_SCHEMA_FOO);
DQLITE__SCHEMA_IMPLEMENT(test_foo, TEST_SCHEMA_FOO);

#define TEST_SCHEMA_BAR(X, ...)                                                \
	X(uint64, i, __VA_ARGS__)                                              \
	X(uint64, j, __VA_ARGS__)

DQLITE__SCHEMA_DEFINE(test_bar, TEST_SCHEMA_BAR);
DQLITE__SCHEMA_IMPLEMENT(test_bar, TEST_SCHEMA_BAR);

/* Type codes */
#define TEST_FOO 0
#define TEST_BAR 1

#define TEST_SCHEMA_TYPES(X, ...)                                              \
	X(TEST_FOO, test_foo, foo, __VA_ARGS__)                                \
	X(TEST_BAR, test_bar, bar, __VA_ARGS__)

DQLITE__SCHEMA_HANDLER_DEFINE(test_handler, TEST_SCHEMA_TYPES);
DQLITE__SCHEMA_HANDLER_IMPLEMENT(test_handler, TEST_SCHEMA_TYPES);

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

	test_assert_no_leaks();
}

/******************************************************************************
 *
 * Tests for the _encode method.
 *
 ******************************************************************************/

static MunitResult test_encode_two_uint64(const MunitParameter params[],
                                          void *               data) {
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

	munit_assert_int(dqlite__flip64(*(uint64_t *)handler->message.body1), ==, 99);
	munit_assert_int(dqlite__flip64(*(uint64_t *)(handler->message.body1 + 8)), ==, 17);

	return MUNIT_OK;
}

static MunitResult test_encode_uint64_and_text(const MunitParameter params[],
                                               void *               data) {
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

	munit_assert_int(dqlite__flip64(*(uint64_t *)handler->message.body1), ==, 123);
	munit_assert_string_equal((const char *)(handler->message.body1 + 8),
	                          "hello world!");
	return MUNIT_OK;
}

static MunitResult test_encode_unknown_type(const MunitParameter params[],
                                            void *               data) {
	struct test_handler *handler = data;
	int                  err;

	(void)params;

	handler->type = 255;

	err = test_handler_encode(handler);
	munit_assert_int(err, ==, DQLITE_PROTO);

	munit_assert_string_equal(handler->error, "unknown message type 255");

	return MUNIT_OK;
}

static MunitTest dqlite__schema_encode_tests[] = {
    {"/two-uint64", test_encode_two_uint64, setup, tear_down, 0, NULL},
    {"/uint64-and-text",
     test_encode_uint64_and_text,
     setup,
     tear_down,
     0,
     NULL},
    {"/unknown-type", test_encode_unknown_type, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * Tests for the _decode method.
 *
 ******************************************************************************/
static MunitResult test_decode_invalid_text(const MunitParameter params[],
                                            void *               data) {
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

static MunitResult test_decode_unknown_type(const MunitParameter params[],
                                            void *               data) {
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

static MunitResult test_decode_two_uint64(const MunitParameter params[],
                                          void *               data) {
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

	dqlite__message_recv_reset(&handler2.message);
	dqlite__message_send_reset(&handler->message);

	test_handler_close(&handler2);

	return MUNIT_OK;
}

static MunitTest dqlite__schema_decode_tests[] = {
    {"/invalid-text", test_decode_invalid_text, setup, tear_down, 0, NULL},
    {"/unknown-type", test_decode_unknown_type, setup, tear_down, 0, NULL},
    {"/two-uint64-fields", test_decode_two_uint64, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL},
};

/******************************************************************************
 *
 * Suite
 *
 ******************************************************************************/

MunitSuite dqlite__schema_suites[] = {
    {"_encode", dqlite__schema_encode_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE},
    {"_decode", dqlite__schema_decode_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE},
    {NULL, NULL, NULL, 0, MUNIT_SUITE_OPTION_NONE},
};
