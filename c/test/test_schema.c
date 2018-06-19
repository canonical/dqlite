#include <CUnit/CUnit.h>

#include "../src/error.h"
#include "../src/message.h"
#include "../src/schema.h"

#include "message.h"

#define TEST_SCHEMA_FOO(X, ...)			\
	X(uint64, id, __VA_ARGS__)		\
	X(text, name, __VA_ARGS__)

DQLITE__SCHEMA_DEFINE(test_foo, TEST_SCHEMA_FOO);
DQLITE__SCHEMA_IMPLEMENT(test_foo, TEST_SCHEMA_FOO);

#define TEST_SCHEMA_BAR(X, ...)			\
	X(uint64, i, __VA_ARGS__)		\
	X(uint64, j, __VA_ARGS__)

DQLITE__SCHEMA_DEFINE(test_bar, TEST_SCHEMA_BAR);
DQLITE__SCHEMA_IMPLEMENT(test_bar, TEST_SCHEMA_BAR);

/* Type codes */
#define TEST_FOO 0
#define TEST_BAR 1

#define TEST_SCHEMA_TYPES(X, ...)		\
	X(TEST_FOO, test_foo, foo, __VA_ARGS__)	\
	X(TEST_BAR, test_bar, bar, __VA_ARGS__)

DQLITE__SCHEMA_HANDLER_DEFINE(test_handler, TEST_SCHEMA_TYPES);
DQLITE__SCHEMA_HANDLER_IMPLEMENT(test_handler, TEST_SCHEMA_TYPES);

static struct test_handler handler;

void test_dqlite__schema_setup()
{
	test_handler_init(&handler);
}

void test_dqlite__schema_teardown()
{
	test_handler_close(&handler);
}

/*
 * dqlite__schema_handler_decode_suite
 */

void test_dqlite__schema_handler_encode_two_uint64()
{
	int err;

	handler.type = TEST_BAR;
	handler.bar.i = 99;
	handler.bar.j = 17;

	err = test_handler_encode(&handler);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(handler.message.type, TEST_BAR);
	CU_ASSERT_EQUAL(handler.message.offset1, 16);

	CU_ASSERT_EQUAL(*(uint64_t*)handler.message.body1, 99);
	CU_ASSERT_EQUAL(*(uint64_t*)(handler.message.body1 + 8), 17);
}

void test_dqlite__schema_handler_encode_uint64_and_text()
{
	int err;

	handler.type = TEST_FOO;
	handler.foo.id = 123;
	handler.foo.name = "hello world!";

	err = test_handler_encode(&handler);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(handler.message.type, TEST_FOO);
	CU_ASSERT_EQUAL(handler.message.offset1, 24);

	CU_ASSERT_EQUAL(*(uint64_t*)handler.message.body1, 123);
	CU_ASSERT_STRING_EQUAL((const char*)(handler.message.body1 + 8), "hello world!");
}

void test_dqlite__schema_handler_encode_unknown_type()
{
	int err;

	handler.type = 255;

	err = test_handler_encode(&handler);
	CU_ASSERT_EQUAL(err, DQLITE_PROTO);

	CU_ASSERT_STRING_EQUAL(handler.error, "unknown message type 255");
}

/*
 * dqlite__schema_handler_encode_suite
 */

void test_dqlite__schema_handler_decode_invalid_text()
{
	int err;
	handler.message.type = TEST_FOO;
	handler.message.words = 2;

	*(uint64_t*)handler.message.body1 = 123;
	*(uint64_t*)(handler.message.body1 + 8) = 0xffffffffffffffff;

	err = test_handler_decode(&handler);
	CU_ASSERT_EQUAL(err, DQLITE_PARSE);

	CU_ASSERT_STRING_EQUAL(
		handler.error,
		"failed to decode 'foo': failed to get 'name' field: no string found");
}

void test_dqlite__schema_handler_decode_unknown_type()
{
	int err;
	handler.message.type = 255;
	handler.message.words = 1;

	err = test_handler_decode(&handler);
	CU_ASSERT_EQUAL(err, DQLITE_PROTO);

	CU_ASSERT_STRING_EQUAL(handler.error, "unknown message type 255");
}

void test_dqlite__schema_handler_decode_two_uint64()
{
	int err;
	static struct test_handler handler2;

	test_handler_init(&handler2);

	handler.type = TEST_BAR;
	handler.bar.i = 99;
	handler.bar.j = 17;

	err = test_handler_encode(&handler);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(handler.message.type, TEST_BAR);

	test_message_send(&handler.message, &handler2.message);

	CU_ASSERT_EQUAL(handler2.message.type, TEST_BAR);

	err = test_handler_decode(&handler2);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(handler2.bar.i, 99);
	CU_ASSERT_EQUAL(handler2.bar.j, 17);

	dqlite__message_recv_reset(&handler2.message);
	dqlite__message_send_reset(&handler.message);

	test_handler_close(&handler2);
}
