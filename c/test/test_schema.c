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

DQLITE__SCHEMA_ENCODER_DEFINE(test_encoder, TEST_SCHEMA_TYPES);
DQLITE__SCHEMA_ENCODER_IMPLEMENT(test_encoder, TEST_SCHEMA_TYPES);

DQLITE__SCHEMA_DECODER_DEFINE(test_decoder, TEST_SCHEMA_TYPES);
DQLITE__SCHEMA_DECODER_IMPLEMENT(test_decoder, TEST_SCHEMA_TYPES);

static struct dqlite__message outgoing;
static struct dqlite__message incoming;
static struct test_encoder encoder;
static struct test_decoder decoder;

void test_dqlite__schema_setup()
{
	dqlite__message_init(&outgoing);
	dqlite__message_init(&incoming);
	test_encoder_init(&encoder);
	test_decoder_init(&decoder);
}

void test_dqlite__schema_teardown()
{
	test_decoder_close(&decoder);
	test_encoder_close(&encoder);
	dqlite__message_close(&incoming);
	dqlite__message_close(&outgoing);
}

/*
 * dqlite__schema_decoder_decode_suite
 */

void test_dqlite__schema_encoder_encode_two_uint64()
{
	int err;

	encoder.type = TEST_BAR;
	encoder.bar.i = 99;
	encoder.bar.j = 17;

	err = test_encoder_encode(&encoder, &outgoing);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(outgoing.type, TEST_BAR);
	CU_ASSERT_EQUAL(outgoing.offset1, 16);

	CU_ASSERT_EQUAL(*(uint64_t*)outgoing.body1, 99);
	CU_ASSERT_EQUAL(*(uint64_t*)(outgoing.body1 + 8), 17);
}

void test_dqlite__schema_encoder_encode_uint64_and_text()
{
	int err;

	encoder.type = TEST_FOO;
	encoder.foo.id = 123;
	encoder.foo.name = "hello world!";

	err = test_encoder_encode(&encoder, &outgoing);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(outgoing.type, TEST_FOO);
	CU_ASSERT_EQUAL(outgoing.offset1, 24);

	CU_ASSERT_EQUAL(*(uint64_t*)outgoing.body1, 123);
	CU_ASSERT_STRING_EQUAL((const char*)(outgoing.body1 + 8), "hello world!");
}

void test_dqlite__schema_encoder_encode_unknown_type()
{
	int err;

	encoder.type = 255;

	err = test_encoder_encode(&encoder, &outgoing);
	CU_ASSERT_EQUAL(err, DQLITE_PROTO);

	CU_ASSERT_STRING_EQUAL(encoder.error, "unknown message type 255");
}

/*
 * dqlite__schema_encoder_encode_suite
 */

void test_dqlite__schema_decoder_decode_invalid_text()
{
	int err;
	incoming.type = TEST_FOO;
	incoming.words = 2;

	*(uint64_t*)incoming.body1 = 123;
	*(uint64_t*)(incoming.body1 + 8) = 0xffffffffffffffff;

	err = test_decoder_decode(&decoder, &incoming);
	CU_ASSERT_EQUAL(err, DQLITE_PARSE);

	CU_ASSERT_STRING_EQUAL(
		decoder.error,
		"failed to decode 'foo': failed to get 'name' field: no string found");
}

void test_dqlite__schema_decoder_decode_unknown_type()
{
	int err;
	incoming.type = 255;
	incoming.words = 1;

	err = test_decoder_decode(&decoder, &incoming);
	CU_ASSERT_EQUAL(err, DQLITE_PROTO);

	CU_ASSERT_STRING_EQUAL(decoder.error, "unknown message type 255");
}

void test_dqlite__schema_decoder_decode_two_uint64()
{
	int err;

	encoder.type = TEST_BAR;
	encoder.bar.i = 99;
	encoder.bar.j = 17;

	err = test_encoder_encode(&encoder, &outgoing);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(outgoing.type, TEST_BAR);

	test_message_send(&outgoing, &incoming);

	CU_ASSERT_EQUAL(incoming.type, TEST_BAR);

	err = test_decoder_decode(&decoder, &incoming);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(decoder.bar.i, 99);
	CU_ASSERT_EQUAL(decoder.bar.j, 17);

	dqlite__message_recv_reset(&incoming);
	dqlite__message_send_reset(&outgoing);
}
