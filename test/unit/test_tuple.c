#include <sqlite3.h>

#include "../../src/tuple.h"

#include "../lib/runner.h"

TEST_MODULE(tuple);

/******************************************************************************
 *
 * Helper macros.
 *
 ******************************************************************************/

#define DECODER_INIT(N)                                          \
	{                                                        \
		int rc2;                                         \
		rc2 = tuple_decoder__init(&decoder, N, &cursor); \
		munit_assert_int(rc2, ==, 0);                    \
	}

#define DECODER_NEXT                                         \
	{                                                    \
		int rc2;                                     \
		rc2 = tuple_decoder__next(&decoder, &value); \
		munit_assert_int(rc2, ==, 0);                \
	}

/******************************************************************************
 *
 * Assertions.
 *
 ******************************************************************************/

#define ASSERT_VALUE_TYPE(TYPE) munit_assert_int(value.type, ==, TYPE)

/******************************************************************************
 *
 * Decoder.
 *
 ******************************************************************************/

TEST_SUITE(decoder);

TEST_GROUP(decoder, init);

/* If n is 0, then the prefix is used to dermine the number of elements of the
 * tuple. */
TEST_CASE(decoder, init, prefix, NULL)
{
	struct tuple_decoder decoder;
	char buf[] = {2, 0, 0, 0, 0, 0, 0, 0, 0};
	struct cursor cursor = {buf, sizeof buf};
	(void)data;
	(void)params;
	DECODER_INIT(0);
	munit_assert_int(decoder.n, ==, 2);
	return MUNIT_OK;
}

/* If n is not 0, then it is the number of elements. */
TEST_CASE(decoder, init, no_prefix, NULL)
{
	struct tuple_decoder decoder;
	char buf[] = {2, 0, 0, 0, 0, 0, 0, 0, 0};
	struct cursor cursor = {buf, sizeof buf};
	(void)data;
	(void)params;
	DECODER_INIT(3);
	munit_assert_int(decoder.n, ==, 3);
	return MUNIT_OK;
}

TEST_GROUP(decoder, row);

/* Decode the next tuple, with row format and only one value. */
TEST_CASE(decoder, row, one_value, NULL)
{
	struct tuple_decoder decoder;
	uint8_t buf[][8] = {
	    {SQLITE_INTEGER, 0, 0, 0, 0, 0, 0, 0},
	    {7, 0, 0, 0, 0, 0, 0, 0},
	};
	struct cursor cursor = {buf, sizeof buf};
	struct value value;

	(void)data;
	(void)params;

	DECODER_INIT(1);
	DECODER_NEXT;

	ASSERT_VALUE_TYPE(SQLITE_INTEGER);
	munit_assert_int(value.integer, ==, 7);

	return MUNIT_OK;
}

/* Decode the next tuple, with row format and two values. */
TEST_CASE(decoder, row, two_values, NULL)
{
	struct tuple_decoder decoder;
	uint8_t buf[][8] = {
	    {SQLITE_INTEGER | SQLITE_TEXT << 4, 0, 0, 0, 0, 0, 0, 0},
	    {7, 0, 0, 0, 0, 0, 0, 0},
	    {'h', 'e', 'l', 'l', 'o', 0, 0, 0},
	};
	struct cursor cursor = {buf, sizeof buf};
	struct value value;

	(void)data;
	(void)params;

	DECODER_INIT(2);
	DECODER_NEXT;

	ASSERT_VALUE_TYPE(SQLITE_INTEGER);
	munit_assert_int(value.integer, ==, 7);

	DECODER_NEXT;

	ASSERT_VALUE_TYPE(SQLITE_TEXT);
	munit_assert_string_equal(value.text, "hello");

	return MUNIT_OK;
}

TEST_GROUP(decoder, params);

/* Decode the next tuple, with params format and only one value. */
TEST_CASE(decoder, params, one_value, NULL)
{
	struct tuple_decoder decoder;
	uint8_t buf[][8] = {
	    {1, SQLITE_INTEGER, 0, 0, 0, 0, 0, 0},
	    {7, 0, 0, 0, 0, 0, 0, 0},
	};
	struct cursor cursor = {buf, sizeof buf};
	struct value value;

	(void)data;
	(void)params;

	DECODER_INIT(0);
	DECODER_NEXT;

	ASSERT_VALUE_TYPE(SQLITE_INTEGER);
	munit_assert_int(value.integer, ==, 7);

	return MUNIT_OK;
}

/* Decode the next tuple, with params format and two values. */
TEST_CASE(decoder, params, two_values, NULL)
{
	struct tuple_decoder decoder;
	uint8_t buf[][8] = {
	    {2, SQLITE_INTEGER, SQLITE_TEXT, 0, 0, 0, 0, 0},
	    {7, 0, 0, 0, 0, 0, 0, 0},
	    {'h', 'e', 'l', 'l', 'o', 0, 0, 0},
	};
	struct cursor cursor = {buf, sizeof buf};
	struct value value;

	(void)data;
	(void)params;

	DECODER_INIT(0);
	DECODER_NEXT;

	ASSERT_VALUE_TYPE(SQLITE_INTEGER);
	munit_assert_int(value.integer, ==, 7);

	DECODER_NEXT;

	ASSERT_VALUE_TYPE(SQLITE_TEXT);
	munit_assert_string_equal(value.text, "hello");

	return MUNIT_OK;
}

TEST_GROUP(decoder, type);

/* Decode a floating point number. */
TEST_CASE(decoder, type, float, NULL)
{
	struct tuple_decoder decoder;
	uint8_t buf[][8] = {
	    {SQLITE_FLOAT, 0, 0, 0, 0, 0, 0, 0},
	    {0, 0, 0, 0, 0, 0, 0, 0},
	};
	struct cursor cursor = {buf, sizeof buf};
	struct value value;
	double pi = 3.1415;

	(void)data;
	(void)params;

	memcpy(buf[1], &pi, sizeof pi);
	*(uint64_t *)buf[1] = byte__flip64(*(uint64_t *)buf[1]);

	DECODER_INIT(1);
	DECODER_NEXT;

	ASSERT_VALUE_TYPE(SQLITE_FLOAT);
	munit_assert_double(value.float_, ==, 3.1415);

	return MUNIT_OK;
}

/* Decode a null value. */
TEST_CASE(decoder, type, null, NULL)
{
	struct tuple_decoder decoder;
	uint8_t buf[][8] = {
	    {SQLITE_NULL, 0, 0, 0, 0, 0, 0, 0},
	    {0, 0, 0, 0, 0, 0, 0, 0},
	};
	struct cursor cursor = {buf, sizeof buf};
	struct value value;

	(void)data;
	(void)params;

	DECODER_INIT(1);
	DECODER_NEXT;

	ASSERT_VALUE_TYPE(SQLITE_NULL);

	return MUNIT_OK;
}

/* Decode a date string in ISO8601 format. */
TEST_CASE(decoder, type, iso8601, NULL)
{
	struct tuple_decoder decoder;
	uint8_t buf[5][8] = {
	    {DQLITE_ISO8601, 0, 0, 0, 0, 0, 0, 0},
	};
	struct cursor cursor = {buf, sizeof buf};
	struct value value;

	(void)data;
	(void)params;

	strcpy((char *)buf[1], "2018-07-20 09:49:05+00:00");

	DECODER_INIT(1);
	DECODER_NEXT;

	ASSERT_VALUE_TYPE(DQLITE_ISO8601);
	munit_assert_string_equal(value.iso8601, "2018-07-20 09:49:05+00:00");

	return MUNIT_OK;
}

/* Decode a boolean. */
TEST_CASE(decoder, type, boolean, NULL)
{
	struct tuple_decoder decoder;
	uint8_t buf[][8] = {
	    {DQLITE_BOOLEAN, 0, 0, 0, 0, 0, 0, 0},
	    {1, 0, 0, 0, 0, 0, 0, 0},
	};
	struct cursor cursor = {buf, sizeof buf};
	struct value value;

	(void)data;
	(void)params;

	DECODER_INIT(1);
	DECODER_NEXT;

	ASSERT_VALUE_TYPE(DQLITE_BOOLEAN);
	munit_assert_int(value.boolean, ==, 1);

	return MUNIT_OK;
}
