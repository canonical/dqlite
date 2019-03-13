#include "../../../src/lib/serialization.h"

#include "../../lib/runner.h"

TEST_MODULE(lib_serialization);

/******************************************************************************
 *
 * Helpers
 *
 ******************************************************************************/

#define PERSON(X, ...)               \
	X(text, name, ##__VA_ARGS__) \
	X(uint64, age, ##__VA_ARGS__)

SERIALIZATION__DEFINE(person, PERSON);
SERIALIZATION__IMPLEMENT(person, PERSON);

struct fixture
{
	struct person person;
};

static void *setup(const MunitParameter params[], void *user_data)
{
	struct fixture *f = munit_malloc(sizeof *f);
	(void)params;
	(void)user_data;
	return f;
}

static void tear_down(void *data)
{
	free(data);
}

/******************************************************************************
 *
 * Fields definition.
 *
 ******************************************************************************/

TEST_SUITE(fields);
TEST_SETUP(fields, setup);
TEST_TEAR_DOWN(fields, tear_down);

/* The expected fields are defined on the struct. */
TEST_CASE(fields, define, NULL)
{
	struct fixture *f = data;
	(void)params;
	f->person.name = "John Doh";
	f->person.age = 40;
	return MUNIT_OK;
}

/******************************************************************************
 *
 * Sizeof method.
 *
 ******************************************************************************/

TEST_SUITE(sizeof);
TEST_SETUP(sizeof, setup);
TEST_TEAR_DOWN(sizeof, tear_down);

/* Padding is added if needed. */
TEST_CASE(sizeof, padding, NULL)
{
	struct fixture *f = data;
	size_t size;
	(void)params;
	f->person.name = "John Doh";
	f->person.age = 40;
	size = person__sizeof(&f->person);
	munit_assert_int(size, ==, 8 + 16);
	return MUNIT_OK;
}

/* Padding is not added if a string ends exactly at word boundary. */
TEST_CASE(sizeof, no_padding, NULL)
{
	struct fixture *f = data;
	size_t size;
	(void)params;
	f->person.name = "Joe Doh";
	f->person.age = 40;
	size = person__sizeof(&f->person);
	munit_assert_int(size, ==, 8 + 8);
	return MUNIT_OK;
}

/******************************************************************************
 *
 * Encode method.
 *
 ******************************************************************************/

TEST_SUITE(encode);
TEST_SETUP(encode, setup);
TEST_TEAR_DOWN(encode, tear_down);

/* Padding is added if needed. */
TEST_CASE(encode, padding, NULL)
{
	struct fixture *f = data;
	size_t size;
	void *buf;
	(void)params;
	f->person.name = "John Doh";
	f->person.age = 40;
	size = person__sizeof(&f->person);
	buf = munit_malloc(size);
	person__encode(&f->person, buf);
	munit_assert_string_equal(buf, "John Doh");
	munit_assert_int(byte__flip64(*(uint64_t *)(buf + 16)), ==, 40);
	free(buf);
	return MUNIT_OK;
}

/* Padding is not added if a string ends exactly at word boundary. */
TEST_CASE(encode, no_padding, NULL)
{
	struct fixture *f = data;
	size_t size;
	void *buf;
	(void)params;
	f->person.name = "Joe Doh";
	f->person.age = 40;
	size = person__sizeof(&f->person);
	buf = munit_malloc(size);
	person__encode(&f->person, buf);
	munit_assert_string_equal(buf, "Joe Doh");
	munit_assert_int(byte__flip64(*(uint64_t *)(buf + 8)), ==, 40);
	free(buf);
	return MUNIT_OK;
}

/******************************************************************************
 *
 * Decode method.
 *
 ******************************************************************************/

TEST_SUITE(decode);
TEST_SETUP(decode, setup);
TEST_TEAR_DOWN(decode, tear_down);

/* Padding is added if needed. */
TEST_CASE(decode, padding, NULL)
{
	struct fixture *f = data;
	void *buf = munit_malloc(16 + 8);
	(void)params;
	strcpy(buf, "John Doh");
	*(uint64_t *)(buf + 16) = byte__flip64(40);
	person__decode(buf, &f->person);
	munit_assert_string_equal(f->person.name, "John Doh");
	munit_assert_int(f->person.age, ==, 40);
	free(buf);
	return MUNIT_OK;
}

/* Padding is not added if a string ends exactly at word boundary. */
TEST_CASE(decode, no_padding, NULL)
{
	struct fixture *f = data;
	void *buf = munit_malloc(16 + 8);
	(void)params;
	strcpy(buf, "Joe Doh");
	*(uint64_t *)(buf + 8) = byte__flip64(40);
	person__decode(buf, &f->person);
	munit_assert_string_equal(f->person.name, "Joe Doh");
	munit_assert_int(f->person.age, ==, 40);
	free(buf);
	return MUNIT_OK;
}
