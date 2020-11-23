#include <uv.h>

#include "../include/dqlite.h"
#include "../src/error.h"

#include "./lib/heap.h"
#include "./lib/runner.h"
#include "./lib/sqlite.h"

TEST_MODULE(error);

/******************************************************************************
 *
 * Setup and tear down
 *
 ******************************************************************************/

static void *setup(const MunitParameter params[], void *user_data)
{
	dqliteError *error;

	testHeapSetup(params, user_data);
	testSqliteSetup(params);

	error = (dqliteError *)munit_malloc(sizeof(*error));

	dqliteError_init(error);

	return error;
}

static void tear_down(void *data)
{
	dqliteError *error = data;

	dqliteError_close(error);

	test_sqlite_tear_down();
	testHeapTearDown(data);

	free(error);
}

/******************************************************************************
 *
 * dqliteError_printf
 *
 ******************************************************************************/

TEST_SUITE(printf);
TEST_SETUP(printf, setup);
TEST_TEAR_DOWN(printf, tear_down);

TEST_CASE(printf, success, NULL)
{
	dqliteError *error = data;

	(void)params;

	munit_assert_true(dqliteError_is_null(error));

	dqliteError_printf(error, "hello %s", "world");

	munit_assert_string_equal(*error, "hello world");

	return MUNIT_OK;
}

TEST_CASE(printf, override, NULL)
{
	dqliteError *error = data;

	(void)params;

	dqliteError_printf(error, "hello %s", "world");
	dqliteError_printf(error, "I'm %s!", "here");

	munit_assert_string_equal(*error, "I'm here!");

	return MUNIT_OK;
}

TEST_CASE(printf, oom, NULL)
{
	dqliteError *error = data;

	(void)params;

	testHeapFaultConfig(0, 1);
	testHeapFaultEnable();

	dqliteError_printf(error, "hello %s", "world");

	munit_assert_string_equal(*error,
				  "error message unavailable (out of memory)");

	return MUNIT_OK;
}

/******************************************************************************
 *
 * dqliteError_wrapf
 *
 ******************************************************************************/

TEST_SUITE(wrapf);
TEST_SETUP(wrapf, setup);
TEST_TEAR_DOWN(wrapf, tear_down);

TEST_CASE(wrapf, success, NULL)
{
	dqliteError *error = data;
	dqliteError cause;

	(void)params;

	dqliteError_init(&cause);

	dqliteError_printf(&cause, "hello %s", "world");

	dqliteError_wrapf(error, &cause, "boom");

	dqliteError_close(&cause);

	munit_assert_string_equal(*error, "boom: hello world");

	return MUNIT_OK;
}

TEST_CASE(wrapf, nullCause, NULL)
{
	dqliteError *error = data;
	dqliteError cause;

	(void)params;

	dqliteError_init(&cause);

	dqliteError_wrapf(error, &cause, "boom");

	dqliteError_close(&cause);

	munit_assert_string_equal(*error, "boom: (null)");

	return MUNIT_OK;
}

TEST_CASE(wrapf, itself, NULL)
{
	dqliteError *error = data;

	(void)params;

	dqliteError_printf(error, "I'm %s!", "here");

	dqliteError_wrapf(error, error, "boom");

	munit_assert_string_equal(*error, "boom: I'm here!");

	return MUNIT_OK;
}

/******************************************************************************
 *
 * dqliteError_oom
 *
 ******************************************************************************/

TEST_SUITE(oom);
TEST_SETUP(oom, setup);
TEST_TEAR_DOWN(oom, tear_down);

TEST_CASE(oom, success, NULL)
{
	dqliteError *error = data;

	(void)params;

	dqliteError_oom(error, "boom");

	munit_assert_string_equal(*error, "boom: out of memory");

	return MUNIT_OK;
}

TEST_CASE(oom, vargs, NULL)
{
	dqliteError *error = data;

	(void)params;

	dqliteError_oom(error, "boom %d", 123);

	munit_assert_string_equal(*error, "boom 123: out of memory");

	return MUNIT_OK;
}

/******************************************************************************
 *
 * dqliteError_sys
 *
 ******************************************************************************/

TEST_SUITE(sys);
TEST_SETUP(sys, setup);
TEST_TEAR_DOWN(sys, tear_down);

TEST_CASE(sys, success, NULL)
{
	dqliteError *error = data;

	(void)params;

	open("/foo/bar/egg/baz", 0);
	dqliteError_sys(error, "boom");

	munit_assert_string_equal(*error, "boom: No such file or directory");

	return MUNIT_OK;
}

/******************************************************************************
 *
 * dqliteError_uv
 *
 ******************************************************************************/

TEST_SUITE(uv);
TEST_SETUP(uv, setup);
TEST_TEAR_DOWN(uv, tear_down);

TEST_CASE(uv, success, NULL)
{
	dqliteError *error = data;

	(void)params;

	dqliteError_uv(error, UV_EBUSY, "boom");

	munit_assert_string_equal(*error,
				  "boom: resource busy or locked (EBUSY)");

	return MUNIT_OK;
}

/******************************************************************************
 *
 * dqliteError_copy
 *
 ******************************************************************************/

TEST_SUITE(copy);
TEST_SETUP(copy, setup);
TEST_TEAR_DOWN(copy, tear_down);

TEST_CASE(copy, success, NULL)
{
	dqliteError *error = data;
	int err;
	char *msg;

	(void)params;

	dqliteError_printf(error, "hello %s", "world");
	err = dqliteError_copy(error, &msg);

	munit_assert_int(err, ==, 0);
	munit_assert_string_equal(msg, "hello world");

	sqlite3_free(msg);

	return MUNIT_OK;
}

TEST_CASE(copy, null, NULL)
{
	dqliteError *error = data;
	int err;
	char *msg;

	(void)params;

	err = dqliteError_copy(error, &msg);

	munit_assert_int(err, ==, DQLITE_ERROR);
	munit_assert_ptr_equal(msg, NULL);

	return MUNIT_OK;
}

TEST_CASE(copy, oom, NULL)
{
	dqliteError *error = data;
	int err;
	char *msg;

	(void)params;
	return MUNIT_SKIP;

	testHeapFaultConfig(2, 1);
	testHeapFaultEnable();

	dqliteError_printf(error, "hello");

	err = dqliteError_copy(error, &msg);

	munit_assert_int(err, ==, DQLITE_NOMEM);
	munit_assert_ptr_equal(msg, NULL);

	return MUNIT_OK;
}

/******************************************************************************
 *
 * dqliteError_isDisconnect
 *
 ******************************************************************************/

TEST_SUITE(isDisconnect);
TEST_SETUP(isDisconnect, setup);
TEST_TEAR_DOWN(isDisconnect, tear_down);

TEST_CASE(isDisconnect, eof, NULL)
{
	dqliteError *error = data;

	(void)params;

	dqliteError_uv(error, UV_EOF, "boom");

	munit_assert_true(dqliteError_isDisconnect(error));

	return MUNIT_OK;
}

TEST_CASE(isDisconnect, econnreset, NULL)
{
	dqliteError *error = data;

	(void)params;

	dqliteError_uv(error, UV_ECONNRESET, "boom");

	munit_assert_true(dqliteError_isDisconnect(error));

	return MUNIT_OK;
}

TEST_CASE(isDisconnect, other, NULL)
{
	dqliteError *error = data;

	(void)params;

	dqliteError_printf(error, "boom");

	munit_assert_true(!dqliteError_isDisconnect(error));

	return MUNIT_OK;
}

TEST_CASE(isDisconnect, null, NULL)
{
	dqliteError *error = data;

	(void)params;

	munit_assert_true(!dqliteError_isDisconnect(error));

	return MUNIT_OK;
}
