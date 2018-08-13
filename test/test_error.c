#include <uv.h>

#include "../include/dqlite.h"
#include "../src/error.h"

#include "case.h"
#include "mem.h"

/******************************************************************************
 *
 * Setup and tear down
 *
 ******************************************************************************/

static void *setup(const MunitParameter params[], void *user_data)
{
	dqlite__error *error;

	test_case_setup(params, user_data);

	error = (dqlite__error *)munit_malloc(sizeof(*error));

	dqlite__error_init(error);

	return error;
}

static void tear_down(void *data)
{
	dqlite__error *error = data;

	dqlite__error_close(error);

	test_case_tear_down(data);
}

/******************************************************************************
 *
 * dqlite__error_printf
 *
 ******************************************************************************/

static MunitResult test_printf(const MunitParameter params[], void *data)
{
	dqlite__error *error = data;

	(void)params;

	munit_assert_true(dqlite__error_is_null(error));

	dqlite__error_printf(error, "hello %s", "world");

	munit_assert_string_equal(*error, "hello world");

	return MUNIT_OK;
}

static MunitResult test_printf_override(const MunitParameter params[],
                                        void *               data)
{
	dqlite__error *error = data;

	(void)params;

	dqlite__error_printf(error, "hello %s", "world");
	dqlite__error_printf(error, "I'm %s!", "here");

	munit_assert_string_equal(*error, "I'm here!");

	return MUNIT_OK;
}

static MunitResult test_printf_oom(const MunitParameter params[], void *data)
{
	dqlite__error *error = data;

	(void)params;

	test_mem_fault_config(0, 1);
	test_mem_fault_enable();

	dqlite__error_printf(error, "hello %s", "world");

	munit_assert_string_equal(*error,
	                          "error message unavailable (out of memory)");

	return MUNIT_OK;
}

static MunitTest dqlite__error_printf_tests[] = {
    {"/", test_printf, setup, tear_down, 0, NULL},
    {"/override", test_printf_override, setup, tear_down, 0, NULL},
    {"/oom", test_printf_oom, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * dqlite__error_wrapf
 *
 ******************************************************************************/

static MunitResult test_wrapf(const MunitParameter params[], void *data)
{
	dqlite__error *error = data;
	dqlite__error  cause;

	(void)params;

	dqlite__error_init(&cause);

	dqlite__error_printf(&cause, "hello %s", "world");

	dqlite__error_wrapf(error, &cause, "boom");

	dqlite__error_close(&cause);

	munit_assert_string_equal(*error, "boom: hello world");

	return MUNIT_OK;
}

static MunitResult test_wrapf_null_cause(const MunitParameter params[],
                                         void *               data)
{
	dqlite__error *error = data;
	dqlite__error  cause;

	(void)params;

	dqlite__error_init(&cause);

	dqlite__error_wrapf(error, &cause, "boom");

	dqlite__error_close(&cause);

	munit_assert_string_equal(*error, "boom: (null)");

	return MUNIT_OK;
}

static MunitResult test_wrapf_itself(const MunitParameter params[], void *data)
{
	dqlite__error *error = data;

	(void)params;

	dqlite__error_printf(error, "I'm %s!", "here");

	dqlite__error_wrapf(error, error, "boom");

	munit_assert_string_equal(*error, "boom: I'm here!");

	return MUNIT_OK;
}

static MunitTest dqlite__error_wrapf_tests[] = {
    {"/", test_wrapf, setup, tear_down, 0, NULL},
    {"/null_cause", test_wrapf_null_cause, setup, tear_down, 0, NULL},
    {"/itself", test_wrapf_itself, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * dqlite__error_oom
 *
 ******************************************************************************/

static MunitResult test_oom(const MunitParameter params[], void *data)
{
	dqlite__error *error = data;

	(void)params;

	dqlite__error_oom(error, "boom");

	munit_assert_string_equal(*error, "boom: out of memory");

	return MUNIT_OK;
}

static MunitResult test_oom_vargs(const MunitParameter params[], void *data)
{
	dqlite__error *error = data;

	(void)params;

	dqlite__error_oom(error, "boom %d", 123);

	munit_assert_string_equal(*error, "boom 123: out of memory");

	return MUNIT_OK;
}

static MunitTest dqlite__error_oom_tests[] = {
    {"/", test_oom, setup, tear_down, 0, NULL},
    {"/vargs", test_oom_vargs, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * dqlite__error_sys
 *
 ******************************************************************************/

static MunitResult test_sys(const MunitParameter params[], void *data)
{
	dqlite__error *error = data;

	(void)params;

	open("/foo/bar/egg/baz", 0);
	dqlite__error_sys(error, "boom");

	munit_assert_string_equal(*error, "boom: No such file or directory");

	return MUNIT_OK;
}

static MunitTest dqlite__error_sys_tests[] = {
    {"/", test_sys, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * dqlite__error_uv
 *
 ******************************************************************************/

static MunitResult test_uv(const MunitParameter params[], void *data)
{
	dqlite__error *error = data;

	(void)params;

	dqlite__error_uv(error, UV_EBUSY, "boom");

	munit_assert_string_equal(*error,
	                          "boom: resource busy or locked (EBUSY)");

	return MUNIT_OK;
}

static MunitTest dqlite__error_uv_tests[] = {
    {"/", test_uv, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * dqlite__error_copy
 *
 ******************************************************************************/

static MunitResult test_copy(const MunitParameter params[], void *data)
{
	dqlite__error *error = data;
	int            err;
	char *         msg;

	(void)params;

	dqlite__error_printf(error, "hello %s", "world");
	err = dqlite__error_copy(error, &msg);

	munit_assert_int(err, ==, 0);
	munit_assert_string_equal(msg, "hello world");

	sqlite3_free(msg);

	return MUNIT_OK;
}

static MunitResult test_copy_null(const MunitParameter params[], void *data)
{
	dqlite__error *error = data;
	int            err;
	char *         msg;

	(void)params;

	err = dqlite__error_copy(error, &msg);

	munit_assert_int(err, ==, DQLITE_ERROR);
	munit_assert_ptr_equal(msg, NULL);

	return MUNIT_OK;
}

static MunitResult test_copy_oom(const MunitParameter params[], void *data)
{
	dqlite__error *error = data;
	int            err;
	char *         msg;

	(void)params;

	test_mem_fault_config(2, 1);
	test_mem_fault_enable();

	dqlite__error_printf(error, "hello");

	err = dqlite__error_copy(error, &msg);

	munit_assert_int(err, ==, DQLITE_NOMEM);
	munit_assert_ptr_equal(msg, NULL);

	return MUNIT_OK;
}

static MunitTest dqlite__error_copy_tests[] = {
    {"/", test_copy, setup, tear_down, 0, NULL},
    {"/null", test_copy_null, setup, tear_down, 0, NULL},
    {"/oom", test_copy_oom, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * dqlite__error_is_disconnect
 *
 ******************************************************************************/

static MunitResult test_is_disconnect_eof(const MunitParameter params[],
                                          void *               data)
{
	dqlite__error *error = data;

	(void)params;

	dqlite__error_uv(error, UV_EOF, "boom");

	munit_assert_true(dqlite__error_is_disconnect(error));

	return MUNIT_OK;
}

static MunitResult test_is_disconnect_econnreset(const MunitParameter params[],
                                                 void *               data)
{
	dqlite__error *error = data;

	(void)params;

	dqlite__error_uv(error, UV_ECONNRESET, "boom");

	munit_assert_true(dqlite__error_is_disconnect(error));

	return MUNIT_OK;
}

static MunitResult test_is_disconnect_other(const MunitParameter params[],
                                            void *               data)
{
	dqlite__error *error = data;

	(void)params;

	dqlite__error_printf(error, "boom");

	munit_assert_true(!dqlite__error_is_disconnect(error));

	return MUNIT_OK;
}

static MunitResult test_is_disconnect_null(const MunitParameter params[],
                                           void *               data)
{
	dqlite__error *error = data;

	(void)params;

	munit_assert_true(!dqlite__error_is_disconnect(error));

	return MUNIT_OK;
}

static MunitTest dqlite__error_is_disconnect_tests[] = {
    {"/eof", test_is_disconnect_eof, setup, tear_down, 0, NULL},
    {"/econnreset", test_is_disconnect_econnreset, setup, tear_down, 0, NULL},
    {"/other", test_is_disconnect_other, setup, tear_down, 0, NULL},
    {"/null", test_is_disconnect_null, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * Test suite
 *
 ******************************************************************************/

MunitSuite dqlite__error_suites[] = {
    {"_printf", dqlite__error_printf_tests, NULL, 1, 0},
    {"_wrapf", dqlite__error_wrapf_tests, NULL, 1, 0},
    {"_oom", dqlite__error_oom_tests, NULL, 1, 0},
    {"_sys", dqlite__error_sys_tests, NULL, 1, 0},
    {"_uv", dqlite__error_uv_tests, NULL, 1, 0},
    {"_copy", dqlite__error_copy_tests, NULL, 1, 0},
    {"_is_disconnect", dqlite__error_is_disconnect_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
