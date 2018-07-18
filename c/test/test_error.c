#include <uv.h>

#include "../include/dqlite.h"
#include "../src/error.h"

#include "leak.h"
#include "munit.h"

static void *setup(const MunitParameter params[], void *user_data) {
	dqlite__error *error;

	(void)params;
	(void)user_data;

	error = (dqlite__error *)munit_malloc(sizeof(*error));

	dqlite__error_init(error);

	return error;
}

static void tear_down(void *data) {
	dqlite__error *error = data;

	dqlite__error_close(error);

	test_assert_no_leaks();
}

static MunitResult test_printf(const MunitParameter params[], void *data) {
	dqlite__error *error = data;

	(void)params;

	munit_assert_true(dqlite__error_is_null(error));

	dqlite__error_printf(error, "hello %s", "world");

	munit_assert_string_equal(*error, "hello world");

	return MUNIT_OK;
}

static MunitResult test_printf_override(const MunitParameter params[], void *data) {
	dqlite__error *error = data;

	(void)params;

	dqlite__error_printf(error, "hello %s", "world");
	dqlite__error_printf(error, "I'm %s!", "here");

	munit_assert_string_equal(*error, "I'm here!");

	return MUNIT_OK;
}

static MunitResult test_wrapf(const MunitParameter params[], void *data) {
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

static MunitResult test_wrapf_null_cause(const MunitParameter params[], void *data) {
	dqlite__error *error = data;
	dqlite__error  cause;

	(void)params;

	dqlite__error_init(&cause);

	dqlite__error_wrapf(error, &cause, "boom");

	dqlite__error_close(&cause);

	munit_assert_string_equal(*error, "boom: (null)");

	return MUNIT_OK;
}

static MunitResult test_wrapf_itself(const MunitParameter params[], void *data) {
	dqlite__error *error = data;

	(void)params;

	dqlite__error_printf(error, "I'm %s!", "here");

	dqlite__error_wrapf(error, error, "boom");

	munit_assert_string_equal(*error, "boom: I'm here!");

	return MUNIT_OK;
}

static MunitResult test_oom(const MunitParameter params[], void *data) {
	dqlite__error *error = data;

	(void)params;

	dqlite__error_oom(error, "boom");

	munit_assert_string_equal(*error, "boom: out of memory");

	return MUNIT_OK;
}

static MunitResult test_uv(const MunitParameter params[], void *data) {
	dqlite__error *error = data;

	(void)params;

	dqlite__error_uv(error, UV_EBUSY, "boom");

	munit_assert_string_equal(*error, "boom: resource busy or locked (EBUSY)");

	return MUNIT_OK;
}

static MunitResult test_copy(const MunitParameter params[], void *data) {
	dqlite__error *error = data;
	int	    err;
	char *	 msg;

	(void)params;

	dqlite__error_printf(error, "hello %s", "world");
	err = dqlite__error_copy(error, &msg);

	munit_assert_int(err, ==, 0);
	munit_assert_string_equal(msg, "hello world");

	sqlite3_free(msg);

	return MUNIT_OK;
}

static MunitResult test_copy_null(const MunitParameter params[], void *data) {
	dqlite__error *error = data;
	int	    err;
	char *	 msg;

	(void)params;

	err = dqlite__error_copy(error, &msg);

	munit_assert_int(err, ==, DQLITE_ERROR);
	munit_assert_ptr_equal(msg, NULL);

	return MUNIT_OK;
}

static MunitResult test_is_disconnect_eof(const MunitParameter params[], void *data) {
	dqlite__error *error = data;

	(void)params;

	dqlite__error_uv(error, UV_EOF, "boom");

	munit_assert_true(dqlite__error_is_disconnect(error));

	return MUNIT_OK;
}

static MunitResult test_is_disconnect_econnreset(const MunitParameter params[], void *data) {
	dqlite__error *error = data;

	(void)params;

	dqlite__error_uv(error, UV_ECONNRESET, "boom");

	munit_assert_true(dqlite__error_is_disconnect(error));

	return MUNIT_OK;
}

static MunitResult test_is_disconnect_other(const MunitParameter params[], void *data) {
	dqlite__error *error = data;

	(void)params;

	dqlite__error_printf(error, "boom");

	munit_assert_true(!dqlite__error_is_disconnect(error));

	return MUNIT_OK;
}

static MunitResult test_is_disconnect_null(const MunitParameter params[], void *data) {
	dqlite__error *error = data;

	(void)params;

	munit_assert_true(!dqlite__error_is_disconnect(error));

	return MUNIT_OK;
}

static MunitTest dqlite__error_tests[] = {
    {"_printf", test_printf, setup, tear_down, MUNIT_TEST_OPTION_NONE, NULL},
    {"_printf/override", test_printf_override, setup, tear_down, MUNIT_TEST_OPTION_NONE, NULL},
    {"_wrapf", test_wrapf, setup, tear_down, MUNIT_TEST_OPTION_NONE, NULL},
    {"_wrapf/null_cause", test_wrapf_null_cause, setup, tear_down, MUNIT_TEST_OPTION_NONE, NULL},
    {"_wrapf/itself", test_wrapf_itself, setup, tear_down, MUNIT_TEST_OPTION_NONE, NULL},
    {"_oom", test_oom, setup, tear_down, MUNIT_TEST_OPTION_NONE, NULL},
    {"_uv", test_uv, setup, tear_down, MUNIT_TEST_OPTION_NONE, NULL},
    {"_copy", test_copy, setup, tear_down, MUNIT_TEST_OPTION_NONE, NULL},
    {"_copy/null", test_copy_null, setup, tear_down, MUNIT_TEST_OPTION_NONE, NULL},
    {"_is_disconnect/eof", test_is_disconnect_eof, setup, tear_down, MUNIT_TEST_OPTION_NONE, NULL},
    {"_is_disconnect/econnreset", test_is_disconnect_econnreset, setup, tear_down, MUNIT_TEST_OPTION_NONE, NULL},
    {"_is_disconnect/other", test_is_disconnect_other, setup, tear_down, MUNIT_TEST_OPTION_NONE, NULL},
    {"_is_disconnect/null", test_is_disconnect_null, setup, tear_down, MUNIT_TEST_OPTION_NONE, NULL},
    {NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL},
};

MunitSuite dqlite__error_suites[] = {
    {"", dqlite__error_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE},
    {NULL, NULL, NULL, 0, MUNIT_SUITE_OPTION_NONE},
};
