#include <CUnit/CUnit.h>
#include <uv.h>

#include "../src/error.h"
#include "../include/dqlite.h"

static dqlite__error error;

void test_dqlite__error_setup()
{
	dqlite__error_init(&error);
}

void test_dqlite__error_teardown()
{
	dqlite__error_close(&error);
}

void test_dqlite__error_printf()
{
	CU_ASSERT_EQUAL(dqlite__error_is_null(&error), 1);

	dqlite__error_printf(&error, "hello %s", "world");

	CU_ASSERT_STRING_EQUAL(error, "hello world");
}

void test_dqlite__error_printf_override()
{
	dqlite__error_printf(&error, "hello %s", "world");
	dqlite__error_printf(&error, "I'm %s!", "here");

	CU_ASSERT_STRING_EQUAL(error, "I'm here!");
}

void test_dqlite__error_wrapf()
{
	static dqlite__error cause;
	dqlite__error_init(&cause);

	dqlite__error_printf(&cause, "hello %s", "world");

	dqlite__error_wrapf(&error, &cause, "boom");

	dqlite__error_close(&cause);

	CU_ASSERT_STRING_EQUAL(error, "boom: hello world");
}

void test_dqlite__error_wrapf_null_cause()
{
	static dqlite__error cause;
	dqlite__error_init(&cause);

	dqlite__error_wrapf(&error, &cause, "boom");

	dqlite__error_close(&cause);

	CU_ASSERT_STRING_EQUAL(error, "boom: (null)");
}

void test_dqlite__error_wrapf_itself()
{
	dqlite__error_printf(&error, "I'm %s!", "here");

	dqlite__error_wrapf(&error, &error, "boom");

	CU_ASSERT_STRING_EQUAL(error, "boom: I'm here!");
}

void test_dqlite__error_oom()
{
	dqlite__error_oom(&error, "boom");

	CU_ASSERT_STRING_EQUAL(error, "boom: out of memory");
}

void test_dqlite__error_uv()
{
	dqlite__error_uv(&error, UV_EBUSY, "boom");

	CU_ASSERT_STRING_EQUAL(error, "boom: resource busy or locked (EBUSY)");
}

void test_dqlite__error_copy()
{
	int err;
	char *msg;

	dqlite__error_printf(&error, "hello %s", "world");
	err = dqlite__error_copy(&error, &msg);

	CU_ASSERT_EQUAL(err, 0);
	CU_ASSERT_STRING_EQUAL(msg, "hello world");

	sqlite3_free(msg);
}

void test_dqlite__error_copy_null()
{
	int err;
	char *msg;

	err = dqlite__error_copy(&error, &msg);

	CU_ASSERT_EQUAL(err, DQLITE_ERROR);
	CU_ASSERT_PTR_NULL(msg);
}

void test_dqlite__error_is_disconnect_eof()
{
	dqlite__error_uv(&error, UV_EOF, "boom");

	CU_ASSERT(dqlite__error_is_disconnect(&error));
}

void test_dqlite__error_is_disconnect_econnreset()
{
	dqlite__error_uv(&error, UV_ECONNRESET, "boom");

	CU_ASSERT(dqlite__error_is_disconnect(&error));
}

void test_dqlite__error_is_disconnect_other()
{
	dqlite__error_printf(&error, "boom");

	CU_ASSERT(!dqlite__error_is_disconnect(&error));
}

void test_dqlite__error_is_disconnect_null()
{
	CU_ASSERT(!dqlite__error_is_disconnect(&error));
}
