#ifndef DQLITE_ERROR_TEST_H
#define DQLITE_ERROR_TEST_H

#include <CUnit/CUnit.h>

void test_dqlite__error_setup();
void test_dqlite__error_teardown();

void test_dqlite__error_printf();
void test_dqlite__error_printf_override();
void test_dqlite__error_wrapf();
void test_dqlite__error_wrapf_null_cause();
void test_dqlite__error_wrapf_itself();
void test_dqlite__error_oom();
void test_dqlite__error_uv();
void test_dqlite__error_copy();
void test_dqlite__error_copy_null();
void test_dqlite__error_is_disconnect_eof();
void test_dqlite__error_is_disconnect_econnreset();
void test_dqlite__error_is_disconnect_other();
void test_dqlite__error_is_disconnect_null();

CU_TestInfo dqlite__error_suite[] = {
	{"printf",               test_dqlite__error_printf},
	{"printf_override",      test_dqlite__error_printf_override},
	{"wrapf",                test_dqlite__error_wrapf},
	{"wrapf null cause",     test_dqlite__error_wrapf_null_cause},
	{"wrapf itself",         test_dqlite__error_wrapf_itself},
	{"oom",                  test_dqlite__error_oom},
	{"uv",                   test_dqlite__error_uv},
	{"copy",                 test_dqlite__error_copy},
	{"copy null",            test_dqlite__error_copy_null},
	{"disconnect EOF",       test_dqlite__error_is_disconnect_eof},
	{"disconnect ECONNRESET",test_dqlite__error_is_disconnect_econnreset},
	{"disconnect other",     test_dqlite__error_is_disconnect_other},
	{"disconnect null",      test_dqlite__error_is_disconnect_null},
	CU_TEST_INFO_NULL,
};

CU_SuiteInfo dqlite__error_suites[] = {
	{
		"dqlite__error",
		NULL, NULL,
		test_dqlite__error_setup, test_dqlite__error_teardown,
		dqlite__error_suite
	},
	CU_SUITE_INFO_NULL,
};

#endif /* DQLITE_ERROR_TEST_H */
