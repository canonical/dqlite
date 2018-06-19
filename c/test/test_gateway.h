#ifndef DQLITE_TEST_GATEWAY_H
#define DQLITE_TEST_GATEWAY_H

#include <CUnit/CUnit.h>

void test_dqlite__gateway_setup();
void test_dqlite__gateway_teardown();

void test_dqlite__gateway_helo();
void test_dqlite__gateway_heartbeat();
void test_dqlite__gateway_open();
void test_dqlite__gateway_open_error();
void test_dqlite__gateway_prepare();
void test_dqlite__gateway_prepare_error();
void test_dqlite__gateway_prepare_invalid_db_id();
void test_dqlite__gateway_exec();

CU_TestInfo dqlite__gateway_handle_suite[] = {
	{"helo",                  test_dqlite__gateway_helo},
	{"heartbeat",             test_dqlite__gateway_heartbeat},
	{"open",                  test_dqlite__gateway_open},
	{"open error",            test_dqlite__gateway_open_error},
	{"prepare",               test_dqlite__gateway_prepare},
	{"prepare error",         test_dqlite__gateway_prepare_error},
	{"prepare invalid db id", test_dqlite__gateway_prepare_invalid_db_id},
	{"exec" ,                 test_dqlite__gateway_exec},
	CU_TEST_INFO_NULL,
};

CU_SuiteInfo dqlite__gateway_suites[] = {
	{
		"dqlite__gateway_handle",
		NULL, NULL,
		test_dqlite__gateway_setup, test_dqlite__gateway_teardown,
		dqlite__gateway_handle_suite
	},
	CU_SUITE_INFO_NULL,
};

#endif /* DQLITE_TEST_GATEWAY_H */
