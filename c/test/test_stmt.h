#ifndef DQLITE_TEST_STMT_H
#define DQLITE_TEST_STMT_H

#include <CUnit/CUnit.h>

void test_dqlite__stmt_setup();
void test_dqlite__stmt_teardown();

void test_dqlite__stmt_prepare();

CU_TestInfo dqlite__stmt_open_suite[] = {
	{"prepare", test_dqlite__stmt_prepare},
	CU_TEST_INFO_NULL,
};

CU_SuiteInfo dqlite__stmt_suites[] = {
	{
		"dqlite__stmt_open",
		NULL, NULL,
		test_dqlite__stmt_setup, test_dqlite__stmt_teardown,
		dqlite__stmt_open_suite
	},
	CU_SUITE_INFO_NULL,
};

#endif /* DQLITE_TEST_STMT_H */
