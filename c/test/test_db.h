#ifndef DQLITE_TEST_DB_H
#define DQLITE_TEST_DB_H

#include <CUnit/CUnit.h>

void test_dqlite__db_setup();
void test_dqlite__db_teardown();

void test_dqlite__db_foo();

CU_TestInfo dqlite__db_foo_suite[] = {
	{"foo",             test_dqlite__db_foo},
	CU_TEST_INFO_NULL,
};

CU_SuiteInfo dqlite__db_suites[] = {
	{
		"dqlite__db foo",
		NULL, NULL,
		test_dqlite__db_setup, test_dqlite__db_teardown,
		dqlite__db_foo_suite
	},
	CU_SUITE_INFO_NULL,
};

#endif /* DQLITE_TEST_DB_H */
