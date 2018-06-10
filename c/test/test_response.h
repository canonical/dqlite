#ifndef DQLITE_RESPONSE_TEST_H
#define DQLITE_RESPONSE_TEST_H

#include <CUnit/CUnit.h>

void test_dqlite__response_setup();
void test_dqlite__response_teardown();

void test_dqlite__response_welcome();
void test_dqlite__response_servers();
void test_dqlite__response_db();

CU_TestInfo dqlite__response_suite[] = {
	{"welcome",  test_dqlite__response_welcome},
	{"servers",  test_dqlite__response_servers},
	/* {"db",       test_dqlite__response_db}, */
	CU_TEST_INFO_NULL,
};
;

CU_SuiteInfo dqlite__response_suites[] = {
	{
		"dqlite__response",
		NULL, NULL,
		test_dqlite__response_setup, test_dqlite__response_teardown,
		dqlite__response_suite
	},
	CU_SUITE_INFO_NULL,
};

#endif /* DQLITE_RESPONSE_TEST_H */
