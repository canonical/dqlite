#ifndef DQLITE_TEST_INTEGRATION_H
#define DQLITE_TEST_INTEGRATION_H

#include <CUnit/CUnit.h>

int test_dqlite_init();
int test_dqlite_cleanup();

void test_dqlite();

CU_TestInfo dqlite_suite[] = {
	{ "dqlite", test_dqlite },
	CU_TEST_INFO_NULL,
};
;

CU_SuiteInfo dqlite_integration_suites[] = {
	{
		"dqlite",
		test_dqlite_init, test_dqlite_cleanup,
		NULL, NULL,
		dqlite_suite
	},
	CU_SUITE_INFO_NULL,
};

#endif /* DQLITE_TEST_INTEGRATION_H */
