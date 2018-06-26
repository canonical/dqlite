#ifndef DQLITE_TEST_SERVER_H
#define DQLITE_TEST_SERVER_H

#include <CUnit/CUnit.h>

void test_dqlite_server_setup();
void test_dqlite_server_teardown();

void test_dqlite_server_lifecycle();

CU_TestInfo dqlite_server_config_suite[] = {
	{ "lifecycle",       test_dqlite_server_lifecycle },
	CU_TEST_INFO_NULL,
};

CU_SuiteInfo dqlite_server_suites[] = {
	{
		"dqlite server",
		NULL, NULL,
		test_dqlite_server_setup, test_dqlite_server_teardown,
		dqlite_server_config_suite,
	},
	CU_SUITE_INFO_NULL,
};

#endif /* DQLITE_TEST_SERVER_H */
