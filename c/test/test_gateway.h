#ifndef DQLITE_TEST_GATEWAY_H
#define DQLITE_TEST_GATEWAY_H

#include <CUnit/CUnit.h>

void test_dqlite__gateway_setup();
void test_dqlite__gateway_teardown();

void test_dqlite__gateway_handle_connect();
void test_dqlite__gateway_handle_connect_wrong_request_type();
void test_dqlite__gateway_heartbeat();

CU_TestInfo dqlite__gateway_handle_suite[] = {
	{"connect",                    test_dqlite__gateway_handle_connect},
	{"connect wrong request type", test_dqlite__gateway_handle_connect_wrong_request_type},
	{"heartbeat",                  test_dqlite__gateway_heartbeat},
	CU_TEST_INFO_NULL,
};
;

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
