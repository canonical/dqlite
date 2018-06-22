#ifndef DQLITE_REQUEST_TEST_H
#define DQLITE_REQUEST_TEST_H

#include <CUnit/CUnit.h>

void test_dqlite__request_setup();
void test_dqlite__request_teardown();

void test_dqlite__request_decode_leader();
void test_dqlite__request_decode_client();
void test_dqlite__request_decode_heartbeat();
void test_dqlite__request_decode_open();

CU_TestInfo dqlite__request_decode_suite[] = {
	{"leader",              test_dqlite__request_decode_leader},
	{"client",              test_dqlite__request_decode_client},
	{"heartbeat",           test_dqlite__request_decode_heartbeat},
	{"open",                test_dqlite__request_decode_open},
	CU_TEST_INFO_NULL,
};

CU_SuiteInfo dqlite__request_suites[] = {
	{
		"dqlite__request_decode",
		NULL, NULL,
		test_dqlite__request_setup, test_dqlite__request_teardown,
		dqlite__request_decode_suite
	},
	CU_SUITE_INFO_NULL,
};

#endif /* DQLITE_REQUEST_TEST_H */
