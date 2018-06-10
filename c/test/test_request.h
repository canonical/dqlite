#ifndef DQLITE_REQUEST_TEST_H
#define DQLITE_REQUEST_TEST_H

#include <CUnit/CUnit.h>

void test_dqlite__request_setup();
void test_dqlite__request_teardown();

void test_dqlite__request_body_received_too_large();
void test_dqlite__request_body_received_malformed_helo();
void test_dqlite__request_body_received_malformed_heartbeat();
void test_dqlite__request_body_received_helo();
void test_dqlite__request_body_received_heartbeat();
void test_dqlite__request_body_received_open();

CU_TestInfo dqlite__request_body_received_suite[] = {
	{"too large",           test_dqlite__request_body_received_too_large},
	{"malformed helo",      test_dqlite__request_body_received_malformed_helo},
	{"malformed heartbeat", test_dqlite__request_body_received_malformed_heartbeat},
	{"helo",                test_dqlite__request_body_received_helo},
	{"heartbeat",           test_dqlite__request_body_received_heartbeat},
	{"open",                test_dqlite__request_body_received_open},
	CU_TEST_INFO_NULL,
};

void test_dqlite__request_type__name_helo();
void test_dqlite__request_type__name_heartbeat();
void test_dqlite__request_type__name_open();
void test_dqlite__request_type__name_unknown();

CU_TestInfo dqlite__request_type_name_suite[] = {
	{"helo",      test_dqlite__request_type__name_helo},
	{"heartbeat", test_dqlite__request_type__name_heartbeat},
	{"open",      test_dqlite__request_type__name_open},
	{"unknown",   test_dqlite__request_type__name_unknown},
	CU_TEST_INFO_NULL,
};

CU_SuiteInfo dqlite__request_suites[] = {
	{
		"dqlite__request_body_received",
		NULL, NULL,
		test_dqlite__request_setup, test_dqlite__request_teardown,
		dqlite__request_body_received_suite
	},
	{
		"dqlite__request_type_name",
		NULL, NULL,
		test_dqlite__request_setup, test_dqlite__request_teardown,
		dqlite__request_type_name_suite
	},
	CU_SUITE_INFO_NULL,
};

#endif /* DQLITE_REQUEST_TEST_H */
