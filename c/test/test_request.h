#ifndef DQLITE_REQUEST_TEST_H
#define DQLITE_REQUEST_TEST_H

#include <CUnit/CUnit.h>

void test_dqlite__request_setup();
void test_dqlite__request_teardown();

void test_dqlite__request_preamble_size();
void test_dqlite__request_preamble_one_segment();
void test_dqlite__request_preamble_two_segments();

CU_TestInfo dqlite__request_preamble_suite[] = {
	{"size",         test_dqlite__request_preamble_size},
	{"one segment",  test_dqlite__request_preamble_one_segment},
	{"two segments", test_dqlite__request_preamble_two_segments},
	CU_TEST_INFO_NULL,
};

void test_dqlite__request_header_size();
void test_dqlite__request_header_valid_segment();
void test_dqlite__request_header_empty_segment();
void test_dqlite__request_header_big_segment();

CU_TestInfo dqlite__request_header_suite[] = {
	{"size",          test_dqlite__request_header_size},
	{"valid segment", test_dqlite__request_header_valid_segment},
	{"empty segment", test_dqlite__request_header_empty_segment},
	{"big segment",   test_dqlite__request_header_big_segment},
	CU_TEST_INFO_NULL,
};

void test_dqlite__request_type_helo();
void test_dqlite__request_type_heartbeat();
void test_dqlite__request_type_unknown();

CU_TestInfo dqlite__request_type_suite[] = {
	{"helo",            test_dqlite__request_type_helo},
	{"heartbeat",       test_dqlite__request_type_heartbeat},
	{"unknown",         test_dqlite__request_type_unknown},
	CU_TEST_INFO_NULL,
};

CU_SuiteInfo dqlite__request_suites[] = {
	{
		"dqlite__request_preamble",
		NULL, NULL,
		test_dqlite__request_setup, test_dqlite__request_teardown,
		dqlite__request_preamble_suite
	},
	{
		"dqlite__request_header",
		NULL, NULL,
		test_dqlite__request_setup, test_dqlite__request_teardown,
		dqlite__request_header_suite
	},
	{
		"dqlite__request_type",
		NULL, NULL,
		test_dqlite__request_setup, test_dqlite__request_teardown,
		dqlite__request_type_suite
	},
	CU_SUITE_INFO_NULL,
};

#endif /* DQLITE_REQUEST_TEST_H */
