#ifndef DQLITE_TEST_QUEUE_H
#define DQLITE_TEST_QUEUE_H

#include <CUnit/CUnit.h>

void test_dqlite__queue_setup();
void test_dqlite__queue_teardown();

void test_dqlite__queue_push();

CU_TestInfo dqlite__queue_push_suite[] = {
	{"push", test_dqlite__queue_push},
	CU_TEST_INFO_NULL,
};

void test_dqlite__queue_process();

CU_TestInfo dqlite__queue_process_suite[] = {
	{"process", test_dqlite__queue_process},
	CU_TEST_INFO_NULL,
};

CU_SuiteInfo dqlite__queue_suites[] = {
	{
		"dqlite__queue_push",
		NULL, NULL,
		test_dqlite__queue_setup, test_dqlite__queue_teardown,
		dqlite__queue_push_suite
	},
	{
		"dqlite__queue_process",
		NULL, NULL,
		test_dqlite__queue_setup, test_dqlite__queue_teardown,
		dqlite__queue_process_suite
	},
	CU_SUITE_INFO_NULL,
};

#endif /* DQLITE_TEST_QUEUE_H */
