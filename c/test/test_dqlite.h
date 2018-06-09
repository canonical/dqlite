#ifndef DQLITE_TEST_SERVER_H
#define DQLITE_TEST_SERVER_H

#include <CUnit/CUnit.h>

void test_dqlite_create();
void test_dqlite_destroy();

CU_TestInfo dqlite_lifecycle_test[] = {
	{ "dqlite_create", test_dqlite_create },
	{ "dqlite_destroy", test_dqlite_destroy },
	CU_TEST_INFO_NULL,
};

int dqlite_loop_init();
int dqlite_loop_cleanup();

void test_dqlite_start();
void test_dqlite_stop();

CU_TestInfo dqlite_loop_tests[] = {
	{ "dqlite_start", test_dqlite_start },
	{ "dqlite_stop", test_dqlite_stop },
	CU_TEST_INFO_NULL,
};

CU_SuiteInfo dqlite_suites[] = {
	{"dqlite lifecycle", NULL, NULL, NULL, NULL, dqlite_lifecycle_test},
	{"dqlite loop", dqlite_loop_init, dqlite_loop_cleanup, NULL, NULL, dqlite_loop_tests},
	CU_SUITE_INFO_NULL,
};

#endif /* DQLITE_TEST_SERVER_H */
