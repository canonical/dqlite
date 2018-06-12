#ifndef DQLITE_TEST_REGISTRY_H
#define DQLITE_TEST_REGISTRY_H

#include <CUnit/CUnit.h>

void test_dqlite__registry_setup();
void test_dqlite__registry_teardown();

void test_dqlite__registry_add();
void test_dqlite__registry_get();
void test_dqlite__registry_get_deleted();
void test_dqlite__registry_get_out_of_bound();
void test_dqlite__registry_del();
void test_dqlite__registry_del_twice();
void test_dqlite__registry_del_out_of_bound();
void test_dqlite__registry_del_many();

CU_TestInfo dqlite__registry_suite[] = {
	{"add",              test_dqlite__registry_add},
	{"get",              test_dqlite__registry_get},
	{"get_deleted",      test_dqlite__registry_get_deleted},
	{"get out of bound", test_dqlite__registry_get_out_of_bound},
	{"del",              test_dqlite__registry_del},
	{"del twice",        test_dqlite__registry_del_twice},
	{"del out of bound", test_dqlite__registry_del_out_of_bound},
	{"del many",         test_dqlite__registry_del_many},
	CU_TEST_INFO_NULL,
};

CU_SuiteInfo dqlite__registry_suites[] = {
	{
		"dqlite__registry",
		NULL, NULL,
		test_dqlite__registry_setup, test_dqlite__registry_teardown,
		dqlite__registry_suite
	},
	CU_SUITE_INFO_NULL,
};

#endif /* DQLITE_TEST_REGISTRY_H */
