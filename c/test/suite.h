#ifndef DQLITE_TEST_SUITE_H
#define DQLITE_TEST_SUITE_H

#include <CUnit/CUnit.h>

/* Public interfaces that be used by tests */
FILE *test_suite_dqlite_log();

void test_suite_printf(const char*, ...);
void test_suite_errorf(const char*, ...);

/* Helper for test teardown function completing cleanly */
void test_suite_teardown_pass();

/* Helper for test teardown functions hitting an error */
void test_suite_teardown_fail();

/* Private interfaces used by the test runner */
void test__suite_start_cb(const CU_pSuite);
void test__suite_init_failure_cb(const CU_pSuite);
void test__suite_complete_cb(const CU_pSuite, const CU_pFailureRecord);

#endif /* DQLITE_TEST_SUITE_H */
