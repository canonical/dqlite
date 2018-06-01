#define _GNU_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <errno.h>
#include <libgen.h>

#include <CUnit/CUnit.h>
#include <sqlite3.h>

#include "../src/lifecycle.h"

#include "log.h"

/* Dqlite log stream to pass to dqlite */
static test_log *test_suite__dqlite_log = 0;

/* Suite stream for test logging */
static test_log *test_suite__control_log = 0;

static void test_suite__dqlite_output_open(const CU_pSuite suite)
{
	assert(suite != NULL);

	assert(test_suite__dqlite_log == NULL);

	test_suite__dqlite_log = test_log_open();
}

static void test_suite__control_output_open(const CU_pSuite suite)
{
	assert(suite != NULL);

	assert(test_suite__control_log == NULL);

	test_suite__control_log = test_log_open();
}

static void test_suite_dqlite_output_close(const CU_pSuite suite, int flush)
{
	assert(suite != NULL);
	assert(suite->pName != NULL);

	assert(test_suite__dqlite_log != NULL);

	test_log_close(test_suite__dqlite_log);

	if (flush && !test_log_is_empty(test_suite__dqlite_log)) {
		fprintf(stdout,
			"\nSuite %s, Output stream:\n\n%s",
			suite->pName, test_log_output(test_suite__dqlite_log));
	}

	test_log_destroy(test_suite__dqlite_log);

	test_suite__dqlite_log = NULL;
}

static void test_suite_control_output_close(const CU_pSuite suite, int flush)
{
	assert(suite != NULL);
	assert(suite->pName != NULL);

	assert(test_suite__control_log != NULL);

	test_log_close(test_suite__control_log);

	if (flush && !test_log_is_empty(test_suite__control_log)) {
		fprintf(stdout,
			"\nSuite %s, Control stream:\n\n%s",
			suite->pName, test_log_output(test_suite__control_log));
	}

	test_log_destroy(test_suite__control_log);

	test_suite__control_log = NULL;
}

static int test_suite_memory_check(const CU_pSuite suite)
{
	int err;
	int current_malloc;
	int highest_malloc;
	int current_memory;
	int highest_memory;

	err = sqlite3_status(
		SQLITE_STATUS_MALLOC_COUNT, &current_malloc, &highest_malloc, 1);

	if (err != SQLITE_OK) {
		fprintf(stderr,
			"\nSuite %s, Failed to get malloc count: %s\n:",
			suite->pName, sqlite3_errstr(err));
		exit(EXIT_FAILURE);
	}

	err = sqlite3_status(
		SQLITE_STATUS_MEMORY_USED, &current_memory, &highest_memory, 1);

	if (err != SQLITE_OK) {
		fprintf(stderr,
			"\nSuite %s, Failed to get used memory: %s\n:",
			suite->pName, sqlite3_errstr(err));
		exit(EXIT_FAILURE);
	}

	if (current_malloc > 0 || current_memory > 0) {
		fprintf(stdout,
			"\nSuite %s, Unfreed memory:\n    bytes: %11d\n    allocations: %5d\n",
			suite->pName, current_memory, current_malloc);
		return -1;
	}

	return 0;
}

static int test_suite__lifecycle_check(const CU_pSuite suite)
{
	int err;
	char *msg;

	err = dqlite__lifecycle_check(&msg);

	if(err != 0){
		fprintf(stdout,	"\nSuite %s, Lifecycle leak:\n\n%s",
			suite->pName, msg);
		return -1;
	}

	return 0;
}

void test__suite_start_cb(const CU_pSuite suite)
{
	test_suite__dqlite_output_open(suite);
	test_suite__control_output_open(suite);
}

void test__suite_init_failure_cb(const CU_pSuite suite)
{
	assert(suite != NULL);
	assert(suite->pName != NULL);

	fprintf(stdout, "\nSuite %s, Initialization failed\n", suite->pName);
}

void test__suite_complete_cb(const CU_pSuite suite, const CU_pFailureRecord failure)
{
	int err;
	int flush;
	int checks_failed = 0;
	CU_pRunSummary summary = CU_get_run_summary();

	assert(summary != NULL);

	err = test_suite_memory_check(suite);
	if (err != 0)
		checks_failed = 1;

	err = test_suite__lifecycle_check(suite);
	if (err != 0)
		checks_failed = 1;

	if (checks_failed)
		summary->nSuitesFailed++;

	flush = failure || checks_failed;

	test_suite_dqlite_output_close(suite, flush);
	test_suite_control_output_close(suite, flush);
}

FILE *test_suite_dqlite_log()
{
	assert(test_suite__dqlite_log != NULL);

	return test_log_stream(test_suite__dqlite_log);
}

void test_suite_printf(const char *format, ...)
{
	va_list args;
	FILE *stream;

	assert(format != NULL);
	assert(test_suite__control_log != NULL);

	stream = test_log_stream(test_suite__control_log);

	va_start(args, format);

	vfprintf(stream, format, args);
	fprintf(stream, "\n");

	va_end(args);
}

#define TEST_SUITE__ERROR_PREFIX \
	fprintf(stream, "%s:%d: ", basename(__FILE__), __LINE__);

void test_suite_errorf(const char *format, ...)
{
	va_list args;
	FILE *stream;

	assert(format != NULL);
	assert(test_suite__control_log != NULL);

	stream = test_log_stream(test_suite__control_log);

	va_start(args, format);

	TEST_SUITE__ERROR_PREFIX
		vfprintf(stream, format, args);
	fprintf(stream, "\n");

	va_end(args);
}

void test_suite_teardown_pass()
{
	CU_pSuite suite;

	suite = CU_get_current_suite();

	assert(suite != NULL);

	/* If there are no errors, reset the logs */
	if (CU_get_number_of_failures() == 0) {
		test_suite_dqlite_output_close(suite, 0);
		test_suite__dqlite_output_open(suite);
		test_suite_control_output_close(suite, 0);
		test_suite__control_output_open(suite);
	}
}

void test_suite_teardown_fail()
{
	CU_pSuite suite;

	suite = CU_get_current_suite();

	assert(suite != NULL);

	/* Only invoke CU_FAIL if the suite has no failures, otherwise
	 * the test framework drops in an infinite loop. */
	if (suite->uiNumberOfTestsFailed == 0) {
		CU_FAIL("test teardown failed");
	}
}
