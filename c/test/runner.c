#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include <CUnit/CUnit.h>

#include "suite.h"
#include "test_conn.h"
#include "test_dqlite.h"
#include "test_error.h"
#include "test_gateway.h"
#include "test_queue.h"
#include "test_request.h"
#include "test_response.h"

/* All suites to run */
static CU_SuiteInfo* test__runner_suites[] = {
	dqlite__conn_suites,
	dqlite__error_suites,
	dqlite__gateway_suites,
	dqlite__queue_suites,
	dqlite__request_suites,
	dqlite__response_suites,
	dqlite_suites,
	0,
};

/* Called when a single test has completed */
static void test__runner_test_complete_cb(const CU_pTest test,
					const CU_pSuite suite,
					const CU_pFailureRecord failures)
{
	CU_pFailureRecord failure;

	assert( suite );
	assert( test );

	/* If there are no failures, just move on */
	if (failures == NULL) {
		return;
	}

	assert(suite->pName != NULL);
	assert(test->pName != NULL );

	/* Print all failures for this test */
	fprintf(stdout, "\nSuite '%s', Test '%s' had failures:\n",
		suite->pName, test->pName);

	for (failure = failures; failure != NULL; failure = failures->pNext) {

		assert(failure->strFileName != NULL);
		assert(failure->strCondition != NULL);

		fprintf(stdout, "\n%s:%d: %s",
			failure->strFileName,
			failure->uiLineNumber,
			failure->strCondition);
	}

	fprintf(stdout, "\n");
}

/* Called when all tests have been run */
static void test__runner_all_test_complete_cb(const CU_pFailureRecord failure)
{
	fprintf(stdout, "\n");
	CU_print_run_results(stdout);
	fprintf(stdout, "\n");
}

/* Test runner executable */
int main(){
	int exit_code = EXIT_SUCCESS;
	int err = 0;
	CU_SuiteInfo **suite;

	/* Unbuffered output so everything reaches the screen */
	setvbuf(stdout, NULL, _IONBF, 0);
	setvbuf(stderr, NULL, _IONBF, 0);

	/* Initialize test registry */
	err = CU_initialize_registry();
	if(err != 0){
		fprintf(stderr, "\nFailed to initialize Test Registry: %s\n",
			CU_get_error_msg());
		return EXIT_FAILURE;
	}

	assert(CU_get_registry() != NULL);
	assert(!CU_is_test_running());

	/* Register callbacks */
	CU_set_suite_start_handler(test__suite_start_cb);
	CU_set_suite_init_failure_handler(test__suite_init_failure_cb);
	CU_set_test_complete_handler(test__runner_test_complete_cb);
	CU_set_suite_complete_handler(test__suite_complete_cb);
	CU_set_all_test_complete_handler(test__runner_all_test_complete_cb);

	/* Register suites. */
	for (suite = test__runner_suites; *suite != NULL; suite++) {
		err = CU_register_suites(*suite);
		if(err != 0) {
			fprintf(stderr, "\nFailed to register suite: %s\n",
				CU_get_error_msg());
			return EXIT_FAILURE;
		}
	}

	/* Run tests. */
	err = CU_run_all_tests();
	if(err != 0){
		fprintf(stderr, "\nFailed to run tests: %s\n",
			CU_get_error_msg());
		exit_code = EXIT_FAILURE;
	}

	/* Inspect failures */
	if (CU_get_number_of_tests_failed() != 0)
		exit_code = EXIT_FAILURE;

	if (CU_get_number_of_suites_failed() !=0)
		exit_code = EXIT_FAILURE;

	CU_cleanup_registry();

	if (exit_code == EXIT_SUCCESS)
		fprintf(stderr, "\nPASSED\n");
	else
		fprintf(stderr, "\nFAILED\n");

	fprintf(stdout, "\n");

	return exit_code;
}
