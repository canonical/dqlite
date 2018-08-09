#include <sqlite3.h>

#include "../src/lifecycle.h"

#include "munit.h"

static void test__assert_no_memory_leaks() {
	int err;
	int current_malloc;
	int highest_malloc;
	int current_memory;
	int highest_memory;

	err = sqlite3_status(
	    SQLITE_STATUS_MALLOC_COUNT, &current_malloc, &highest_malloc, 1);

	if (err != SQLITE_OK) {
		munit_errorf("failed to get malloc count: %s",
		             sqlite3_errstr(err));
	}

	err = sqlite3_status(
	    SQLITE_STATUS_MEMORY_USED, &current_memory, &highest_memory, 1);

	if (err != SQLITE_OK) {
		munit_errorf("failed to get used memory: %s\n:",
		             sqlite3_errstr(err));
	}

	if (current_malloc > 0 || current_memory > 0) {
		munit_errorf(
		    "unfreed memory:\n    bytes: %11d\n    allocations: %5d\n",
		    current_memory,
		    current_malloc);
	}

	err = sqlite3_shutdown();
	munit_assert_int(err, ==, 0);
}

static void test__assert_no_lifecycle_leak() {
	int   err;
	char *msg = NULL;

	err = dqlite__lifecycle_check(&msg);

	if (err != 0) {
		munit_errorf("lifecycle leak:\n\n%s", msg);
	}
}

void test_assert_no_leaks() {
	test__assert_no_memory_leaks();
	test__assert_no_lifecycle_leak();
}
