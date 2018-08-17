#include <stdlib.h>

#include <sqlite3.h>
#include <uv.h>

#include "../src/lifecycle.h"

#include "mem.h"
#include "munit.h"

/******************************************************************************
 *
 * Global SQLite configuration.
 *
 ******************************************************************************/

/* SQLite log function redirecting to munit's log. */
static void test__case_sqlite_log(void *ctx, int rc, const char *errmsg)
{
	(void)ctx;

	munit_logf(MUNIT_LOG_INFO, "SQLite error: %s (%d)", errmsg, rc);
}

/* Ensure that SQLite is unconfigured and set test-specific options. */
static void test__case_config_setup(const MunitParameter params[],
                                    void *               user_data)
{
	int                 rc;
	sqlite3_mem_methods mem;
	sqlite3_mem_methods mem_fault;

	(void)params;
	(void)user_data;

	/* Faulty malloc implementation */
	rc = sqlite3_config(SQLITE_CONFIG_GETMALLOC, &mem);
	if (rc != SQLITE_OK) {
		munit_errorf("can't get default mem: %s", sqlite3_errstr(rc));
	}

	test_mem_fault_wrap(&mem, &mem_fault);

	rc = sqlite3_config(SQLITE_CONFIG_MALLOC, &mem_fault);
	if (rc != SQLITE_OK) {
		munit_errorf("can't set faulty mem: %s", sqlite3_errstr(rc));
	}

	/* Set singlethread mode */
	rc = sqlite3_config(SQLITE_CONFIG_SINGLETHREAD);
	if (rc != SQLITE_OK) {
		munit_errorf("can't set singlethread: %s", sqlite3_errstr(rc));
	}

	/* Redirect logging */
	rc = sqlite3_config(SQLITE_CONFIG_LOG, test__case_sqlite_log);
	if (rc != SQLITE_OK) {
		munit_errorf("can't set log func: %s", sqlite3_errstr(rc));
	}
}

static void test__case_config_tear_down(void *data)
{
	int                 rc;
	sqlite3_mem_methods mem;
	sqlite3_mem_methods mem_fault;

	(void)data;

	rc = sqlite3_shutdown();
	if (rc != SQLITE_OK) {
		munit_errorf("SQLite did not shutdown: %s", sqlite3_errstr(rc));
	}

	/* Reset logging. */
	rc = sqlite3_config(SQLITE_CONFIG_LOG, NULL);
	if (rc != SQLITE_OK) {
		munit_errorf("can't unset log func: %s", sqlite3_errstr(rc));
	}

	/* Restore default memory management. */
	rc = sqlite3_config(SQLITE_CONFIG_GETMALLOC, &mem_fault);
	if (rc != SQLITE_OK) {
		munit_errorf("can't get faulty mem: %s", sqlite3_errstr(rc));
	}

	test_mem_fault_unwrap(&mem_fault, &mem);

	rc = sqlite3_config(SQLITE_CONFIG_MALLOC, &mem);
	if (rc != SQLITE_OK) {
		munit_errorf("can't reset default mem: %s", sqlite3_errstr(rc));
	}
}

/******************************************************************************
 *
 * Global libv configuration.
 *
 ******************************************************************************/

/* Implementation of uv_malloc_func using SQLite's memory allocator. */
static void *test__uv_malloc(size_t size) { return sqlite3_malloc(size); }

/* Implementation of uv_realloc_func using SQLite's memory allocator. */
static void *test__uv_realloc(void *ptr, size_t size)
{
	return sqlite3_realloc(ptr, size);
}

/* Implementation of uv_calloc_func using SQLite's memory allocator. */
static void *test__uv_calloc(size_t nmemb, size_t size)
{
	size_t total_size = nmemb * size;
	void * p          = sqlite3_malloc(total_size);

	memset(p, 0, total_size);

	return p;
}

static void test__case_uv_setup(const MunitParameter params[], void *user_data)
{
	int rv;

	(void)params;
	(void)user_data;

	rv = uv_replace_allocator(
	    test__uv_malloc, test__uv_realloc, test__uv_calloc, sqlite3_free);
	munit_assert_int(rv, ==, 0);
}

static void test__case_uv_tear_down(void *data)
{
	int rv;

	(void)data;

	rv = uv_replace_allocator(malloc, realloc, calloc, free);
	munit_assert_int(rv, ==, 0);
}

/******************************************************************************
 *
 * Memory management.
 *
 ******************************************************************************/

/* Ensure we're starting from a clean memory state with no allocations and
 * optionally inject malloc failures. */
static void test__case_mem_setup(const MunitParameter params[], void *user_data)
{
	int         malloc_count;
	int         memory_used;
	const char *fault_delay;
	const char *fault_repeat;

	(void)params;
	(void)user_data;

	/* Check that memory is clean. */
	test_mem_stats(&malloc_count, &memory_used);
	if (malloc_count > 0 || memory_used > 0) {
		munit_errorf(
		    "setup memory:\n    bytes: %11d\n    allocations: %5d\n",
		    malloc_count,
		    memory_used);
	}

	/* Optionally inject memory allocation failures. */
	fault_delay  = munit_parameters_get(params, "mem-fault-delay");
	fault_repeat = munit_parameters_get(params, "mem-fault-repeat");

	munit_assert((fault_delay != NULL && fault_repeat != NULL) ||
	             (fault_delay == NULL && fault_repeat == NULL));

	if (fault_delay != NULL) {
		test_mem_fault_config(atoi(fault_delay), atoi(fault_repeat));
	}
}

/* Ensure we're starting leaving a clean memory behind. */
static void test__case_mem_tear_down(void *data)
{
	(void)data;

	int malloc_count;
	int memory_used;

	test_mem_stats(&malloc_count, &memory_used);

	if (malloc_count > 0 || memory_used > 0) {
		munit_errorf(
		    "teardown memory:\n    bytes: %11d\n    allocations: %5d\n",
		    malloc_count,
		    memory_used);
	}
}

/******************************************************************************
 *
 * Objects lifecycle.
 *
 ******************************************************************************/

/* Ensure that there are no outstanding initializations. */
static void test__case_lifecycle_setup(const MunitParameter params[],
                                       void *               user_data)
{
	int   rc;
	char *msg;

	(void)params;
	(void)user_data;

	rc = dqlite__lifecycle_check(&msg);
	if (rc != 0) {
		munit_errorf("lifecycle setup leak:\n\n%s", msg);
	}
}

static void test__case_lifecycle_tear_down(void *data)
{
	int   rc;
	char *msg;

	(void)data;

	rc = dqlite__lifecycle_check(&msg);
	if (rc != 0) {
		munit_errorf("lifecycle tear down leak:\n\n%s", msg);
	}
}

/******************************************************************************
 *
 * Common test case setup and tear down.
 *
 ******************************************************************************/

void *test_case_setup(const MunitParameter params[], void *user_data)
{
	test__case_config_setup(params, user_data);
	test__case_mem_setup(params, user_data);
	test__case_lifecycle_setup(params, user_data);
	test__case_uv_setup(params, user_data);

	return NULL;
}

void test_case_tear_down(void *data)
{
	(void)data;

	test__case_uv_tear_down(data);
	test__case_lifecycle_tear_down(data);
	test__case_mem_tear_down(data);
	test__case_config_tear_down(data);
}
