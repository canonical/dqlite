#include <stdint.h>

#include <sqlite3.h>

#include "mem.h"
#include "munit.h"

void test_mem_stats(int *malloc_count, int *memory_used)
{
	int rc;
	int watermark;

	rc = sqlite3_status(
	    SQLITE_STATUS_MALLOC_COUNT, malloc_count, &watermark, 1);
	if (rc != SQLITE_OK) {
		munit_errorf("can't get malloc count: %s", sqlite3_errstr(rc));
	}

	rc = sqlite3_status(
	    SQLITE_STATUS_MEMORY_USED, memory_used, &watermark, 1);
	if (rc != SQLITE_OK) {
		munit_errorf("can't get memory: %s\n:", sqlite3_errstr(rc));
	}
}

/* This structure is used to encapsulate the global state variables used by
 * malloc() fault simulation. */
struct test__mem_fault {
	int     countdown; /* Number of pending successes before a failure */
	int     repeat;    /* Number of times to repeat the failure */
	int     fail;      /* Number of failures seen since last config */
	uint8_t enabled;   /* True if enabled */
	sqlite3_mem_methods m; /* Actual malloc implementation */
};

/* Check to see if a fault should be simulated. Return true to simulate the
 * fault.  Return false if the fault should not be simulated. */
static int test__mem_fault_step(struct test__mem_fault *f)
{
	if (MUNIT_LIKELY(!f->enabled)) {
		return 0;
	}
	if (f->countdown > 0) {
		f->countdown--;
		return 0;
	}
	f->fail++;
	f->repeat--;
	if (f->repeat == 0) {
		f->enabled = 0;
	}

	return 1;
}

/* We need to use a global variable here because after a sqlite3_mem_methods
 * instance has been installed using sqlite3_config(), and after
 * sqlite3_initialize() has been called, there's no way to retrieve it back with
 * sqlite3_config(). */
static struct test__mem_fault __mem_fault;

/* A version of sqlite3_mem_methods.xMalloc() that includes fault simulation
 * logic.*/
static void *test__mem_fault_malloc(int n)
{
	void *p = NULL;

	if (!test__mem_fault_step(&__mem_fault)) {
		p = __mem_fault.m.xMalloc(n);
	}

	return p;
}

/* A version of sqlite3_mem_methods.xRealloc() that includes fault simulation
 * logic. */
static void *test__mem_fault_realloc(void *old, int n)
{
	void *p = NULL;

	if (!test__mem_fault_step(&__mem_fault)) {
		p = __mem_fault.m.xRealloc(old, n);
	}

	return p;
}

/* The following method calls are passed directly through to the underlying
 * malloc system:
 *
 *     xFree
 *     xSize
 *     xRoundup
 *     xInit
 *     xShutdown
 */
static void test__mem_fault_free(void *p) { __mem_fault.m.xFree(p); }

static int test__mem_fault_size(void *p) { return __mem_fault.m.xSize(p); }

static int test__mem_fault_roundup(int n) { return __mem_fault.m.xRoundup(n); }

static int test__mem_fault_init(void *p)
{
	(void)p;
	return __mem_fault.m.xInit(__mem_fault.m.pAppData);
}

static void test__mem_fault_shutdown(void *p)
{
	(void)p;
	__mem_fault.m.xShutdown(__mem_fault.m.pAppData);
}

void test_mem_fault_wrap(sqlite3_mem_methods *m, sqlite3_mem_methods *wrap)
{
	__mem_fault.countdown = 0;
	__mem_fault.repeat    = 0;
	__mem_fault.fail      = 0;
	__mem_fault.enabled   = 0;
	__mem_fault.m         = *m;

	wrap->xMalloc   = test__mem_fault_malloc;
	wrap->xFree     = test__mem_fault_free;
	wrap->xRealloc  = test__mem_fault_realloc;
	wrap->xSize     = test__mem_fault_size;
	wrap->xRoundup  = test__mem_fault_roundup;
	wrap->xInit     = test__mem_fault_init;
	wrap->xShutdown = test__mem_fault_shutdown;

	wrap->pAppData = &__mem_fault;
}

void test_mem_fault_unwrap(sqlite3_mem_methods *wrap, sqlite3_mem_methods *m)
{
	(void)wrap;

	*m = __mem_fault.m;
}

void test_mem_fault_config(int delay, int repeat)
{
	if (__mem_fault.enabled) {
		munit_error("memory management failures already configured");
	}
	__mem_fault.countdown = delay;
	__mem_fault.repeat    = repeat;
}

void test_mem_fault_enable() { __mem_fault.enabled = 1; }
