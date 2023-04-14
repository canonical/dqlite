#include <sqlite3.h>

#include "fault.h"
#include "heap.h"

/* This structure is used to encapsulate the global state variables used by
 * malloc() fault simulation. */
struct mem_fault
{
	struct test_fault fault; /* Fault trigger */
	sqlite3_mem_methods m;   /* Actual malloc implementation */
};

/* We need to use a global variable here because after a sqlite3_mem_methods
 * instance has been installed using sqlite3_config(), and after
 * sqlite3_initialize() has been called, there's no way to retrieve it back with
 * sqlite3_config(). */
static struct mem_fault memFault;

/* A version of sqlite3_mem_methods.xMalloc() that includes fault simulation
 * logic.*/
static void *mem_fault_malloc(int n)
{
	void *p = NULL;

	if (!test_fault_tick(&memFault.fault)) {
		p = memFault.m.xMalloc(n);
	}

	return p;
}

/* A version of sqlite3_mem_methods.xRealloc() that includes fault simulation
 * logic. */
static void *mem_fault_realloc(void *old, int n)
{
	void *p = NULL;

	if (!test_fault_tick(&memFault.fault)) {
		p = memFault.m.xRealloc(old, n);
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
static void mem_fault_free(void *p)
{
	memFault.m.xFree(p);
}

static int mem_fault_size(void *p)
{
	return memFault.m.xSize(p);
}

static int mem_fault_roundup(int n)
{
	return memFault.m.xRoundup(n);
}

static int mem_fault_init(void *p)
{
	(void)p;
	return memFault.m.xInit(memFault.m.pAppData);
}

static void mem_fault_shutdown(void *p)
{
	(void)p;
	memFault.m.xShutdown(memFault.m.pAppData);
}

/* Wrap the given SQLite memory management instance with the faulty memory
 * management interface. By default no faults will be triggered. */
static void mem_wrap(sqlite3_mem_methods *m, sqlite3_mem_methods *wrap)
{
	test_fault_init(&memFault.fault);
	memFault.m = *m;

	wrap->xMalloc = mem_fault_malloc;
	wrap->xFree = mem_fault_free;
	wrap->xRealloc = mem_fault_realloc;
	wrap->xSize = mem_fault_size;
	wrap->xRoundup = mem_fault_roundup;
	wrap->xInit = mem_fault_init;
	wrap->xShutdown = mem_fault_shutdown;

	wrap->pAppData = &memFault;
}

/* Unwrap the given faulty memory management instance returning the original
 * one. */
static void mem_unwrap(sqlite3_mem_methods *wrap, sqlite3_mem_methods *m)
{
	(void)wrap;

	*m = memFault.m;
}

/* Get the current number of outstanding malloc()'s without a matching free()
 * and the total number of used memory. */
static void mem_stats(int *malloc_count, int *memory_used)
{
	int rc;
	int watermark;

	rc = sqlite3_status(SQLITE_STATUS_MALLOC_COUNT, malloc_count,
			    &watermark, 1);
	if (rc != SQLITE_OK) {
		munit_errorf("can't get malloc count: %s", sqlite3_errstr(rc));
	}

	rc = sqlite3_status(SQLITE_STATUS_MEMORY_USED, memory_used, &watermark,
			    1);
	if (rc != SQLITE_OK) {
		munit_errorf("can't get memory: %s\n:", sqlite3_errstr(rc));
	}
}

/* Ensure we're starting from a clean memory state with no allocations and
 * optionally inject malloc failures. */
void test_heap_setup(const MunitParameter params[], void *user_data)
{
	int malloc_count;
	int memory_used;
	const char *fault_delay;
	const char *fault_repeat;
	sqlite3_mem_methods mem;
	sqlite3_mem_methods mem_fault;
	int rc;

	(void)params;
	(void)user_data;

	/* Install the faulty malloc implementation */
	rc = sqlite3_config(SQLITE_CONFIG_GETMALLOC, &mem);
	if (rc != SQLITE_OK) {
		munit_errorf("can't get default mem: %s", sqlite3_errstr(rc));
	}

	mem_wrap(&mem, &mem_fault);

	rc = sqlite3_config(SQLITE_CONFIG_MALLOC, &mem_fault);
	if (rc != SQLITE_OK) {
		munit_errorf("can't set faulty mem: %s", sqlite3_errstr(rc));
	}

	/* Check that memory is clean. */
	mem_stats(&malloc_count, &memory_used);
	if (malloc_count > 0 || memory_used > 0) {
		munit_errorf(
		    "setup memory:\n    bytes: %11d\n    allocations: %5d\n",
		    malloc_count, memory_used);
	}

	/* Optionally inject memory allocation failures. */
	fault_delay = munit_parameters_get(params, "mem-fault-delay");
	fault_repeat = munit_parameters_get(params, "mem-fault-repeat");

	munit_assert((fault_delay != NULL && fault_repeat != NULL) ||
		     (fault_delay == NULL && fault_repeat == NULL));

	if (fault_delay != NULL) {
		test_heap_fault_config(atoi(fault_delay), atoi(fault_repeat));
	}
}

/* Ensure we're starting leaving a clean memory behind. */
void test_heap_tear_down(void *data)
{
	sqlite3_mem_methods mem;
	sqlite3_mem_methods mem_fault;
	int rc;

	(void)data;

	int malloc_count;
	int memory_used;

	mem_stats(&malloc_count, &memory_used);
	if (malloc_count > 0 || memory_used > 0) {
		munit_errorf(
		    "teardown memory:\n    bytes: %11d\n    allocations: %5d\n",
		    memory_used, malloc_count);
	}

	/* Restore default memory management. */
	rc = sqlite3_config(SQLITE_CONFIG_GETMALLOC, &mem_fault);
	if (rc != SQLITE_OK) {
		munit_errorf("can't get faulty mem: %s", sqlite3_errstr(rc));
	}

	mem_unwrap(&mem_fault, &mem);

	rc = sqlite3_config(SQLITE_CONFIG_MALLOC, &mem);
	if (rc != SQLITE_OK) {
		munit_errorf("can't reset default mem: %s", sqlite3_errstr(rc));
	}
}

void test_heap_fault_config(int delay, int repeat)
{
	test_fault_config(&memFault.fault, delay, repeat);
}

void test_heap_fault_enable()
{
	test_fault_enable(&memFault.fault);
}
