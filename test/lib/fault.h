/**
 * Helper for test components supporting fault injection.
 */

#ifndef TEST_FAULT_H
#define TEST_FAULT_H

#include <stdbool.h>

/**
 * Information about a fault that should occurr in a component.
 */
struct test_fault
{
	int countdown; /* Trigger the fault when this counter gets to zero. */
	int n;         /* Repeat the fault this many times. Default is -1. */
	bool enabled;  /* Enable fault triggering. */
};

/**
 * Initialize a fault.
 */
void test_fault_init(struct test_fault *f);

/**
 * Advance the counters of the fault. Return true if the fault should be
 * triggered, false otherwise.
 */
bool test_fault_tick(struct test_fault *f);

/**
 * Configure the fault with the given values.
 */
void test_fault_config(struct test_fault *f, int delay, int repeat);

/**
 * Enable fault triggering.
 */
void test_fault_enable(struct test_fault *f);

#endif /* TEST_FAULT_H */
