#ifndef DQLITE_TEST_HEAP_H
#define DQLITE_TEST_HEAP_H

#include "munit.h"

/* Munit parameter defining the delay of the faulty memory implementation. */
#define TEST_HEAP_FAULT_DELAY "mem-fault-delay"

/* Munit parameter defining the repeat of the faulty memory implementation. */
#define TEST_HEAP_FAULT_REPEAT "mem-fault-repeat"

void test_heap_setup(const MunitParameter params[], void *user_data);
void test_heap_tear_down(void *data);

/* Configure the faulty memory management implementation so malloc()-related
 * functions start returning NULL pointers after 'delay' calls, and keep failing
 * for 'repeat' consecutive times.
 *
 * Note that the faults won't automatically take place, an explicit call to
 * test_mem_fault_enable() is needed. This allows configuration and actual
 * behavior to happen at different times (e.g. configure at test setup time and
 * enable at test case time). */
void test_heap_fault_config(int delay, int repeat);

/* Enable the faulty behavior, which from this point on will honor the
 * parameters passed to test_mem_fault_config(). */
void test_heap_fault_enable();

#define SETUP_HEAP test_heap_setup(params, user_data);
#define TEAR_DOWN_HEAP test_heap_tear_down(data);

#endif /* DQLITE_TEST_HEAP_H */
