#ifndef DQLITE_TEST_MEM_H
#define DQLITE_TEST_MEM_H

#include <sqlite3.h>

#include "munit.h"

/* Munit parameter defining the delay of the faulty memory implementation. */
#define TEST_MEM_FAULT_DELAY_PARAM "mem-fault-delay"

/* Munit parameter defining the repeat of the faulty memory implementation. */
#define TEST_MEM_FAULT_REPEAT_PARAM "mem-fault-repeat"

/* Get the current number of outstanding malloc()'s without a matching free()
 * and the total number of used memory. */
void test_mem_stats(int *malloc_count, int *memory_used);

/* Wrap the given SQLite memory management instance with the faulty memory
 * management interface. By default no faults will be triggered. */
void test_mem_fault_wrap(sqlite3_mem_methods *m, sqlite3_mem_methods *wrap);

/* Unwrap the given faulty memory management instance returning the original
 * one. */
void test_mem_fault_unwrap(sqlite3_mem_methods *wrap, sqlite3_mem_methods *m);

/* Configure the faulty memory management implementation so malloc()-related
 * functions start returning NULL pointers after 'delay' calls, and keep failing
 * for 'repeat' consecutive times.
 *
 * Note that the faults won't automatically take place, an explicit call to
 * test_mem_fault_enable() is needed. This allows configuration and actual
 * behavior to happen at different times (e.g. configure at test setup time and
 * enable at test case time). */
void test_mem_fault_config(int delay, int repeat);

/* Enable the faulty behavior, which from this point on will honor the
 * parameters passed to test_mem_fault_config(). */
void test_mem_fault_enable();

#endif /* DQLITE_TEST_MEM_H */
