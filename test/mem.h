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
 * for 'repeat' consecutive times. */
void test_mem_fault_config(int delay, int repeat);

#endif /* DQLITE_TEST_MEM_H */
