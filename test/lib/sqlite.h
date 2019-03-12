/**
 * Global SQLite configuration.
 */

#ifndef DQLITE_TEST_SQLITE_H
#define DQLITE_TEST_SQLITE_H

#include "munit.h"

/**
 * Setup SQLite global state.
 */
void test_sqlite_setup(const MunitParameter params[]);

/**
 * Teardown SQLite global state.
 */
void test_sqlite_tear_down();

#define SETUP_SQLITE test_sqlite_setup(params)
#define TEAR_DOWN_SQLITE test_sqlite_tear_down()

#endif /* DQLITE_TEST_SQLITE_H */
