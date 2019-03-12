/**
 * Setup a test database.
 */

#ifndef TEST_DB_H
#define TEST_DB_H

#include <sqlite3.h>

/**
 * The DB parameter is the fixture field name of the database object.
 */
#define FIXTURE_DB(DB) sqlite3 *DB;

#define SETUP_DB(DB)                                                    \
	{                                                               \
		int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE; \
		int rc;                                                 \
		rc = sqlite3_open_v2("test.db", &f->DB, flags, "test"); \
		munit_assert_int(rc, ==, SQLITE_OK);                    \
		DB_EXEC(DB, "PRAGMA page_size=512", 0);                 \
		DB_EXEC(DB, "PRAGMA synchronous=OFF", 0);               \
		DB_EXEC(DB, "PRAGMA journal_mode=WAL", 0);              \
	}

#define TEAR_DOWN_DB(DB)                             \
	{                                            \
		int rc;                              \
		rc = sqlite3_close(f->DB);           \
		munit_assert_int(rc, ==, SQLITE_OK); \
	}

/**
 * Execute the SQL text on DB and check that RC is returned.
 */
#define DB_EXEC(DB, SQL, RC)                                     \
	{                                                        \
		int rc;                                          \
		rc = sqlite3_exec(f->DB, SQL, NULL, NULL, NULL); \
		munit_assert_int(rc, ==, RC);                    \
	}

#endif /* TEST_DB_H */
