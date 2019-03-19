/**
 * Setup a test prepared statement.
 */

#ifndef TEST_STMT_H
#define TEST_STMT_H

#include <sqlite3.h>

#define FIXTURE_STMT sqlite3_stmt *stmt

#define STMT_PREPARE(CONN, STMT, SQL)                                \
	{                                                            \
		int rc;                                              \
		rc = sqlite3_prepare_v2(CONN, SQL, -1, &STMT, NULL); \
		munit_assert_int(rc, ==, 0);                         \
	}

#define STMT_FINALIZE(STMT) sqlite3_finalize(STMT)

#define STMT_EXEC(CONN, SQL)                                    \
	{                                                       \
		int rc;                                         \
		char *msg;                                      \
		rc = sqlite3_exec(CONN, SQL, NULL, NULL, &msg); \
		munit_assert_int(rc, ==, SQLITE_OK);            \
	}

#endif /* TEST_STMT_H */
