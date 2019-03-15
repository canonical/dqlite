/**
 * Setup a test prepared statement.
 */

#ifndef TEST_STMT_H
#define TEST_STMT_H

#include <sqlite3.h>

#define FIXTURE_STMT sqlite3_stmt *stmt
#define SETUP_STMT SETUP_STMT_X(f)
#define TEAR_DOWN_STMT TEAR_DOWN_STMT_X(f)

#define SETUP_STMT_X(F)                                                  \
	{                                                                \
		int rc;                                                  \
		rc = sqlite3_prepare_v2(F->leader.conn,                  \
					"CREATE TABLE test (a INT)", -1, \
					&F->stmt, NULL);                 \
		munit_assert_int(rc, ==, 0);                             \
	}

#define TEAR_DOWN_STMT_X(F) sqlite3_finalize(F->stmt)

#endif /* TEST_STMT_H */
