/**
 * Setup a test prepared statement.
 */

#ifndef TEST_STMT_H
#define TEST_STMT_H

#include <sqlite3.h>

#define FIXTURE_STMT sqlite3_stmt *stmt

#define SETUP_STMT                                                       \
	{                                                                \
		int rc;                                                  \
		rc = sqlite3_prepare_v2(f->leader.conn,                  \
					"CREATE TABLE test (a INT)", -1, \
					&f->stmt, NULL);                 \
		munit_assert_int(rc, ==, 0);                             \
	}

#define TEAR_DOWN_STMT sqlite3_finalize(f->stmt)

#endif /* TEST_STMT_H */
