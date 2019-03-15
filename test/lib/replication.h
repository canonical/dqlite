/**
 * Setup a sqlite3_wal_replication instance using dqlite's engine.
 */

#ifndef TEST_REPLICATION_H
#define TEST_REPLICATION_H

#include "../../src/replication.h"

#define FIXTURE_REPLICATION sqlite3_wal_replication replication;
#define SETUP_REPLICATION SETUP_REPLICATION_X(f, "test")
#define TEAR_DOWN_REPLICATION TEAR_DOWN_REPLICATION_X(f)

#define SETUP_REPLICATION_X(F, NAME)                                           \
	{                                                                      \
		int rc;                                                        \
		rc = replication__init(&F->replication, &F->logger, &F->raft); \
		F->replication.zName = NAME;                                   \
		munit_assert_int(rc, ==, 0);                                   \
		sqlite3_wal_replication_register(&F->replication, 0);          \
	}

#define TEAR_DOWN_REPLICATION_X(F)                           \
	sqlite3_wal_replication_unregister(&F->replication); \
	replication__close(&F->replication);

#endif /* TEST_REPLICATION_H */
