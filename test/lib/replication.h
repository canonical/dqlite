/**
 * Setup a sqlite3_wal_replication instance using dqlite's engine.
 */

#ifndef TEST_REPLICATION_H
#define TEST_REPLICATION_H

#include "../../src/replication.h"

#define FIXTURE_REPLICATION sqlite3_wal_replication replication;

#define SETUP_REPLICATION                                                      \
	{                                                                      \
		int rc;                                                        \
		rc = replication__init(&f->replication, &f->logger, &f->raft); \
		f->replication.zName = "test";                                 \
		munit_assert_int(rc, ==, 0);                                   \
		sqlite3_wal_replication_register(&f->replication, 0);          \
	}

#define TEAR_DOWN_REPLICATION                                \
	sqlite3_wal_replication_unregister(&f->replication); \
	replication__close(&f->replication);

/**
 * Setup the given database for leader replication.
 */
#define REPLICATION_LEADER(DB)                                             \
	{                                                                  \
		int rc;                                                    \
		rc = sqlite3_wal_replication_leader(f->DB, "main", "test", \
						    NULL);                 \
		munit_assert_int(rc, ==, 0);                               \
	}

#endif /* TEST_REPLICATION_H */
