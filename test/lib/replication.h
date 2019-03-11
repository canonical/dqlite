/**
 * Setup a sqlite3_wal_replication instance using dqlite's engine.
 */

#ifndef TEST_REPLICATION_H
#define TEST_REPLICATION_H

#define REPLICATION_FIXTURE sqlite3_wal_replication replication;

#define REPLICATION_SETUP                                                      \
	{                                                                      \
		int rc;                                                        \
		rc = replication__init(&f->replication, &f->logger, &f->raft); \
		f->replication.zName = "test";                                 \
		munit_assert_int(rc, ==, 0);                                   \
		sqlite3_wal_replication_register(&f->replication, 0);          \
	}

#define REPLICATION_TEAR_DOWN                                \
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
