#ifndef DQLITE_TEST_REPLICATION_H
#define DQLITE_TEST_REPLICATION_H

#include <sqlite3.h>

sqlite3_wal_replication *test_replication();

#define FIXTURE_STUB_REPLICATION sqlite3_wal_replication replication;

#define SETUP_STUB_REPLICATION                \
	f->replication = *test_replication(); \
	sqlite3_wal_replication_register(&f->replication, 0)

#define TEAR_DOWN_STUB_REPLICATION \
	sqlite3_wal_replication_unregister(&f->replication);

#endif /* DQLITE_TEST_REPLICATION_H */
