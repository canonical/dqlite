#include <CUnit/CUnit.h>

#include <sqlite3.h>

#include "../src/db.h"
#include "../include/dqlite.h"

#include "replication.h"
#include "suite.h"

static sqlite3_vfs* vfs;
static struct dqlite__db db;
static sqlite3_wal_replication *replication;

void test_dqlite__db_setup()
{
	int err;

	replication = test_replication();

	err = sqlite3_wal_replication_register(replication, 0);
	if (err != 0) {
		test_suite_errorf("failed to register wal: %s - %d", sqlite3_errstr(err), err);
		CU_FAIL("test setup failed");
	}


	err = dqlite_vfs_register(replication->zName, &vfs);
	if (err != 0) {
		test_suite_errorf("failed to register vfs: %s - %d", sqlite3_errstr(err), err);
		CU_FAIL("test setup failed");
	}

	dqlite__db_init(&db);
}

void test_dqlite__db_teardown()
{
	dqlite__db_close(&db);
	sqlite3_wal_replication_unregister(replication);
	dqlite_vfs_unregister(vfs);
}

void test_dqlite__db_open()
{
	int err;
	int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;

	err = dqlite__db_open(&db, "test.db", flags, replication->zName);
	CU_ASSERT_EQUAL(err, 0);
}
