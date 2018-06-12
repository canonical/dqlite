#include <CUnit/CUnit.h>

#include "../src/db.h"
#include "../src/vfs.h"

#include "suite.h"

static sqlite3_vfs* vfs;
static struct dqlite__db db;

void test_dqlite__db_setup()
{
	int err = dqlite__vfs_register("volatile", &vfs);
	if (err != 0) {
		test_suite_errorf("failed to register vfs: %s - %d", sqlite3_errstr(err), err);
		CU_FAIL("test setup failed");
	}

	dqlite__db_init(&db);
}

void test_dqlite__db_teardown()
{
	dqlite__db_close(&db);
	dqlite__vfs_unregister(vfs);
}

void test_dqlite__db_open()
{
	int err;

	err = dqlite__db_open(&db, "test.db", SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "volatile");
	CU_ASSERT_EQUAL(err, 0);

	err = dqlite__db_abort(&db);
	CU_ASSERT_EQUAL(err, 0);
}
