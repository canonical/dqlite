#include <sqlite3.h>

#include "../src/db_.h"

#include "replication.h"

#include "log.h"
#include "./lib/runner.h"

TEST_MODULE(db);

/******************************************************************************
 *
 * Helpers
 *
 ******************************************************************************/

/* Open a test database. */
static void __db_open(struct db_ *db)
{
	int rc;
	int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;

	rc = db__open(db, "test.db", flags, "test", 4096, "test");
	munit_assert_int(rc, ==, SQLITE_OK);
}

/******************************************************************************
 *
 * Setup and tear down
 *
 ******************************************************************************/

dqlite_logger *logger;

static void *setup(const MunitParameter params[], void *user_data)
{
	sqlite3_vfs *vfs;
	sqlite3_wal_replication *replication;
	struct db_ *db;
	int err;
	int rc;

	(void)params;
	(void)user_data;

	/* The replication code relies on mutexes being disabled */
	rc = sqlite3_config(SQLITE_CONFIG_SINGLETHREAD);
	munit_assert_int(rc, ==, SQLITE_OK);

	replication = test_replication();

	err = sqlite3_wal_replication_register(replication, 0);
	munit_assert_int(err, ==, 0);

	logger = test_logger();

	vfs = dqlite_vfs_create(replication->zName, logger);
	munit_assert_ptr_not_null(vfs);

	munit_assert_int(err, ==, 0);
	sqlite3_vfs_register(vfs, 0);

	db = munit_malloc(sizeof *db);

	db__init_(db);

	return db;
}

static void tear_down(void *data)
{
	struct db_ *db = data;
	sqlite3_wal_replication *replication =
	    sqlite3_wal_replication_find("test");
	sqlite3_vfs *vfs = sqlite3_vfs_find(replication->zName);

	db__close_(db);
	free(db);

	sqlite3_vfs_unregister(vfs);
	sqlite3_wal_replication_unregister(replication);

	dqlite_vfs_destroy(vfs);

	free(logger);
}

/******************************************************************************
 *
 * db__open
 *
 ******************************************************************************/

TEST_SUITE(open);
TEST_SETUP(open, setup);
TEST_TEAR_DOWN(open, tear_down);

/* An error is returned if the database does not exists and the
 * SQLITE_OPEN_CREATE flag is not on. */
TEST_CASE(open, cantopen, NULL)
{
	struct db_ *db = data;
	int flags = SQLITE_OPEN_READWRITE;
	int rc;

	(void)params;

	rc = db__open(db, "test.db", flags, "test", 4096, "test");
	munit_assert_int(rc, ==, SQLITE_CANTOPEN);

	munit_assert_string_equal(db->error, "unable to open database file");

	return MUNIT_OK;
}

/* An error is returned if no VFS is registered under the given
 * name. */
TEST_CASE(open, bad_vfs, NULL)
{
	struct db_ *db = data;
	int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	int rc;

	(void)params;

	rc = db__open(db, "test.db", flags, "foo", 4096, "test");
	munit_assert_int(rc, ==, SQLITE_ERROR);

	munit_assert_string_equal(db->error, "no such vfs: foo");

	return MUNIT_OK;
}

/* Open a new database */
TEST_CASE(open, success, NULL)
{
	struct db_ *db = data;
	int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	int rc;

	(void)params;

	rc = db__open(db, "test.db", flags, "test", 4096, "test");
	munit_assert_int(rc, ==, SQLITE_OK);

	return MUNIT_OK;
}

/******************************************************************************
 *
 * db__prepare
 *
 ******************************************************************************/

TEST_SUITE(prepare);
TEST_SETUP(prepare, setup);
TEST_TEAR_DOWN(prepare, tear_down);

/* If the SQL text is invalid, an error is returned. */
TEST_CASE(prepare, bad_sql, NULL)
{
	struct db_ *db = data;
	struct stmt *stmt;
	int rc;

	(void)params;

	__db_open(db);

	rc = db__prepare(db, "FOO bar", &stmt);
	munit_assert_int(rc, ==, SQLITE_ERROR);

	munit_assert_string_equal(db->error, "near \"FOO\": syntax error");

	return MUNIT_OK;
}
