#include <sqlite3.h>

#include "../include/dqlite.h"
#include "../src/db.h"

#include "replication.h"

#include "leak.h"
#include "munit.h"

static void *setup(const MunitParameter params[], void *user_data) {
	sqlite3_vfs *            vfs;
	sqlite3_wal_replication *replication;
	struct dqlite__db *      db;
	int                      err;
	int                      rc;

	(void)params;
	(void)user_data;

	/* The replication code relies on mutexes being disabled */
	rc = sqlite3_config(SQLITE_CONFIG_SINGLETHREAD);
	munit_assert_int(rc, ==, SQLITE_OK);

	replication = test_replication();

	err = sqlite3_wal_replication_register(replication, 0);
	munit_assert_int(err, ==, 0);

	err = dqlite_vfs_register(replication->zName, &vfs);
	munit_assert_int(err, ==, 0);

	db = munit_malloc(sizeof *db);

	dqlite__db_init(db);

	return db;
}

static void tear_down(void *data) {
	struct dqlite__db *      db          = data;
	sqlite3_wal_replication *replication = sqlite3_wal_replication_find("test");
	sqlite3_vfs *            vfs         = sqlite3_vfs_find(replication->zName);

	dqlite__db_close(db);

	dqlite_vfs_unregister(vfs);
	sqlite3_wal_replication_unregister(replication);

	test_assert_no_leaks();
}

/* An error is returned if the database does not exists and the
 * SQLITE_OPEN_CREATE flag is not on. */
static MunitResult test_open_cantopen(const MunitParameter params[], void *data) {
	struct dqlite__db *db    = data;
	int                flags = SQLITE_OPEN_READWRITE;
	int                rc;

	(void)params;

	rc = dqlite__db_open(db, "test.db", flags, "test");
	munit_assert_int(rc, ==, SQLITE_CANTOPEN);

	munit_assert_string_equal(db->error, "unable to open database file");

	return MUNIT_OK;
}

/* An error is returned if no VFS is registered under the given
 * name. */
static MunitResult test_open_bad_vfs(const MunitParameter params[], void *data) {
	struct dqlite__db *db    = data;
	int                flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	int                rc;

	(void)params;

	rc = dqlite__db_open(db, "test.db", flags, "foo");
	munit_assert_int(rc, ==, SQLITE_ERROR);

	munit_assert_string_equal(db->error, "no such vfs: foo");

	return MUNIT_OK;
}

/* Open a new database */
static MunitResult test_open(const MunitParameter params[], void *data) {
	struct dqlite__db *db    = data;
	int                flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	int                rc;

	(void)params;

	rc = dqlite__db_open(db, "test.db", flags, "test");
	munit_assert_int(rc, ==, SQLITE_OK);

	return MUNIT_OK;
}

/* If the SQL text is invalid, an error is returned. */
static MunitResult test_prepare_bad_sql(const MunitParameter params[], void *data) {
	struct dqlite__db *  db = data;
	struct dqlite__stmt *stmt;
	int                  flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	int                  rc;

	(void)params;

	rc = dqlite__db_open(db, "test.db", flags, "test");
	munit_assert_int(rc, ==, SQLITE_OK);

	rc = dqlite__db_prepare(db, "FOO bar", &stmt);
	munit_assert_int(rc, ==, SQLITE_ERROR);

	munit_assert_string_equal(db->error, "near \"FOO\": syntax error");

	return MUNIT_OK;
}

static MunitTest dqlite__db_tests[] = {
    {"_open/cantopen", test_open_cantopen, setup, tear_down, 0, NULL},
    {"_open/bad-vfs", test_open_bad_vfs, setup, tear_down, 0, NULL},
    {"_open", test_open, setup, tear_down, 0, NULL},
    {"_prepare/bad-sql", test_prepare_bad_sql, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

MunitSuite dqlite__db_suites[] = {
    {"", dqlite__db_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE},
    {NULL, NULL, NULL, 0, MUNIT_SUITE_OPTION_NONE},
};
