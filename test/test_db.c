#include <sqlite3.h>

#include "../include/dqlite.h"
#include "../src/db.h"

#include "replication.h"

#include "leak.h"
#include "log.h"
#include "munit.h"

/******************************************************************************
 *
 * Helpers
 *
 ******************************************************************************/

/* Open a test database. */
static void __db_open(struct dqlite__db *db)
{
	int rc;
	int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;

	rc = dqlite__db_open(db, "test.db", flags, "test", 4096, "test");
	munit_assert_int(rc, ==, SQLITE_OK);
}

/******************************************************************************
 *
 * Setup and tear down
 *
 ******************************************************************************/

static void *setup(const MunitParameter params[], void *user_data)
{
	dqlite_logger *          logger = test_logger();
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

	vfs = dqlite_vfs_create(replication->zName, logger);
	munit_assert_ptr_not_null(vfs);

	munit_assert_int(err, ==, 0);
	sqlite3_vfs_register(vfs, 0);

	db = munit_malloc(sizeof *db);

	dqlite__db_init(db);

	return db;
}

static void tear_down(void *data)
{
	struct dqlite__db *      db = data;
	sqlite3_wal_replication *replication =
	    sqlite3_wal_replication_find("test");
	sqlite3_vfs *vfs = sqlite3_vfs_find(replication->zName);

	dqlite__db_close(db);

	sqlite3_vfs_unregister(vfs);
	sqlite3_wal_replication_unregister(replication);

	dqlite_vfs_destroy(vfs);

	test_assert_no_leaks();
}

/******************************************************************************
 *
 * dqlite__db_open
 *
 ******************************************************************************/

/* An error is returned if the database does not exists and the
 * SQLITE_OPEN_CREATE flag is not on. */
static MunitResult test_open_cantopen(const MunitParameter params[], void *data)
{
	struct dqlite__db *db    = data;
	int                flags = SQLITE_OPEN_READWRITE;
	int                rc;

	(void)params;

	rc = dqlite__db_open(db, "test.db", flags, "test", 4096, "test");
	munit_assert_int(rc, ==, SQLITE_CANTOPEN);

	munit_assert_string_equal(db->error, "unable to open database file");

	return MUNIT_OK;
}

/* An error is returned if no VFS is registered under the given
 * name. */
static MunitResult test_open_bad_vfs(const MunitParameter params[], void *data)
{
	struct dqlite__db *db    = data;
	int                flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	int                rc;

	(void)params;

	rc = dqlite__db_open(db, "test.db", flags, "foo", 4096, "test");
	munit_assert_int(rc, ==, SQLITE_ERROR);

	munit_assert_string_equal(db->error, "no such vfs: foo");

	return MUNIT_OK;
}

/* Open a new database */
static MunitResult test_open(const MunitParameter params[], void *data)
{
	struct dqlite__db *db    = data;
	int                flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	int                rc;

	(void)params;

	rc = dqlite__db_open(db, "test.db", flags, "test", 4096, "test");
	munit_assert_int(rc, ==, SQLITE_OK);

	return MUNIT_OK;
}

static MunitTest dqlite__open_tests[] = {
    {"/cantopen", test_open_cantopen, setup, tear_down, 0, NULL},
    {"/bad-vfs", test_open_bad_vfs, setup, tear_down, 0, NULL},
    {"", test_open, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * dqlite__db_prepare
 *
 ******************************************************************************/

/* If the SQL text is invalid, an error is returned. */
static MunitResult test_prepare_bad_sql(const MunitParameter params[],
                                        void *               data)
{
	struct dqlite__db *  db = data;
	struct dqlite__stmt *stmt;
	int                  rc;

	(void)params;

	__db_open(db);

	rc = dqlite__db_prepare(db, "FOO bar", &stmt);
	munit_assert_int(rc, ==, SQLITE_ERROR);

	munit_assert_string_equal(db->error, "near \"FOO\": syntax error");

	return MUNIT_OK;
}

static MunitTest dqlite__prepare_tests[] = {
    {"/bad-sql", test_prepare_bad_sql, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * dqlite__db_begin
 *
 ******************************************************************************/

/* If the transaction fails to begin, the in_a_tx flag is not switched on. */
static MunitResult test_begin_error(const MunitParameter params[], void *data)
{
	struct dqlite__db *db = data;
	char *             msg;
	int                rc;

	(void)params;

	__db_open(db);

	/* Start a transaction by hand to so the call to dqlite__db_begin will
	 * fail. */
	rc = sqlite3_exec(db->db, "BEGIN", NULL, NULL, &msg);
	munit_assert_int(rc, ==, SQLITE_OK);

	rc = dqlite__db_begin(db);
	munit_assert_int(rc, ==, SQLITE_ERROR);

	munit_assert_string_equal(
	    db->error, "cannot start a transaction within a transaction");

	return MUNIT_OK;
}

/* The in_a_tx flag gets switched on after a transaction is successfully
 * started. */
static MunitResult test_begin(const MunitParameter params[], void *data)
{
	struct dqlite__db *db = data;
	int                rc;

	(void)params;

	__db_open(db);

	rc = dqlite__db_begin(db);
	munit_assert_int(rc, ==, SQLITE_OK);

	return MUNIT_OK;
}

static MunitTest dqlite__begin_tests[] = {
    {"/error", test_begin_error, setup, tear_down, 0, NULL},
    {"", test_begin, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * dqlite__db_commit
 *
 ******************************************************************************/

/* If the transaction fails to commit, the in_a_tx flag is still switched off */
static MunitResult test_commit_error(const MunitParameter params[], void *data)
{
	struct dqlite__db *  db = data;
	struct dqlite__stmt *stmt;
	char *               msg;
	int                  rc;
	uint64_t             last_insert_id;
	uint64_t             rows_affected;

	(void)params;

	__db_open(db);

	/* Create two test tables, one with a foreign reference to the other. */
	rc = sqlite3_exec(db->db,
	                  "CREATE TABLE test1 (n INT, UNIQUE(n)); "
	                  "CREATE TABLE test2 (n INT,"
	                  "    FOREIGN KEY (n) REFERENCES test1 (n) "
	                  "    DEFERRABLE INITIALLY DEFERRED);",
	                  NULL,
	                  NULL,
	                  &msg);
	munit_assert_int(rc, ==, SQLITE_OK);

	/* Begin a transaction */
	rc = dqlite__db_begin(db);
	munit_assert_int(rc, ==, SQLITE_OK);

	/* Insert a broken foreign key. This won't fail immediately because the
	 * fk check is deferred. */
	rc = dqlite__db_prepare(db, "INSERT INTO test2(n) VALUES(1)", &stmt);
	munit_assert_int(rc, ==, SQLITE_OK);

	rc = dqlite__stmt_exec(stmt, &last_insert_id, &rows_affected);
	munit_assert_int(rc, ==, SQLITE_OK);

	/* Attempt to commit the transaction. */
	rc = dqlite__db_commit(db);
	munit_assert_int(rc, ==, SQLITE_CONSTRAINT_FOREIGNKEY);

	/* Rollback. */
	rc = dqlite__db_rollback(db);
	munit_assert_int(rc, ==, SQLITE_OK);

	/* A new transaction can begin. */
	rc = dqlite__db_begin(db);
	munit_assert_int(rc, ==, SQLITE_OK);

	return MUNIT_OK;
}

/* Successful commit. */
static MunitResult test_commit(const MunitParameter params[], void *data)
{
	struct dqlite__db *      db = data;
	struct dqlite__vfs_file *file;
	int                      rc;

	(void)params;

	__db_open(db);

	rc = dqlite__db_begin(db);
	munit_assert_int(rc, ==, SQLITE_OK);

	rc = dqlite__db_commit(db);
	munit_assert_int(rc, ==, SQLITE_OK);

	/* The transaction refcount has dropped to 0 */
	rc = sqlite3_file_control(
	    db->db, "main", SQLITE_FCNTL_FILE_POINTER, &file);
	munit_assert_int(rc, ==, SQLITE_OK);

	return MUNIT_OK;
}

static MunitTest dqlite__commit_tests[] = {
    {"/error", test_commit_error, setup, tear_down, 0, NULL},
    {"", test_commit, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * Suite
 *
 ******************************************************************************/

MunitSuite dqlite__db_suites[] = {
    {"_open", dqlite__open_tests, NULL, 1, 0},
    {"_prepare", dqlite__prepare_tests, NULL, 1, 0},
    {"_begin", dqlite__begin_tests, NULL, 1, 0},
    {"_commit", dqlite__commit_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
