#include <sqlite3.h>

#include "../src/db.h"

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
static void __db_open(struct db *db)
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
	struct db *db;
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

	db__init(db);

	return db;
}

static void tear_down(void *data)
{
	struct db *db = data;
	sqlite3_wal_replication *replication =
	    sqlite3_wal_replication_find("test");
	sqlite3_vfs *vfs = sqlite3_vfs_find(replication->zName);

	db__close(db);
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
	struct db *db = data;
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
	struct db *db = data;
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
	struct db *db = data;
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
	struct db *db = data;
	struct stmt *stmt;
	int rc;

	(void)params;

	__db_open(db);

	rc = db__prepare(db, "FOO bar", &stmt);
	munit_assert_int(rc, ==, SQLITE_ERROR);

	munit_assert_string_equal(db->error, "near \"FOO\": syntax error");

	return MUNIT_OK;
}

/******************************************************************************
 *
 * db__begin
 *
 ******************************************************************************/

TEST_SUITE(begin);
TEST_SETUP(begin, setup);
TEST_TEAR_DOWN(begin, tear_down);

/* If the transaction fails to begin, the in_a_tx flag is not switched on. */
TEST_CASE(begin, error, NULL)
{
	struct db *db = data;
	char *msg;
	int rc;

	(void)params;

	__db_open(db);

	/* Start a transaction by hand to so the call to db__begin will
	 * fail. */
	rc = sqlite3_exec(db->db, "BEGIN", NULL, NULL, &msg);
	munit_assert_int(rc, ==, SQLITE_OK);

	rc = db__begin(db);
	munit_assert_int(rc, ==, SQLITE_ERROR);

	munit_assert_string_equal(
	    db->error, "cannot start a transaction within a transaction");

	return MUNIT_OK;
}

/* The in_a_tx flag gets switched on after a transaction is successfully
 * started. */
TEST_CASE(begin, success, NULL)
{
	struct db *db = data;
	int rc;

	(void)params;

	__db_open(db);

	rc = db__begin(db);
	munit_assert_int(rc, ==, SQLITE_OK);

	return MUNIT_OK;
}

/******************************************************************************
 *
 * db__commit
 *
 ******************************************************************************/

TEST_SUITE(commit);
TEST_SETUP(commit, setup);
TEST_TEAR_DOWN(commit, tear_down);

/* If the transaction fails to commit, the in_a_tx flag is still switched off */
TEST_CASE(commit, error, NULL)
{
	struct db *db = data;
	struct stmt *stmt;
	char *msg;
	int rc;
	uint64_t last_insert_id;
	uint64_t rows_affected;

	(void)params;

	__db_open(db);

	/* Create two test tables, one with a foreign reference to the other. */
	rc = sqlite3_exec(db->db,
			  "CREATE TABLE test1 (n INT, UNIQUE(n)); "
			  "CREATE TABLE test2 (n INT,"
			  "    FOREIGN KEY (n) REFERENCES test1 (n) "
			  "    DEFERRABLE INITIALLY DEFERRED);",
			  NULL, NULL, &msg);
	munit_assert_int(rc, ==, SQLITE_OK);

	/* Begin a transaction */
	rc = db__begin(db);
	munit_assert_int(rc, ==, SQLITE_OK);

	/* Insert a broken foreign key. This won't fail immediately because the
	 * fk check is deferred. */
	rc = db__prepare(db, "INSERT INTO test2(n) VALUES(1)", &stmt);
	munit_assert_int(rc, ==, SQLITE_OK);

	rc = stmt__exec(stmt, &last_insert_id, &rows_affected);
	munit_assert_int(rc, ==, SQLITE_OK);

	/* Attempt to commit the transaction. */
	rc = db__commit(db);
	munit_assert_int(rc, ==, SQLITE_CONSTRAINT_FOREIGNKEY);

	/* Rollback. */
	rc = db__rollback(db);
	munit_assert_int(rc, ==, SQLITE_OK);

	/* A new transaction can begin. */
	rc = db__begin(db);
	munit_assert_int(rc, ==, SQLITE_OK);

	return MUNIT_OK;
}

/* Successful commit. */
TEST_CASE(commit, success, NULL)
{
	struct db *db = data;
	struct dqlite__vfs_file *file;
	int rc;

	(void)params;

	__db_open(db);

	rc = db__begin(db);
	munit_assert_int(rc, ==, SQLITE_OK);

	rc = db__commit(db);
	munit_assert_int(rc, ==, SQLITE_OK);

	/* The transaction refcount has dropped to 0 */
	rc = sqlite3_file_control(db->db, "main", SQLITE_FCNTL_FILE_POINTER,
				  &file);
	munit_assert_int(rc, ==, SQLITE_OK);

	return MUNIT_OK;
}
