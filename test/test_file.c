#include "../include/dqlite.h"

#include "case.h"
#include "log.h"
#include "./lib/heap.h"
#include "./lib/runner.h"
#include "./lib/sqlite.h"

TEST_MODULE(file);

/******************************************************************************
 *
 * Helpers
 *
 ******************************************************************************/

/* Helper to execute a SQL statement. */
static void __db_exec(sqlite3 *db, const char *sql)
{
	int   rc;
	char *errmsg;

	rc = sqlite3_exec(db, sql, NULL, NULL, &errmsg);
	munit_assert_int(rc, ==, SQLITE_OK);
}

/* Helper to open and initialize a database, setting the page size and
 * WAL mode. */
static sqlite3 *__db_open(sqlite3_vfs *vfs)
{
	int      rc;
	sqlite3 *db;

	rc = sqlite3_open_v2("test.db",
	                     &db,
	                     SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
	                     vfs->zName);
	munit_assert_int(rc, ==, SQLITE_OK);

	__db_exec(db, "PRAGMA page_size=512");
	__db_exec(db, "PRAGMA synchronous=OFF");
	__db_exec(db, "PRAGMA journal_mode=WAL");

	return db;
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

	(void)user_data;

	test_heap_setup(params, user_data);
	test_sqlite_setup(params);

	logger = test_logger();

	vfs = dqlite_vfs_create("volatile", logger);
	munit_assert_ptr_not_null(vfs);

	sqlite3_vfs_register(vfs, 0);

	return vfs;
}

static void tear_down(void *data)
{
	sqlite3_vfs *vfs = data;

	sqlite3_vfs_unregister(vfs);

	dqlite_vfs_destroy(vfs);

	test_sqlite_tear_down();
	test_heap_tear_down(data);

	free(logger);
}

/******************************************************************************
 *
 * dqlite__file_read
 *
 ******************************************************************************/

TEST_SUITE(read);
TEST_SETUP(read, setup);
TEST_TEAR_DOWN(read, tear_down);

/* If the file being read does not exists, an error is returned. */
TEST_CASE(read, cantopen, NULL)
{
	sqlite3_vfs *vfs = data;
	uint8_t *    buf;
	size_t       len;
	int          rc;

	(void)params;

	rc = dqlite_file_read(vfs->zName, "test.db", &buf, &len);
	munit_assert_int(rc, ==, SQLITE_CANTOPEN);

	return MUNIT_OK;
}

/* Read the content of an empty file. */
TEST_CASE(read, empty, NULL)
{
	sqlite3_vfs *vfs = data;
	sqlite3 *    db;
	uint8_t *    buf;
	size_t       len;
	int          flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	int          rc;

	(void)params;

	rc = sqlite3_open_v2("test.db", &db, flags, vfs->zName);
	munit_assert_int(rc, ==, SQLITE_OK);

	rc = dqlite_file_read(vfs->zName, "test.db", &buf, &len);
	munit_assert_int(rc, ==, SQLITE_OK);

	munit_assert_ptr_null(buf);
	munit_assert_int(len, ==, 0);

	rc = sqlite3_close(db);
	munit_assert_int(rc, ==, SQLITE_OK);

	return MUNIT_OK;
}

/* Read the content of a database and WAL files and then write them back. */
TEST_CASE(read, then_write, NULL)
{
	sqlite3_vfs * vfs = data;
	sqlite3 *     db  = __db_open(vfs);
	int           rc;
	uint8_t *     buf1;
	uint8_t *     buf2;
	size_t        len1;
	size_t        len2;
	sqlite3_stmt *stmt;
	const char *  tail;

	(void)params;

	__db_exec(db, "CREATE TABLE test (n INT)");

	rc = dqlite_file_read(vfs->zName, "test.db", &buf1, &len1);
	munit_assert_int(rc, ==, SQLITE_OK);

	munit_assert_ptr_not_equal(buf1, NULL);
	munit_assert_int(len1, ==, 512);

	rc = dqlite_file_read(vfs->zName, "test.db-wal", &buf2, &len2);
	munit_assert_int(rc, ==, SQLITE_OK);

	munit_assert_ptr_not_equal(buf2, NULL);
	munit_assert_int(len2, ==, 1104);

	rc = sqlite3_close(db);
	munit_assert_int(rc, ==, SQLITE_OK);

	rc = dqlite_file_write(vfs->zName, "test.db", buf1, len1);
	munit_assert_int(rc, ==, SQLITE_OK);

	rc = dqlite_file_write(vfs->zName, "test.db-wal", buf2, len2);
	munit_assert_int(rc, ==, SQLITE_OK);

	sqlite3_free(buf1);
	sqlite3_free(buf2);

	rc = sqlite3_open_v2("test.db", &db, SQLITE_OPEN_READWRITE, "volatile");
	munit_assert_int(rc, ==, SQLITE_OK);

	rc = sqlite3_prepare(
	    db, "INSERT INTO test(n) VALUES(?)", -1, &stmt, &tail);
	munit_assert_int(rc, ==, SQLITE_OK);

	rc = sqlite3_finalize(stmt);
	munit_assert_int(rc, ==, SQLITE_OK);

	rc = sqlite3_close(db);
	munit_assert_int(rc, ==, SQLITE_OK);

	return MUNIT_OK;
}

static char *test_read_oom_delay[]  = {"0", "1", NULL};
static char *test_read_oom_repeat[] = {"1", NULL};

static MunitParameterEnum test_read_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, test_read_oom_delay},
    {TEST_HEAP_FAULT_REPEAT, test_read_oom_repeat},
    {NULL, NULL},
};

/* Test out of memory scenarios. */
TEST_CASE(read, oom, test_read_oom_params)
{
	sqlite3_vfs *vfs = data;
	sqlite3 *    db  = __db_open(vfs);
	uint8_t *    buf;
	size_t       len;
	int          rc;

	(void)params;

	__db_exec(db, "CREATE TABLE test (n INT)");

	test_heap_fault_enable();

	rc = dqlite_file_read(vfs->zName, "test.db", &buf, &len);
	munit_assert_int(rc, ==, SQLITE_NOMEM);

	rc = sqlite3_close(db);
	munit_assert_int(rc, ==, SQLITE_OK);

	return MUNIT_OK;
}
