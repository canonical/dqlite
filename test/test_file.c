#include "../include/dqlite.h"

#include "case.h"
#include "log.h"
#include "mem.h"

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

static void *setup(const MunitParameter params[], void *user_data)
{
	sqlite3_vfs *vfs;

	test_case_setup(params, user_data);

	vfs = munit_malloc(sizeof *vfs);

	vfs = dqlite_vfs_create("volatile", test_logger());
	munit_assert_ptr_not_null(vfs);

	sqlite3_vfs_register(vfs, 0);

	return vfs;
}

static void tear_down(void *data)
{
	sqlite3_vfs *vfs = data;

	sqlite3_vfs_unregister(vfs);

	dqlite_vfs_destroy(vfs);

	test_case_tear_down(data);
}

/******************************************************************************
 *
 * dqlite__file_read
 *
 ******************************************************************************/

/* If the file being read does not exists, an error is returned. */
static MunitResult test_read_cantopen(const MunitParameter params[], void *data)
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
static MunitResult test_read_empty(const MunitParameter params[], void *data)
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
static MunitResult test_read_then_write(const MunitParameter params[],
                                        void *               data)
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
    {TEST_MEM_FAULT_DELAY_PARAM, test_read_oom_delay},
    {TEST_MEM_FAULT_REPEAT_PARAM, test_read_oom_repeat},
    {NULL, NULL},
};

/* Test out of memory scenarios. */
static MunitResult test_read_oom(const MunitParameter params[], void *data)
{
	sqlite3_vfs *vfs = data;
	sqlite3 *    db  = __db_open(vfs);
	uint8_t *    buf;
	size_t       len;
	int          rc;

	(void)params;

	__db_exec(db, "CREATE TABLE test (n INT)");

	test_mem_fault_enable();

	rc = dqlite_file_read(vfs->zName, "test.db", &buf, &len);
	munit_assert_int(rc, ==, SQLITE_NOMEM);

	rc = sqlite3_close(db);
	munit_assert_int(rc, ==, SQLITE_OK);

	return MUNIT_OK;
}

static MunitTest dqlite__file_read_tests[] = {
    {"/cantopen", test_read_cantopen, setup, tear_down, 0, NULL},
    {"/empty", test_read_empty, setup, tear_down, 0, NULL},
    {"/then-write", test_read_then_write, setup, tear_down, 0, NULL},
    {"/oom", test_read_oom, setup, tear_down, 0, test_read_oom_params},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * Test suite
 *
 ******************************************************************************/

MunitSuite dqlite__file_suites[] = {
    {"_read", dqlite__file_read_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
