#ifdef DQLITE_EXPERIMENTAL

#include <libco.h>

#include "../include/dqlite.h"
#include "../src/replication.h"

#include "case.h"
#include "log.h"

/******************************************************************************
 *
 * Helpers
 *
 ******************************************************************************/

struct fixture {
	struct dqlite__replication_ctx ctx;
	sqlite3_wal_replication        replication;
	sqlite3_vfs *                  vfs;
	sqlite3 *                      db1;
	sqlite3 *                      db2;
};

/* Execute a statement. */
static void __db_exec(sqlite3 *db, const char *sql)
{
	char *errmsg;
	int   rc;

	rc = sqlite3_exec(db, sql, NULL, NULL, &errmsg);
	munit_assert_int(rc, ==, SQLITE_OK);
}

/* Open a test database and configure it */
static sqlite3 *__db_open(int replicated)
{
	sqlite3 *db;
	int      flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	int      rc;

	rc = sqlite3_open_v2("test.db", &db, flags, "dqlite");
	munit_assert_int(rc, ==, SQLITE_OK);

	/* Enable extended result codes by default */
	rc = sqlite3_extended_result_codes(db, 1);
	munit_assert_int(rc, ==, SQLITE_OK);

	/* Confgure the database. */
	__db_exec(db, "PRAGMA page_size=512");
	__db_exec(db, "PRAGMA synchronous=OFF");
	__db_exec(db, "PRAGMA journal_mode=WAL");

	if (replicated) {
		rc = sqlite3_wal_replication_leader(db, "main", "dqlite", db);
		munit_assert_int(rc, ==, SQLITE_OK);
	}

	return db;
}

/******************************************************************************
 *
 * Setup and tear down
 *
 ******************************************************************************/

static void *setup(const MunitParameter params[], void *user_data)
{
	struct fixture *f;
	dqlite_logger * logger = test_logger();
	int             rc;

	(void)params;
	(void)user_data;

	test_case_setup(params, user_data);

	f = munit_malloc(sizeof *f);

	dqlite__replication_ctx_init(&f->ctx);

	f->ctx.main_coroutine = co_active();

	f->replication.iVersion = 1;
	f->replication.zName    = "dqlite";
	f->replication.pAppData = &f->ctx;

	f->replication.xBegin  = dqlite__replication_begin;
	f->replication.xAbort  = dqlite__replication_abort;
	f->replication.xFrames = dqlite__replication_frames;
	f->replication.xUndo   = dqlite__replication_undo;
	f->replication.xEnd    = dqlite__replication_end;

	rc = sqlite3_wal_replication_register(&f->replication, 0);
	munit_assert_int(rc, ==, SQLITE_OK);

	f->vfs = dqlite_vfs_create(f->replication.zName, logger);
	munit_assert_ptr_not_null(f->vfs);

	sqlite3_vfs_register(f->vfs, 0);

	f->db1 = __db_open(1);
	f->db2 = __db_open(0);

	return f;
}

static void tear_down(void *data)
{
	struct fixture *f = data;
	int             rc;

	rc = sqlite3_close_v2(f->db1);
	munit_assert_int(rc, ==, SQLITE_OK);

	rc = sqlite3_close_v2(f->db2);
	munit_assert_int(rc, ==, SQLITE_OK);

	sqlite3_vfs_unregister(f->vfs);
	sqlite3_wal_replication_unregister(&f->replication);

	dqlite_vfs_destroy(f->vfs);

	dqlite__replication_ctx_close(&f->ctx);

	test_case_tear_down(data);
}

/******************************************************************************
 *
 * Concurrency
 *
 ******************************************************************************/

static struct fixture *test_coroutine_arg;

static void test_coroutine()
{
	struct fixture *f = test_coroutine_arg;

	__db_exec(f->db1, "CREATE TABLE test (n INT)");
	co_switch(f->ctx.main_coroutine);
}

/* A coroutine attempting to perform a concurrent write while another has locked
 * the WAL fails with SQLITE_BUSY. */
static MunitResult test_concurrency_write(const MunitParameter params[],
                                          void *               data)
{
	struct fixture *f         = data;
	cothread_t *    coroutine = co_create(1024 * 1024, test_coroutine);
	sqlite3_stmt *  stmt;
	int             rc;

	(void)params;

	/* Start the test coroutine, which will enter a write transaction
	 * without completing it and return control here. */
	test_coroutine_arg = f;
	co_switch(coroutine);

	/* Try to perform a write transaction on another connection. */
	rc = sqlite3_prepare_v2(
	    f->db2, "CREATE TABLE test2 (n INT)", -1, &stmt, NULL);
	munit_assert_int(rc, ==, SQLITE_OK);

	rc = sqlite3_step(stmt);
	munit_assert_int(rc, ==, SQLITE_BUSY);

	rc = sqlite3_finalize(stmt);
	munit_assert_int(rc, ==, SQLITE_BUSY);

	co_switch(coroutine);
	co_delete(coroutine);

	return MUNIT_OK;
}

static MunitTest concurrency_tests[] = {
    {"/write", test_concurrency_write, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * Test suite
 *
 ******************************************************************************/

MunitSuite dqlite__replication_suites[] = {
    {"/concurrency", concurrency_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};

#endif /* DQLITE_EXPERIMENTAL */
