#include <stdio.h>

#include "../lib/heap.h"
#include "../lib/runner.h"
#include "../lib/sqlite.h"

#include "../../include/dqlite.h"

SUITE(vfs);

#define N_VFS 2

struct fixture
{
	struct sqlite3_vfs vfs[N_VFS]; /* A "cluster" of VFS objects. */
	char names[8][N_VFS];          /* Registration names */
};

static void *setUp(const MunitParameter params[], void *user_data)
{
	struct fixture *f = munit_malloc(sizeof *f);
	unsigned i;
	int rv;

	SETUP_HEAP;
	SETUP_SQLITE;

	for (i = 0; i < N_VFS; i++) {
		sprintf(f->names[i], "%u", i);
		rv = dqlite_vfs_init(&f->vfs[i], f->names[i]);
		munit_assert_int(rv, ==, 0);
		sqlite3_vfs_register(&f->vfs[i], 0);
	}

	return f;
}

static void tearDown(void *data)
{
	struct fixture *f = data;
	unsigned i;

	for (i = 0; i < N_VFS; i++) {
		sqlite3_vfs_unregister(&f->vfs[i]);
		dqlite_vfs_close(&f->vfs[i]);
	}

	TEAR_DOWN_SQLITE;
	TEAR_DOWN_HEAP;

	free(f);
}

#define PAGE_SIZE 512

/* Helper to execute a SQL statement on the given DB. */
#define EXEC(DB, SQL)                                  \
	_rv = sqlite3_exec(DB, SQL, NULL, NULL, NULL); \
	munit_assert_int(_rv, ==, SQLITE_OK);

/* Open a new database connection on the given VFS. */
#define OPEN(VFS, DB)                                                         \
	do {                                                                  \
		int _flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;      \
		int _rv;                                                      \
		_rv = sqlite3_open_v2("test.db", &DB, _flags, VFS);           \
		munit_assert_int(_rv, ==, SQLITE_OK);                         \
		_rv = sqlite3_extended_result_codes(DB, 1);                   \
		munit_assert_int(_rv, ==, SQLITE_OK);                         \
		EXEC(DB, "PRAGMA page_size=512");                             \
		EXEC(DB, "PRAGMA synchronous=OFF");                           \
		EXEC(DB, "PRAGMA journal_mode=WAL");                          \
		_rv = sqlite3_db_config(DB, SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, \
					1, NULL);                             \
		munit_assert_int(_rv, ==, SQLITE_OK);                         \
	} while (0)

/* Close a database connection. */
#define CLOSE(DB)                                     \
	do {                                          \
		int _rv;                              \
		_rv = sqlite3_close(DB);              \
		munit_assert_int(_rv, ==, SQLITE_OK); \
	} while (0)

/* Prepare a statement. */
#define PREPARE(DB, STMT, SQL)                                      \
	do {                                                        \
		int _rv;                                            \
		_rv = sqlite3_prepare_v2(DB, SQL, -1, &STMT, NULL); \
		if (_rv != SQLITE_OK) {                             \
			munit_errorf("prepare '%s': %s (%d)", SQL,  \
				     sqlite3_errmsg(DB), _rv);      \
			munit_assert_int(_rv, ==, SQLITE_OK);       \
		}                                                   \
	} while (0)

/* Reset a statement. */
#define RESET(STMT, RV)                        \
	do {                                   \
		int _rv;                       \
		_rv = sqlite3_reset(STMT);     \
		munit_assert_int(_rv, ==, RV); \
	} while (0)

/* Finalize a statement. */
#define FINALIZE(STMT)                                \
	do {                                          \
		int _rv;                              \
		_rv = sqlite3_finalize(STMT);         \
		munit_assert_int(_rv, ==, SQLITE_OK); \
	} while (0)

/* Step through a statement and assert that the given value is returned. */
#define STEP(STMT, RV)                                                        \
	do {                                                                  \
		int _rv;                                                      \
		_rv = sqlite3_step(STMT);                                     \
		if (_rv != RV) {                                              \
			munit_errorf("step: %s (%d)",                         \
				     sqlite3_errmsg(sqlite3_db_handle(STMT)), \
				     _rv);                                    \
		}                                                             \
	} while (0)

/* Poll the given VFS object. */
#define POLL(VFS, FRAMES, N)                                        \
	do {                                                        \
		sqlite3_vfs *vfs = sqlite3_vfs_find(VFS);           \
		int _rv;                                            \
		_rv = dqlite_vfs_poll(vfs, "test.db", &FRAMES, &N); \
		munit_assert_int(_rv, ==, 0);                       \
	} while (0)

/* Commit WAL frames to the given VFS. */
#define COMMIT(VFS, N, PAGE_NUMBERS, FRAMES)                             \
	do {                                                             \
		sqlite3_vfs *vfs = sqlite3_vfs_find(VFS);                \
		int _rv;                                                 \
		_rv = dqlite_vfs_commit(vfs, "test.db", N, PAGE_NUMBERS, \
					FRAMES);                         \
		munit_assert_int(_rv, ==, 0);                            \
	} while (0)

/* Open and close a new connection using the dqlite VFS. */
TEST(vfs, open, setUp, tearDown, 0, NULL)
{
	sqlite3 *db;
	OPEN("1", db);
	CLOSE(db);
	return MUNIT_OK;
}

/* Write transactions are not committed synchronously, so they are not visible
 * from other connections yet when sqlite3_step() returns. */
TEST(vfs, unreplicatedCommitIsNotVisible, setUp, tearDown, 0, NULL)
{
	sqlite3 *db1;
	sqlite3 *db2;
	sqlite3_stmt *stmt1;
	sqlite3_stmt *stmt2;
	int rv;
	OPEN("1", db1);
	PREPARE(db1, stmt1, "CREATE TABLE test(n INT)");
	STEP(stmt1, SQLITE_DONE);
	FINALIZE(stmt1);

	OPEN("1", db2);
	rv = sqlite3_prepare_v2(db2, "SELECT * FROM test", -1, &stmt2, NULL);
	munit_assert_int(rv, ==, SQLITE_ERROR);

	CLOSE(db1);
	CLOSE(db2);

	return MUNIT_OK;
}

/* A call to dqlite_vfs_poll() after a sqlite3_step() triggered a write
 * transaction returns the newly appended WAL frames. */
TEST(vfs, pollAfterWriteTransaction, setUp, tearDown, 0, NULL)
{
	sqlite3 *db;
	sqlite3_stmt *stmt;
	dqlite_vfs_frame *frames;
	unsigned n;
	unsigned i;

	OPEN("1", db);
	PREPARE(db, stmt, "CREATE TABLE test(n INT)");
	STEP(stmt, SQLITE_DONE);

	POLL("1", frames, n);

	munit_assert_ptr_not_null(frames);
	munit_assert_int(n, ==, 2);

	for (i = 0; i < n; i++) {
		munit_assert_int(frames[i].page_number, ==, i + 1);
		sqlite3_free(frames[i].data);
	}
	sqlite3_free(frames);

	FINALIZE(stmt);
	CLOSE(db);

	return MUNIT_OK;
}

/* A call to dqlite_vfs_poll() after a sqlite3_step() triggered a write
 * transaction sets a write lock on the WAL. */
TEST(vfs, pollWriteLock, setUp, tearDown, 0, NULL)
{
	sqlite3 *db1;
	sqlite3 *db2;
	sqlite3_stmt *stmt1;
	sqlite3_stmt *stmt2;
	dqlite_vfs_frame *frames;
	unsigned n;
	unsigned i;

	OPEN("1", db1);
	OPEN("1", db2);

	PREPARE(db1, stmt1, "CREATE TABLE test(n INT)");
	PREPARE(db2, stmt2, "CREATE TABLE test2(n INT)");

	STEP(stmt1, SQLITE_DONE);
	POLL("1", frames, n);
	munit_assert_ptr_not_null(frames);

	STEP(stmt2, SQLITE_BUSY);
	RESET(stmt2, SQLITE_BUSY);

	FINALIZE(stmt1);
	FINALIZE(stmt2);

	CLOSE(db1);
	CLOSE(db2);

	for (i = 0; i < n; i++) {
		munit_assert_int(frames[i].page_number, ==, i + 1);
		sqlite3_free(frames[i].data);
	}
	sqlite3_free(frames);

	return MUNIT_OK;
}

/* Use dqlite_vfs_commit() to actually modify the WAL after quorum is reached,
 * then perform a read transaction and check that it can see the committed
 * changes. */
TEST(vfs, commitThenRead, setUp, tearDown, 0, NULL)
{
	sqlite3 *db;
	sqlite3_stmt *stmt;
	dqlite_vfs_frame *frames;
	unsigned *page_numbers;
	void *buf;
	unsigned n;
	unsigned i;

	OPEN("1", db);

	PREPARE(db, stmt, "CREATE TABLE test(n INT)");
	STEP(stmt, SQLITE_DONE);

	POLL("1", frames, n);

	page_numbers = munit_malloc(sizeof *page_numbers * n);
	buf = munit_malloc(n * PAGE_SIZE);
	for (i = 0; i < n; i++) {
		page_numbers[i] = frames[i].page_number;
		memcpy(buf + i * PAGE_SIZE, frames[i].data, PAGE_SIZE);
	}

	COMMIT("1", n, page_numbers, buf);

	for (i = 0; i < n; i++) {
		sqlite3_free(frames[i].data);
	}
	sqlite3_free(frames);

	free(page_numbers);
	free(buf);

	FINALIZE(stmt);

	PREPARE(db, stmt, "SELECT * FROM test");
	STEP(stmt, SQLITE_DONE);
	FINALIZE(stmt);

	CLOSE(db);

	return MUNIT_OK;
}

/* Use dqlite_vfs_commit() to actually modify the WAL after quorum is reached,
 * then perform another commit and finally run a read transaction and check that
 * it can see all committed changes. */
TEST(vfs, commitThenCommitAgainThenRead, setUp, tearDown, 0, NULL)
{
	sqlite3 *db;
	sqlite3_stmt *stmt;
	dqlite_vfs_frame *frames;
	unsigned *page_numbers;
	void *buf;
	unsigned n;
	unsigned i;

	OPEN("1", db);

	PREPARE(db, stmt, "CREATE TABLE test(n INT)");
	STEP(stmt, SQLITE_DONE);

	POLL("1", frames, n);

	page_numbers = munit_malloc(sizeof *page_numbers * n);
	buf = munit_malloc(n * PAGE_SIZE);
	for (i = 0; i < n; i++) {
		page_numbers[i] = frames[i].page_number;
		memcpy(buf + i * PAGE_SIZE, frames[i].data, PAGE_SIZE);
	}

	COMMIT("1", n, page_numbers, buf);

	for (i = 0; i < n; i++) {
		sqlite3_free(frames[i].data);
	}
	sqlite3_free(frames);

	free(page_numbers);
	free(buf);

	FINALIZE(stmt);

	PREPARE(db, stmt, "INSERT INTO test(n) VALUES(123)");
	STEP(stmt, SQLITE_DONE);

	POLL("1", frames, n);

	page_numbers = munit_malloc(sizeof *page_numbers * n);
	buf = munit_malloc(n * PAGE_SIZE);
	for (i = 0; i < n; i++) {
		page_numbers[i] = frames[i].page_number;
		memcpy(buf + i * PAGE_SIZE, frames[i].data, PAGE_SIZE);
	}

	COMMIT("1", n, page_numbers, buf);

	for (i = 0; i < n; i++) {
		sqlite3_free(frames[i].data);
	}
	sqlite3_free(frames);

	free(page_numbers);
	free(buf);

	FINALIZE(stmt);

	PREPARE(db, stmt, "SELECT * FROM test");
	STEP(stmt, SQLITE_ROW);
	munit_assert_int(sqlite3_column_int(stmt, 0), ==, 123);
	STEP(stmt, SQLITE_DONE);

	FINALIZE(stmt);

	CLOSE(db);

	return MUNIT_OK;
}

/* Use dqlite_vfs_commit() to actually modify the WAL after quorum is reached,
 * then open a new connection. A read transaction started in the second
 * connection can see the changes committed by the first one. */
TEST(vfs, commitThenReadOnNewConn, setUp, tearDown, 0, NULL)
{
	sqlite3 *db1;
	sqlite3 *db2;
	sqlite3_stmt *stmt;
	dqlite_vfs_frame *frames;
	unsigned *page_numbers;
	void *buf;
	unsigned n;
	unsigned i;

	OPEN("1", db1);
	OPEN("1", db2);

	PREPARE(db1, stmt, "CREATE TABLE test(n INT)");
	STEP(stmt, SQLITE_DONE);

	POLL("1", frames, n);

	page_numbers = munit_malloc(sizeof *page_numbers * n);
	buf = munit_malloc(n * PAGE_SIZE);
	for (i = 0; i < n; i++) {
		page_numbers[i] = frames[i].page_number;
		memcpy(buf + i * PAGE_SIZE, frames[i].data, PAGE_SIZE);
	}

	COMMIT("1", n, page_numbers, buf);

	for (i = 0; i < n; i++) {
		sqlite3_free(frames[i].data);
	}
	sqlite3_free(frames);

	free(page_numbers);
	free(buf);

	FINALIZE(stmt);

	PREPARE(db2, stmt, "SELECT * FROM test");
	STEP(stmt, SQLITE_DONE);
	FINALIZE(stmt);

	CLOSE(db1);
	CLOSE(db2);

	return MUNIT_OK;
}

/* Use dqlite_vfs_commit() to actually modify the WAL after quorum is reached,
 * then close the committing connection and open a new one. A read transaction
 * started in the second connection can see the changes committed by the first
 * one. */
TEST(vfs, commitThenCloseThenReadOnNewConn, setUp, tearDown, 0, NULL)
{
	sqlite3 *db;
	sqlite3_stmt *stmt;
	dqlite_vfs_frame *frames;
	unsigned *page_numbers;
	void *buf;
	unsigned n;
	unsigned i;

	OPEN("1", db);
	PREPARE(db, stmt, "CREATE TABLE test(n INT)");
	STEP(stmt, SQLITE_DONE);

	POLL("1", frames, n);

	page_numbers = munit_malloc(sizeof *page_numbers * n);
	buf = munit_malloc(n * PAGE_SIZE);
	for (i = 0; i < n; i++) {
		page_numbers[i] = frames[i].page_number;
		memcpy(buf + i * PAGE_SIZE, frames[i].data, PAGE_SIZE);
	}

	COMMIT("1", n, page_numbers, buf);

	for (i = 0; i < n; i++) {
		sqlite3_free(frames[i].data);
	}
	sqlite3_free(frames);

	free(page_numbers);
	free(buf);

	FINALIZE(stmt);

	CLOSE(db);

	OPEN("1", db);
	PREPARE(db, stmt, "SELECT * FROM test");
	STEP(stmt, SQLITE_DONE);
	FINALIZE(stmt);
	CLOSE(db);

	return MUNIT_OK;
}
