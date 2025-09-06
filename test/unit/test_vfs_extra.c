#include <sqlite3.h>
#include <stdio.h>

#include "../lib/fs.h"
#include "../lib/heap.h"
#include "../lib/runner.h"
#include "../lib/sqlite.h"

#include "../../include/dqlite.h"
#include "../../src/raft.h"
#include "../../src/lib/byte.h"
#include "../../src/vfs.h"

#include <sys/mman.h>

SUITE(vfs_extra);

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
		sprintf(f->names[i], "%u", i + 1);
		rv = VfsInit(&f->vfs[i], f->names[i]);
		munit_assert_int(rv, ==, 0);
		rv = sqlite3_vfs_register(&f->vfs[i], 0);
		munit_assert_int(rv, ==, 0);
	}

	return f;
}

static void tearDown(void *data)
{
	struct fixture *f = data;
	unsigned i;
	int rv;

	for (i = 0; i < N_VFS; i++) {
		rv = sqlite3_vfs_unregister(&f->vfs[i]);
		munit_assert_int(rv, ==, 0);
		VfsClose(&f->vfs[i]);
	}

	TEAR_DOWN_SQLITE;
	TEAR_DOWN_HEAP;

	free(f);
}

extern unsigned dq_sqlite_pending_byte;
static void tearDownRestorePendingByte(void *data)
{
	sqlite3_test_control(SQLITE_TESTCTRL_PENDING_BYTE, 0x40000000);
	dq_sqlite_pending_byte = 0x40000000;
	tearDown(data);
}

#define DB_PAGE_SIZE 512

#define PRAGMA(DB, COMMAND)                                          \
	_rv = sqlite3_exec(DB, "PRAGMA " COMMAND, NULL, NULL, NULL); \
	if (_rv != SQLITE_OK) {                                      \
		munit_errorf("PRAGMA " COMMAND ": %s (%d)",          \
			     sqlite3_errmsg(DB), _rv);               \
	}

#define VFS_PATH "test.db"

/* Open a new database connection on the given VFS. */
#define OPEN(VFS, DB)                                                         \
	do {                                                                  \
		int _flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;      \
		int _rv = sqlite3_open_v2(VFS_PATH, &DB, _flags, VFS);        \
		munit_assert_int(_rv, ==, SQLITE_OK);                         \
		_rv = sqlite3_extended_result_codes(DB, 1);                   \
		munit_assert_int(_rv, ==, SQLITE_OK);                         \
		PRAGMA(DB, "page_size=512");                                  \
		PRAGMA(DB, "synchronous=OFF");                                \
		PRAGMA(DB, "journal_mode=WAL");                               \
		PRAGMA(DB, "cache_size=1");                                   \
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

/* Shortcut for PREPARE, STEP, FINALIZE. */
#define EXEC(DB, SQL)                     \
	do {                              \
		sqlite3_stmt *_stmt;      \
		PREPARE(DB, _stmt, SQL);  \
		STEP(_stmt, SQLITE_DONE); \
		FINALIZE(_stmt);          \
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

/* Poll the given VFS object and serialize the transaction data into the given
 * tx object. */
#define POLL(DB, TX)                          \
	do {                                  \
		int _rv;                      \
		_rv = VfsPoll(DB, &(TX));     \
		munit_assert_int(_rv, ==, 0); \
	} while (0)

/* Apply WAL frames to the given VFS. */
#define APPLY(DB, TX)                                                 \
	do {                                                          \
		int _rv;                                              \
		_rv = VfsApply(DB, &(TX)); \
		munit_assert_int(_rv, ==, 0);                         \
	} while (0)

/* Abort a transaction on the given VFS. */
#define ABORT(DB)                             \
	do {                                  \
		int _rv = VfsAbort(DB);       \
		munit_assert_int(_rv, ==, 0); \
	} while (0)

/* Release all memory used by a vfsTransaction object. */
#define DONE(TX)                                                 \
	do {                                                     \
		for (unsigned _i = 0; _i < (TX).n_pages; _i++) { \
			sqlite3_free((TX).pages[_i]);            \
		}                                                \
		sqlite3_free((TX).pages);                        \
		sqlite3_free((TX).page_numbers);                 \
	} while (0)

/* Peform a full checkpoint on the given database. */
#define CHECKPOINT(DB)                                                     \
	do {                                                               \
		int _size;                                                 \
		int _ckpt;                                                 \
		int _rv;                                                   \
		_rv = sqlite3_wal_checkpoint_v2(                           \
		    DB, NULL, SQLITE_CHECKPOINT_TRUNCATE, &_size, &_ckpt); \
		if (_rv != SQLITE_OK) {                                    \
			munit_errorf("checkpoint: %s (%d)",                \
				     sqlite3_errmsg(DB), _rv);             \
		}                                                          \
		munit_assert_int(_size, ==, 0);                            \
		munit_assert_int(_ckpt, ==, 0);                            \
	} while (0)

/* Perform a full checkpoint on a fresh connection, mimicking dqlite's
 * checkpoint behavior. */
#define CHECKPOINT_FRESH(VFS)    \
	do {                     \
		sqlite3 *_db;    \
		OPEN(VFS, _db);  \
		CHECKPOINT(_db); \
		CLOSE(_db);      \
	} while (0)

/* Attempt to perform a full checkpoint on the given database, but fail. */
#define CHECKPOINT_FAIL(DB, RV)                                            \
	do {                                                               \
		int _size;                                                 \
		int _ckpt;                                                 \
		int _rv;                                                   \
		_rv = sqlite3_wal_checkpoint_v2(                           \
		    DB, NULL, SQLITE_CHECKPOINT_TRUNCATE, &_size, &_ckpt); \
		munit_assert_int(_rv, ==, RV);                             \
	} while (0)

/* Open and close a new connection using the dqlite VFS. */
TEST(vfs_extra, open, setUp, tearDown, 0, NULL)
{
	sqlite3 *db;
	OPEN("1", db);
	CLOSE(db);
	return MUNIT_OK;
}

/* New frames appended to the WAL file by a sqlite3_step() call that has
 * triggered a write transactions are not immediately visible to other
 * connections after sqlite3_step() has returned. */
TEST(vfs_extra, writeTransactionNotImmediatelyVisible, setUp, tearDown, 0, NULL)
{
	sqlite3 *db1;
	sqlite3 *db2;
	sqlite3_stmt *stmt;
	int rv;

	OPEN("1", db1);
	EXEC(db1, "CREATE TABLE test(n INT)");

	OPEN("1", db2);
	rv = sqlite3_prepare_v2(db2, "SELECT * FROM test", -1, &stmt, NULL);
	munit_assert_int(rv, ==, SQLITE_ERROR);
	munit_assert_string_equal(sqlite3_errmsg(db2), "no such table: test");

	struct vfsTransaction tx;
	POLL(db1, tx);
	ABORT(db1);
	DONE(tx);

	CLOSE(db1);
	CLOSE(db2);

	return MUNIT_OK;
}

/* Invoking VfsPoll() after a call to sqlite3_step() has triggered a
 * write transaction returns the newly appended WAL frames. */
TEST(vfs_extra, pollAfterWriteTransaction, setUp, tearDown, 0, NULL)
{
	sqlite3 *db;
	sqlite3_stmt *stmt;
	struct vfsTransaction tx;
	unsigned i;

	OPEN("1", db);

	PREPARE(db, stmt, "CREATE TABLE test(n INT)");
	STEP(stmt, SQLITE_DONE);

	POLL(db, tx);

	munit_assert_ptr_not_null(tx.pages);
	munit_assert_ptr_not_null(tx.page_numbers);
	munit_assert_int(tx.n_pages, ==, 2);
	for (i = 0; i < tx.n_pages; i++) {
		munit_assert_int(tx.page_numbers[i], ==, i + 1);
	}

	DONE(tx);

	FINALIZE(stmt);
	ABORT(db);
	CLOSE(db);

	return MUNIT_OK;
}

/* Invoking VfsPoll() after a call to sqlite3_step() has triggered a
 * write transaction sets a write lock on the WAL, so calls to sqlite3_step()
 * from other connections return SQLITE_BUSY if they try to start a write
 * transaction. */
TEST(vfs_extra, pollAcquireWriteLock, setUp, tearDown, 0, NULL)
{
	sqlite3 *db1;
	sqlite3 *db2;
	sqlite3_stmt *stmt1;
	sqlite3_stmt *stmt2;
	struct vfsTransaction tx;

	OPEN("1", db1);
	OPEN("1", db2);

	PREPARE(db1, stmt1, "CREATE TABLE test(n INT)");
	PREPARE(db2, stmt2, "CREATE TABLE test2(n INT)");

	STEP(stmt1, SQLITE_DONE);
	POLL(db1, tx);
	DONE(tx);

	STEP(stmt2, SQLITE_BUSY);
	RESET(stmt2, SQLITE_BUSY);

	FINALIZE(stmt1);
	FINALIZE(stmt2);

	CLOSE(db2);
	ABORT(db1);
	CLOSE(db1);

	return MUNIT_OK;
}

/* If the page cache limit is exceeded during a call to sqlite3_step() that has
 * triggered a write transaction, some WAL frames will be written and then
 * overwritten before the final commit. Only the final version of the frame is
 * included in the set returned by VfsPoll(). */
TEST(vfs_extra, pollAfterPageStress, setUp, tearDown, 0, NULL)
{
	sqlite3 *db;
	sqlite3_stmt *stmt;
	struct vfsTransaction tx;
	unsigned i;
	char sql[64];

	OPEN("1", db);

	EXEC(db, "CREATE TABLE test(n INT)");

	POLL(db, tx);
	APPLY(db, tx);
	DONE(tx);

	EXEC(db, "BEGIN");
	for (i = 0; i < 163; i++) {
		sprintf(sql, "INSERT INTO test(n) VALUES(%d)", i + 1);
		EXEC(db, sql);
		POLL(db, tx);
		munit_assert_int(tx.n_pages, ==, 0);
	}
	for (i = 0; i < 163; i++) {
		sprintf(sql, "UPDATE test SET n=%d WHERE n=%d", i, i + 1);
		EXEC(db, sql);
		POLL(db, tx);
		munit_assert_int(tx.n_pages, ==, 0);
	}
	EXEC(db, "COMMIT");

	POLL(db, tx);

	/* Five frames were replicated and the first frame actually contains a
	 * spill of the third page. */
	munit_assert_int(tx.n_pages, ==, 6);
	munit_assert_int(tx.page_numbers[0], ==, 3);
	munit_assert_int(tx.page_numbers[1], ==, 4);
	munit_assert_int(tx.page_numbers[2], ==, 5);
	munit_assert_int(tx.page_numbers[3], ==, 1);
	munit_assert_int(tx.page_numbers[4], ==, 2);

	APPLY(db, tx);
	DONE(tx);

	/* All records have been inserted. */
	PREPARE(db, stmt, "SELECT * FROM test");
	for (i = 0; i < 163; i++) {
		STEP(stmt, SQLITE_ROW);
		munit_assert_int(sqlite3_column_int(stmt, 0), ==, i);
	}
	STEP(stmt, SQLITE_DONE);
	FINALIZE(stmt);

	CLOSE(db);

	return MUNIT_OK;
}

/* Set the SQLite PENDING_BYTE at the start of the second page and make sure
 * all data entry is successful.
 */
TEST(vfs_extra, adaptPendingByte, setUp, tearDownRestorePendingByte, 0, NULL)
{
	sqlite3 *db;
	sqlite3_stmt *stmt;
	struct vfsTransaction tx;
	int i;
	int n;
	char sql[64];

	/* Set the pending byte at the start of the second page */
	const unsigned new_pending_byte = 512;
	dq_sqlite_pending_byte = new_pending_byte;
	sqlite3_test_control(SQLITE_TESTCTRL_PENDING_BYTE, new_pending_byte);

	OPEN("1", db);

	EXEC(db, "CREATE TABLE test(n INT)");

	POLL(db, tx);
	APPLY(db, tx);
	DONE(tx);

	EXEC(db, "BEGIN");
	n = 65536;
	for (i = 0; i < n; i++) {
		sprintf(sql, "INSERT INTO test(n) VALUES(%d)", i);
		EXEC(db, sql);
		POLL(db, tx);
		munit_assert_uint(tx.n_pages, ==, 0);
	}
	EXEC(db, "COMMIT");

	POLL(db, tx);
	APPLY(db, tx);
	DONE(tx);

	/* All records have been inserted. */
	PREPARE(db, stmt, "SELECT * FROM test");
	for (i = 0; i < n; i++) {
		STEP(stmt, SQLITE_ROW);
		munit_assert_int(sqlite3_column_int(stmt, 0), ==, i);
	}
	STEP(stmt, SQLITE_DONE);
	FINALIZE(stmt);

	CLOSE(db);

	return MUNIT_OK;
}

/* Use VfsApply() to actually modify the WAL after a write transaction
 * was triggered by a call to sqlite3_step(), then perform a read transaction
 * and check that it can see the transaction changes. */
TEST(vfs_extra, applyMakesTransactionVisible, setUp, tearDown, 0, NULL)
{
	sqlite3 *db;
	sqlite3_stmt *stmt;
	struct vfsTransaction tx;

	OPEN("1", db);

	EXEC(db, "CREATE TABLE test(n INT)");

	POLL(db, tx);
	APPLY(db, tx);
	DONE(tx);

	PREPARE(db, stmt, "SELECT * FROM test");
	STEP(stmt, SQLITE_DONE);
	FINALIZE(stmt);

	CLOSE(db);

	return MUNIT_OK;
}

/* Use VfsApply() to actually modify the WAL after a write transaction
 * was triggered by an explicit "COMMIT" statement and check that changes are
 * visible. */
TEST(vfs_extra, applyExplicitTransaction, setUp, tearDown, 0, NULL)
{
	sqlite3 *db;
	sqlite3_stmt *stmt;
	struct vfsTransaction tx;

	OPEN("1", db);

	PREPARE(db, stmt, "BEGIN");
	STEP(stmt, SQLITE_DONE);
	POLL(db, tx);
	munit_assert_int(tx.n_pages, ==, 0);
	FINALIZE(stmt);

	PREPARE(db, stmt, "CREATE TABLE test(n INT)");
	STEP(stmt, SQLITE_DONE);
	POLL(db, tx);
	munit_assert_int(tx.n_pages, ==, 0);
	FINALIZE(stmt);

	PREPARE(db, stmt, "COMMIT");
	STEP(stmt, SQLITE_DONE);
	POLL(db, tx);
	munit_assert_int(tx.n_pages, ==, 2);
	APPLY(db, tx);
	DONE(tx);
	FINALIZE(stmt);

	PREPARE(db, stmt, "SELECT * FROM test");
	STEP(stmt, SQLITE_DONE);
	FINALIZE(stmt);

	CLOSE(db);

	return MUNIT_OK;
}

/* Perform two consecutive full write transactions using sqlite3_step(),
 * VfsPoll() and VfsApply(), then run a read transaction and
 * check that it can see all committed changes. */
TEST(vfs_extra, consecutiveWriteTransactions, setUp, tearDown, 0, NULL)
{
	sqlite3 *db;
	sqlite3_stmt *stmt;
	struct vfsTransaction tx;

	OPEN("1", db);

	EXEC(db, "CREATE TABLE test(n INT)");

	POLL(db, tx);
	APPLY(db, tx);
	DONE(tx);

	EXEC(db, "INSERT INTO test(n) VALUES(123)");

	POLL(db, tx);
	APPLY(db, tx);
	DONE(tx);

	PREPARE(db, stmt, "SELECT * FROM test");
	STEP(stmt, SQLITE_ROW);
	munit_assert_int(sqlite3_column_int(stmt, 0), ==, 123);
	STEP(stmt, SQLITE_DONE);

	FINALIZE(stmt);

	CLOSE(db);

	return MUNIT_OK;
}

/* Perform three consecutive write transactions, then re-open the database and
 * finally run a read transaction and check that it can see all committed
 * changes. */
TEST(vfs_extra,
     reopenAfterConsecutiveWriteTransactions,
     setUp,
     tearDown,
     0,
     NULL)
{
	sqlite3 *db;
	sqlite3_stmt *stmt;
	struct vfsTransaction tx;

	OPEN("1", db);

	EXEC(db, "CREATE TABLE foo(id INT)");
	POLL(db, tx);
	APPLY(db, tx);
	DONE(tx);

	EXEC(db, "CREATE TABLE bar (id INT)");
	POLL(db, tx);
	APPLY(db, tx);
	DONE(tx);

	EXEC(db, "INSERT INTO foo(id) VALUES(1)");
	POLL(db, tx);
	APPLY(db, tx);
	DONE(tx);

	CLOSE(db);

	OPEN("1", db);

	PREPARE(db, stmt, "SELECT * FROM sqlite_master");
	STEP(stmt, SQLITE_ROW);
	FINALIZE(stmt);

	CLOSE(db);

	return MUNIT_OK;
}

/* Use VfsApply() to actually modify the WAL after a write transaction
 * was triggered by sqlite3_step(), and verify that the transaction is visible
 * from another existing connection. */
TEST(vfs_extra,
     transactionIsVisibleFromExistingConnection,
     setUp,
     tearDown,
     0,
     NULL)
{
	sqlite3 *db1;
	sqlite3 *db2;
	sqlite3_stmt *stmt;
	struct vfsTransaction tx;

	OPEN("1", db1);
	OPEN("1", db2);

	EXEC(db1, "CREATE TABLE test(n INT)");

	POLL(db1, tx);
	APPLY(db1, tx);
	DONE(tx);

	PREPARE(db2, stmt, "SELECT * FROM test");
	STEP(stmt, SQLITE_DONE);
	FINALIZE(stmt);

	CLOSE(db1);
	CLOSE(db2);

	return MUNIT_OK;
}

/* Use VfsApply() to actually modify the WAL after a write transaction
 * was triggered by sqlite3_step(), and verify that the transaction is visible
 * from a brand new connection. */
TEST(vfs_extra, transactionIsVisibleFromNewConnection, setUp, tearDown, 0, NULL)
{
	sqlite3 *db1;
	sqlite3 *db2;
	sqlite3_stmt *stmt;
	struct vfsTransaction tx;

	OPEN("1", db1);

	EXEC(db1, "CREATE TABLE test(n INT)");

	POLL(db1, tx);
	APPLY(db1, tx);
	DONE(tx);

	OPEN("1", db2);

	PREPARE(db2, stmt, "SELECT * FROM test");
	STEP(stmt, SQLITE_DONE);
	FINALIZE(stmt);

	CLOSE(db1);
	CLOSE(db2);

	return MUNIT_OK;
}

/* Use VfsApply() to actually modify the WAL after a write transaction
 * was triggered by sqlite3_step(), then close the connection and open a new
 * one. A read transaction started in the new connection can see the changes
 * committed by the first one. */
TEST(vfs_extra,
     transactionIsVisibleFromReopenedConnection,
     setUp,
     tearDown,
     0,
     NULL)
{
	sqlite3 *db;
	sqlite3_stmt *stmt;
	struct vfsTransaction tx;

	OPEN("1", db);

	EXEC(db, "CREATE TABLE test(n INT)");

	POLL(db, tx);
	APPLY(db, tx);
	DONE(tx);

	CLOSE(db);

	OPEN("1", db);
	PREPARE(db, stmt, "SELECT * FROM test");
	STEP(stmt, SQLITE_DONE);
	FINALIZE(stmt);
	CLOSE(db);

	return MUNIT_OK;
}

/* Use VfsApply() to replicate the very first write transaction on a
 * different VFS than the one that initially generated it. In that case it's
 * necessary to initialize the database file on the other VFS by opening and
 * closing a connection. */
TEST(vfs_extra, firstApplyOnDifferentVfs, setUp, tearDown, 0, NULL)
{
	sqlite3 *db1;
	sqlite3 *db2;
	sqlite3_stmt *stmt;
	struct vfsTransaction tx;

	OPEN("1", db1);

	PREPARE(db1, stmt, "CREATE TABLE test(n INT)");
	STEP(stmt, SQLITE_DONE);
	POLL(db1, tx);
	APPLY(db1, tx);

	OPEN("2", db2);
	APPLY(db2, tx);
	CLOSE(db2);

	DONE(tx);

	FINALIZE(stmt);
	CLOSE(db1);

	return MUNIT_OK;
}

/* Use VfsApply() to replicate a second write transaction on a different
 * VFS than the one that initially generated it. In that case it's not necessary
 * to do anything special before calling VfsApply(). */
TEST(vfs_extra, secondApplyOnDifferentVfs, setUp, tearDown, 0, NULL)
{
	sqlite3 *db1;
	sqlite3 *db2;
	struct vfsTransaction tx;

	OPEN("1", db1);

	EXEC(db1, "CREATE TABLE test(n INT)");
	POLL(db1, tx);
	APPLY(db1, tx);

	OPEN("2", db2);
	APPLY(db2, tx);

	DONE(tx);

	EXEC(db1, "INSERT INTO test(n) VALUES(123)");

	POLL(db1, tx);
	APPLY(db1, tx);
	APPLY(db2, tx);
	DONE(tx);

	CLOSE(db2);
	CLOSE(db1);

	return MUNIT_OK;
}

/* Use VfsApply() to replicate a second write transaction on a different
 * VFS than the one that initially generated it and that has an open connection
 * which has built the WAL index header by preparing a statement. */
TEST(vfs_extra, applyOnDifferentVfsWithOpenConnection, setUp, tearDown, 0, NULL)
{
	sqlite3 *db1;
	sqlite3 *db2;
	sqlite3_stmt *stmt;
	struct vfsTransaction tx;

	OPEN("1", db1);

	PREPARE(db1, stmt, "CREATE TABLE test(n INT)");
	STEP(stmt, SQLITE_DONE);
	FINALIZE(stmt);
	POLL(db1, tx);
	APPLY(db1, tx);

	OPEN("2", db2);
	APPLY(db2, tx);
	CLOSE(db2);
	DONE(tx);

	EXEC(db1, "INSERT INTO test(n) VALUES(123)");
	POLL(db1, tx);
	ABORT(db1);
	CLOSE(db1);

	OPEN("2", db2);
	PREPARE(db2, stmt, "PRAGMA cache_size=-5000");
	FINALIZE(stmt);

	APPLY(db2, tx);

	PREPARE(db2, stmt, "SELECT * FROM test");
	STEP(stmt, SQLITE_ROW);
	FINALIZE(stmt);

	DONE(tx);

	CLOSE(db2);

	return MUNIT_OK;
}

/* A write transaction that gets replicated to a different VFS is visible to a
 * new connection opened on that VFS. */
TEST(vfs_extra, transactionVisibleOnDifferentVfs, setUp, tearDown, 0, NULL)
{
	sqlite3 *db1;
	sqlite3 *db2;
	sqlite3_stmt *stmt;
	struct vfsTransaction tx;

	OPEN("1", db1);

	EXEC(db1, "CREATE TABLE test(n INT)");

	POLL(db1, tx);
	APPLY(db1, tx);
	OPEN("2", db2);
	APPLY(db2, tx);
	CLOSE(db2);
	DONE(tx);

	CLOSE(db1);

	OPEN("2", db1);
	PREPARE(db1, stmt, "SELECT * FROM test");
	STEP(stmt, SQLITE_DONE);
	FINALIZE(stmt);
	CLOSE(db1);

	return MUNIT_OK;
}

/* Calling VfsAbort() to cancel a transaction releases the write
 * lock on the WAL. */
TEST(vfs_extra, abort, setUp, tearDown, 0, NULL)
{
	sqlite3 *db1;
	sqlite3 *db2;
	sqlite3_stmt *stmt1;
	sqlite3_stmt *stmt2;
	struct vfsTransaction tx;

	OPEN("1", db1);
	OPEN("1", db2);

	PREPARE(db1, stmt1, "CREATE TABLE test(n INT)");
	PREPARE(db2, stmt2, "CREATE TABLE test2(n INT)");

	STEP(stmt1, SQLITE_DONE);
	POLL(db1, tx);
	ABORT(db1);
	DONE(tx);

	STEP(stmt2, SQLITE_DONE);
	POLL(db2, tx);
	ABORT(db2);
	DONE(tx);

	FINALIZE(stmt1);
	FINALIZE(stmt2);

	CLOSE(db1);
	CLOSE(db2);


	return MUNIT_OK;
}

/* Perform a checkpoint after a write transaction has completed, then perform
 * another write transaction and check that changes both before and after the
 * checkpoint are visible. */
TEST(vfs_extra, checkpoint, setUp, tearDown, 0, NULL)
{
	sqlite3 *db1;
	sqlite3 *db2;
	sqlite3_stmt *stmt;
	struct vfsTransaction tx;

	OPEN("1", db1);

	EXEC(db1, "CREATE TABLE test(n INT)");
	POLL(db1, tx);
	APPLY(db1, tx);
	DONE(tx);
	EXEC(db1, "INSERT INTO test(n) VALUES(123)");
	POLL(db1, tx);
	APPLY(db1, tx);
	DONE(tx);

	OPEN("1", db2);
	CHECKPOINT(db2);
	CLOSE(db2);

	EXEC(db1, "INSERT INTO test(n) VALUES(456)");
	POLL(db1, tx);
	APPLY(db1, tx);
	DONE(tx);

	PREPARE(db1, stmt, "SELECT * FROM test");
	STEP(stmt, SQLITE_ROW);
	munit_assert_int(sqlite3_column_int(stmt, 0), ==, 123);
	STEP(stmt, SQLITE_ROW);
	munit_assert_int(sqlite3_column_int(stmt, 0), ==, 456);
	STEP(stmt, SQLITE_DONE);
	FINALIZE(stmt);

	CLOSE(db1);

	return MUNIT_OK;
}

TEST(vfs_extra, checkpointReclaimsSpace, setUp, tearDown, 0, NULL)
{
	sqlite3 *conn;
	struct vfsTransaction tx;
	struct sqlite3_file *main_f;
	sqlite3_int64 pre_vacuum_size, post_vacuum_size;
	int rv;


	OPEN("1", conn);
	rv = sqlite3_file_control(conn, NULL, SQLITE_FCNTL_FILE_POINTER,
				  &main_f);
	assert(rv == SQLITE_OK);

	EXEC(conn, "CREATE TABLE test(n INT)");
	POLL(conn, tx);
	APPLY(conn, tx);
	DONE(tx);

	EXEC(conn, "DROP TABLE test");
	POLL(conn, tx);
	APPLY(conn, tx);
	DONE(tx);
	CHECKPOINT(conn);

	rv = main_f->pMethods->xFileSize(main_f, &pre_vacuum_size);
	assert(rv == SQLITE_OK);

	EXEC(conn, "VACUUM");
	POLL(conn, tx);
	APPLY(conn, tx);
	DONE(tx);

	CHECKPOINT(conn);
	
	rv = main_f->pMethods->xFileSize(main_f, &post_vacuum_size);
	assert(rv == SQLITE_OK);
	CLOSE(conn);

	munit_assert_int(post_vacuum_size, <, pre_vacuum_size);
	munit_assert_int(post_vacuum_size, ==, 512);
	return MUNIT_OK;
}

TEST(vfs_extra, applyOnDifferentVfsCheckpointReclaimsSpace, setUp, tearDown, 0, NULL)
{
	sqlite3 *db1;
	sqlite3 *db2;
	struct vfsTransaction tx;
	struct sqlite3_file *main_f;
	sqlite3_int64 pre_vacuum_size, post_vacuum_size;
	int rv;


	OPEN("1", db1);
	OPEN("2", db2);

	EXEC(db1, "CREATE TABLE test(n INT)");
	POLL(db1, tx);
	APPLY(db1, tx);
	APPLY(db2, tx);
	DONE(tx);

	EXEC(db1, "DROP TABLE test");
	POLL(db1, tx);
	APPLY(db1, tx);
	APPLY(db2, tx);
	DONE(tx);
	CLOSE(db2);

	OPEN("2", db2);
	rv =
	    sqlite3_file_control(db2, NULL, SQLITE_FCNTL_FILE_POINTER, &main_f);
	assert(rv == SQLITE_OK);
	CHECKPOINT(db2);

	rv = main_f->pMethods->xFileSize(main_f, &pre_vacuum_size);
	assert(rv == SQLITE_OK);

	EXEC(db1, "VACUUM");
	POLL(db1, tx);
	APPLY(db1, tx);
	APPLY(db2, tx);
	DONE(tx);

	CHECKPOINT(db2);
	
	rv = main_f->pMethods->xFileSize(main_f, &post_vacuum_size);
	assert(rv == SQLITE_OK);
	CLOSE(db1);
	CLOSE(db2);

	munit_assert_int(post_vacuum_size, <, pre_vacuum_size);
	munit_assert_int(post_vacuum_size, ==, 512);
	return MUNIT_OK;
}

/* Replicate a write transaction that happens after a checkpoint. */
TEST(vfs_extra, applyOnDifferentVfsAfterCheckpoint, setUp, tearDown, 0, NULL)
{
	sqlite3 *db;
	sqlite3_stmt *stmt;
	struct vfsTransaction tx1;
	struct vfsTransaction tx2;
	struct vfsTransaction tx3;

	OPEN("1", db);

	EXEC(db, "CREATE TABLE test(n INT)");
	POLL(db, tx1);
	APPLY(db, tx1);
	EXEC(db, "INSERT INTO test(n) VALUES(123)");
	POLL(db, tx2);
	APPLY(db, tx2);

	CHECKPOINT(db);

	EXEC(db, "INSERT INTO test(n) VALUES(456)");
	POLL(db, tx3);
	APPLY(db, tx3);

	CLOSE(db);

	OPEN("2", db);
	APPLY(db, tx1);
	APPLY(db, tx2);
	CLOSE(db);

	OPEN("2", db);
	CHECKPOINT(db);
	APPLY(db, tx3);
	CLOSE(db);

	OPEN("2", db);
	PREPARE(db, stmt, "SELECT * FROM test ORDER BY n");
	STEP(stmt, SQLITE_ROW);
	munit_assert_int(sqlite3_column_int(stmt, 0), ==, 123);
	STEP(stmt, SQLITE_ROW);
	munit_assert_int(sqlite3_column_int(stmt, 0), ==, 456);
	STEP(stmt, SQLITE_DONE);
	FINALIZE(stmt);
	CLOSE(db);

	DONE(tx1);
	DONE(tx2);
	DONE(tx3);

	return MUNIT_OK;
}

/* Replicate a write transaction that happens after a checkpoint, without
 * performing the checkpoint on the replicated DB. */
TEST(vfs_extra,
     applyOnDifferentVfsAfterCheckpointOtherVfsNoCheckpoint,
     setUp,
     tearDown,
     0,
     NULL)
{
	sqlite3 *db;
	sqlite3_stmt *stmt;
	struct vfsTransaction tx1;
	struct vfsTransaction tx2;
	struct vfsTransaction tx3;
	struct vfsTransaction tx4;

	/* Create transactions and checkpoint the DB after every transaction */
	OPEN("1", db);

	EXEC(db, "CREATE TABLE test(n INT)");
	POLL(db, tx1);
	APPLY(db, tx1);
	CHECKPOINT_FRESH("1");

	EXEC(db, "CREATE TABLE test2(n INT)");
	POLL(db, tx2);
	APPLY(db, tx2);
	CHECKPOINT_FRESH("1");

	EXEC(db, "INSERT INTO test(n) VALUES(123)");
	POLL(db, tx3);
	APPLY(db, tx3);
	CHECKPOINT_FRESH("1");

	EXEC(db, "INSERT INTO test2(n) VALUES(456)");
	POLL(db, tx4);
	APPLY(db, tx4);
	CHECKPOINT_FRESH("1");

	CLOSE(db);

	/* Create a second VFS and Apply the transactions without checkpointing
	 * the DB in between. */
	OPEN("2", db);

	APPLY(db, tx1);
	APPLY(db, tx2);
	APPLY(db, tx3);
	APPLY(db, tx4);

	/* Ensure data is there. */
	PREPARE(db, stmt, "SELECT * FROM test ORDER BY n");
	STEP(stmt, SQLITE_ROW);
	munit_assert_int(sqlite3_column_int(stmt, 0), ==, 123);
	STEP(stmt, SQLITE_DONE);
	FINALIZE(stmt);

	PREPARE(db, stmt, "SELECT * FROM test2 ORDER BY n");
	STEP(stmt, SQLITE_ROW);
	munit_assert_int(sqlite3_column_int(stmt, 0), ==, 456);
	STEP(stmt, SQLITE_DONE);
	FINALIZE(stmt);

	/* Make sure checkpoint succeeds */
	CHECKPOINT_FRESH("2");
	CLOSE(db);

	DONE(tx1);
	DONE(tx2);
	DONE(tx3);
	DONE(tx4);

	return MUNIT_OK;
}

/* Replicate a write transaction that happens before a checkpoint, and is
 * replicated on a DB that has been checkpointed. */
TEST(vfs_extra,
     applyOnDifferentVfsExtraCheckpointsOnOtherVfs,
     setUp,
     tearDown,
     0,
     NULL)
{
	sqlite3 *db;
	sqlite3_stmt *stmt;
	struct vfsTransaction tx1;
	struct vfsTransaction tx2;
	struct vfsTransaction tx3;
	struct vfsTransaction tx4;

	/* Create transactions */
	OPEN("1", db);

	EXEC(db, "CREATE TABLE test(n INT)");
	POLL(db, tx1);
	APPLY(db, tx1);

	EXEC(db, "CREATE TABLE test2(n INT)");
	POLL(db, tx2);
	APPLY(db, tx2);

	EXEC(db, "INSERT INTO test(n) VALUES(123)");
	POLL(db, tx3);
	APPLY(db, tx3);

	EXEC(db, "INSERT INTO test2(n) VALUES(456)");
	POLL(db, tx4);
	APPLY(db, tx4);

	CLOSE(db);

	/* Create a second VFS and Apply the transactions while checkpointing
	 * after every transaction. */
	OPEN("2", db);
	APPLY(db, tx1);
	CHECKPOINT_FRESH("2");
	APPLY(db, tx2);
	CHECKPOINT_FRESH("2");
	APPLY(db, tx3);
	CHECKPOINT_FRESH("2");
	APPLY(db, tx4);
	CHECKPOINT_FRESH("2");
	CLOSE(db);

	/* Ensure all the data is there. */
	OPEN("2", db);

	PREPARE(db, stmt, "SELECT * FROM test ORDER BY n");
	STEP(stmt, SQLITE_ROW);
	munit_assert_int(sqlite3_column_int(stmt, 0), ==, 123);
	STEP(stmt, SQLITE_DONE);
	FINALIZE(stmt);

	PREPARE(db, stmt, "SELECT * FROM test2 ORDER BY n");
	STEP(stmt, SQLITE_ROW);
	munit_assert_int(sqlite3_column_int(stmt, 0), ==, 456);
	STEP(stmt, SQLITE_DONE);
	FINALIZE(stmt);

	CLOSE(db);

	DONE(tx1);
	DONE(tx2);
	DONE(tx3);
	DONE(tx4);

	return MUNIT_OK;
}

/* Replicate to another VFS a series of changes including a checkpoint, then
 * perform a new write transaction on that other VFS. */
TEST(vfs_extra, checkpointThenPerformTransaction, setUp, tearDown, 0, NULL)
{
	sqlite3 *db;
	struct vfsTransaction tx1;
	struct vfsTransaction tx2;
	struct vfsTransaction tx3;

	OPEN("1", db);

	EXEC(db, "CREATE TABLE test(n INT)");
	POLL(db, tx1);
	APPLY(db, tx1);
	EXEC(db, "INSERT INTO test(n) VALUES(123)");
	POLL(db, tx2);
	APPLY(db, tx2);

	CHECKPOINT(db);

	EXEC(db, "INSERT INTO test(n) VALUES(456)");
	POLL(db, tx3);
	APPLY(db, tx3);

	CLOSE(db);

	OPEN("2", db);

	APPLY(db, tx1);
	APPLY(db, tx2);

	CHECKPOINT_FRESH("2");

	APPLY(db, tx3);

	DONE(tx1);
	DONE(tx2);
	DONE(tx3);

	EXEC(db, "INSERT INTO test(n) VALUES(789)");
	POLL(db, tx1);
	APPLY(db, tx1);
	DONE(tx1);

	CLOSE(db);

	return MUNIT_OK;
}

/* Rollback a transaction that didn't hit the page cache limit and hence didn't
 * perform any pre-commit WAL writes. */
TEST(vfs_extra, rollbackTransactionWithoutPageStress, setUp, tearDown, 0, NULL)
{
	sqlite3 *db;
	struct vfsTransaction tx;
	sqlite3_stmt *stmt;

	OPEN("1", db);
	EXEC(db, "CREATE TABLE test(n INT)");

	POLL(db, tx);
	APPLY(db, tx);
	DONE(tx);

	EXEC(db, "BEGIN");
	EXEC(db, "INSERT INTO test(n) VALUES(1)");
	EXEC(db, "ROLLBACK");

	POLL(db, tx);
	munit_assert_int(tx.n_pages, ==, 0);

	PREPARE(db, stmt, "SELECT * FROM test");
	STEP(stmt, SQLITE_DONE);
	RESET(stmt, SQLITE_OK);

	EXEC(db, "INSERT INTO test(n) VALUES(1)");
	POLL(db, tx);
	APPLY(db, tx);
	DONE(tx);

	STEP(stmt, SQLITE_ROW);

	FINALIZE(stmt);

	CLOSE(db);

	return MUNIT_OK;
}

/* Rollback a transaction that hit the page cache limit and hence performed some
 * pre-commit WAL writes. */
TEST(vfs_extra, rollbackTransactionWithPageStress, setUp, tearDown, 0, NULL)
{
	sqlite3 *db;
	sqlite3_stmt *stmt;
	struct vfsTransaction tx;
	unsigned i;

	OPEN("1", db);

	EXEC(db, "CREATE TABLE test(n INT)");

	POLL(db, tx);
	APPLY(db, tx);
	DONE(tx);

	EXEC(db, "BEGIN");
	for (i = 0; i < 163; i++) {
		char sql[64];
		sprintf(sql, "INSERT INTO test(n) VALUES(%d)", i + 1);
		EXEC(db, sql);
		POLL(db, tx);
		munit_assert_int(tx.n_pages, ==, 0);
	}
	EXEC(db, "ROLLBACK");

	POLL(db, tx);
	munit_assert_int(tx.n_pages, ==, 0);
	PREPARE(db, stmt, "SELECT * FROM test");
	STEP(stmt, SQLITE_DONE);
	RESET(stmt, SQLITE_OK);

	EXEC(db, "INSERT INTO test(n) VALUES(1)");
	POLL(db, tx);
	APPLY(db, tx);
	DONE(tx);

	STEP(stmt, SQLITE_ROW);

	FINALIZE(stmt);

	CLOSE(db);

	return MUNIT_OK;
}

/* Try and fail to checkpoint a WAL that performed some pre-commit WAL writes.
 */
TEST(vfs_extra, checkpointTransactionWithPageStress, setUp, tearDown, 0, NULL)
{
	sqlite3 *db;
	struct vfsTransaction tx;
	unsigned i;

	OPEN("1", db);

	EXEC(db, "CREATE TABLE test(n INT)");

	POLL(db, tx);
	APPLY(db, tx);
	DONE(tx);

	EXEC(db, "BEGIN");
	for (i = 0; i < 163; i++) {
		char sql[64];
		sprintf(sql, "INSERT INTO test(n) VALUES(%d)", i + 1);
		EXEC(db, sql);
		POLL(db, tx);
		munit_assert_int(tx.n_pages, ==, 0);
	}

	CHECKPOINT_FAIL(db, SQLITE_LOCKED);

	CLOSE(db);

	return MUNIT_OK;
}

/* A snapshot of a brand new database that has been just initialized contains
 * just the first page of the main database file. */
TEST(vfs_extra, snapshotInitialDatabase, setUp, tearDown, 0, NULL)
{
	sqlite3 *db;
	struct vfsSnapshot snapshot;
	uint8_t *page;
	uint8_t page_size[2] = {2, 0};           /* Big-endian page size */
	uint8_t database_size[4] = {0, 0, 0, 1}; /* Big-endian database size */

	OPEN("1", db);

	int rv = VfsAcquireSnapshot(db, &snapshot);
	munit_assert_int(rv, ==, SQLITE_OK);
	munit_assert_int(snapshot.main.page_count, ==, 1);
	munit_assert_int(snapshot.main.page_size, ==, DB_PAGE_SIZE);
	munit_assert_int(snapshot.wal.page_count, ==, 0);
	munit_assert_int(snapshot.wal.page_size, ==, DB_PAGE_SIZE);

	page = snapshot.main.pages[0];

	munit_assert_int(memcmp(&page[16], page_size, 2), ==, 0);
	munit_assert_int(memcmp(&page[28], database_size, 4), ==, 0);

	VfsReleaseSnapshot(db, &snapshot);
	CLOSE(db);

	return MUNIT_OK;
}

/* Take a snapshot of a database after the first write transaction has been
 * applied. */
TEST(vfs_extra, snapshotAfterFirstTransaction, setUp, tearDown, 0, NULL)
{
	sqlite3 *db;
	struct vfsSnapshot snapshot;
	struct vfsTransaction tx;

	OPEN("1", db);
	EXEC(db, "CREATE TABLE test(n INT)");

	POLL(db, tx);
	APPLY(db, tx);
	DONE(tx);

	int rv = VfsAcquireSnapshot(db, &snapshot);
	munit_assert_int(rv, ==, SQLITE_OK);
	/*
	 * Page number 1 contains the header and the schema root.
	 * Page number 2 contains the (empty) root for test table.
	 */
	const uint32_t pages = 2;
	munit_assert_int(snapshot.main.page_count, ==, pages);
	munit_assert_int(snapshot.main.page_size, ==, DB_PAGE_SIZE);
	munit_assert_int(snapshot.wal.page_count, ==, 0);
	munit_assert_int(snapshot.wal.page_size, ==, DB_PAGE_SIZE);

	uint8_t *page = snapshot.main.pages[0];
	munit_assert_int(ByteGetBe16(&page[16]), ==, DB_PAGE_SIZE);
	munit_assert_int(ByteGetBe32(&page[28]), ==, pages);

	VfsReleaseSnapshot(db, &snapshot);
	CLOSE(db);

	return MUNIT_OK;
}

/* A snapshot of a database after a checkpoint contains all checkpointed pages
 * and no WAL frames. */
TEST(vfs_extra, snapshotAfterCheckpoint, setUp, tearDown, 0, NULL)
{
	sqlite3 *db;
	struct vfsSnapshot snapshot;
	struct vfsTransaction tx;

	OPEN("1", db);
	EXEC(db, "CREATE TABLE test(n INT)");

	POLL(db, tx);
	APPLY(db, tx);
	DONE(tx);

	CHECKPOINT(db);

	int rv = VfsAcquireSnapshot(db, &snapshot);
	munit_assert_int(rv, ==, SQLITE_OK);

	const uint32_t pages = 2;
	munit_assert_int(snapshot.main.page_count, ==, pages);
	munit_assert_int(snapshot.main.page_size, ==, DB_PAGE_SIZE);
	munit_assert_int(snapshot.wal.page_count, ==, 0);
	munit_assert_int(snapshot.wal.page_size, ==, DB_PAGE_SIZE);

	uint8_t *page = snapshot.main.pages[0];
	munit_assert_int(ByteGetBe16(&page[16]), ==, DB_PAGE_SIZE);
	munit_assert_int(ByteGetBe32(&page[28]), ==, pages);

	CLOSE(db);

	return MUNIT_OK;
}

/* Restore a snapshot taken after a brand new database has been just
 * initialized. */
TEST(vfs_extra, restoreInitialDatabase, setUp, tearDown, 0, NULL)
{
	sqlite3 *db1, *db2;
	struct vfsSnapshot snapshot;

	OPEN("1", db1);

	// TODO
	int rv = VfsAcquireSnapshot(db1, &snapshot);
	munit_assert_int(rv, ==, SQLITE_OK);

	OPEN("2", db2);
	rv = VfsRestore(db2, &snapshot);
	munit_assert_int(rv, ==, SQLITE_OK);
	CLOSE(db2);

	VfsReleaseSnapshot(db1, &snapshot);
	CLOSE(db1);

	return MUNIT_OK;
}

/* Restore a snapshot of a database taken after the first write transaction gets
 * applied. */
TEST(vfs_extra, restoreAfterFirstTransaction, setUp, tearDown, 0, NULL)
{
	sqlite3 *db1, *db2;
	sqlite3_stmt *stmt;
	struct vfsSnapshot snapshot;
	struct vfsTransaction tx;

	OPEN("1", db1);
	EXEC(db1, "CREATE TABLE test(n INT)");

	POLL(db1, tx);
	APPLY(db1, tx);
	DONE(tx);

	int rv = VfsAcquireSnapshot(db1, &snapshot);
	munit_assert_int(rv, ==, SQLITE_OK);

	OPEN("2", db2);

	rv = VfsRestore(db2, &snapshot);
	munit_assert_int(rv, ==, SQLITE_OK);

	VfsReleaseSnapshot(db1, &snapshot);

	CLOSE(db2);
	CLOSE(db1);


	OPEN("2", db2);

	PREPARE(db2, stmt, "SELECT * FROM test");
	STEP(stmt, SQLITE_DONE);
	FINALIZE(stmt);
	CLOSE(db2);

	return MUNIT_OK;
}

/* Restore a snapshot of a database while a connection is open. */
TEST(vfs_extra, restoreWithOpenConnection, setUp, tearDown, 0, NULL)
{
	sqlite3 *db1, *db2;
	sqlite3_stmt *stmt;
	struct vfsSnapshot snapshot;
	struct vfsTransaction tx;

	OPEN("1", db1);
	EXEC(db1, "CREATE TABLE test(n INT)");
	POLL(db1, tx);
	APPLY(db1, tx);
	DONE(tx);


	int rv = VfsAcquireSnapshot(db1, &snapshot);
	munit_assert_int(rv, ==, SQLITE_OK);

	OPEN("2", db2);

	rv = VfsRestore(db2, &snapshot);
	munit_assert_int(rv, ==, SQLITE_OK);

	VfsReleaseSnapshot(db1, &snapshot);
	CLOSE(db1);

	PREPARE(db2, stmt, "SELECT * FROM test");
	STEP(stmt, SQLITE_DONE);
	FINALIZE(stmt);

	CLOSE(db2);

	return MUNIT_OK;
}

/* Changing page_size to non-default value fails. */
TEST(vfs_extra, changePageSize, setUp, tearDown, 0, NULL)
{
	sqlite3 *db;
	int rv;

	OPEN("1", db);

	rv = sqlite3_exec(db, "PRAGMA page_size=1024", NULL, NULL, NULL);
	munit_assert_int(rv, !=, 0);

	CLOSE(db);

	return MUNIT_OK;
}

/* Changing page_size to current value succeeds. */
TEST(vfs_extra, changePageSizeSameValue, setUp, tearDown, 0, NULL)
{
	sqlite3 *db;
	int rv;

	OPEN("1", db);

	rv = sqlite3_exec(db, "PRAGMA page_size=512", NULL, NULL, NULL);
	munit_assert_int(rv, ==, 0);

	CLOSE(db);

	return MUNIT_OK;
}

static void deleteHook(void *data, const char *name)
{
	bool *deleted = data;
	munit_assert_string_equal(name, VFS_PATH);
	*deleted = true;
}

/* Changing page_size to current value succeeds. */
TEST(vfs_extra, delete, setUp, tearDown, 0, NULL)
{
	sqlite3 *db;
	struct vfsTransaction tx;

	bool deleted = false;
	VfsDeleteHook(sqlite3_vfs_find("1"),deleteHook, &deleted);

	OPEN("1", db);
	int rv = sqlite3_exec(db, "BEGIN IMMEDIATE; PRAGMA delete_database; COMMIT;", NULL, NULL, NULL);
	munit_assert_int(rv, ==, SQLITE_OK);
	POLL(db, tx);
	APPLY(db, tx);
	DONE(tx);
	CLOSE(db);

	/* Make sure the database is not there anymore */
	munit_assert_true(deleted);
	rv = sqlite3_open_v2(VFS_PATH, &db, SQLITE_OPEN_READONLY, "1");
	munit_assert_int(rv, ==, SQLITE_CANTOPEN);
	munit_assert_int(sqlite3_system_errno(db), ==, ENOENT);
	sqlite3_close(db);

	return MUNIT_OK;
}

/* Changing page_size to current value succeeds. */
TEST(vfs_extra, delete_multiple, setUp, tearDown, 0, NULL)
{
	sqlite3 *db1;
	sqlite3 *db2;
	struct vfsTransaction tx;

	bool deleted = false;
	VfsDeleteHook(sqlite3_vfs_find("1"),deleteHook, &deleted);

	OPEN("1", db1);
	OPEN("1", db2);

	int rv = sqlite3_exec(db1, "BEGIN IMMEDIATE; PRAGMA delete_database; COMMIT;", NULL, NULL, NULL);
	munit_assert_int(rv, ==, SQLITE_OK);
	POLL(db1, tx);
	APPLY(db1, tx);
	DONE(tx);
	CLOSE(db1);

	/* Make sure the database is deleted only when the last file is closed. */
	munit_assert_false(deleted);
	CLOSE(db2);
	munit_assert_true(deleted);
	rv = sqlite3_open_v2(VFS_PATH, &db1, SQLITE_OPEN_READONLY, "1");
	munit_assert_int(rv, ==, SQLITE_CANTOPEN);
	munit_assert_int(sqlite3_system_errno(db1), ==, ENOENT);
	sqlite3_close(db1);

	return MUNIT_OK;
}

