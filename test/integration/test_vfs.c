#include <stdio.h>

#include "../lib/heap.h"
#include "../lib/runner.h"
#include "../lib/sqlite.h"

#include "../../include/dqlite.h"

SUITE(vfs);

#define N_VFS 2

#define PAGE_SIZE 512

#define PRAGMA(DB, COMMAND)                                          \
	_rv = sqlite3_exec(DB, "PRAGMA " COMMAND, NULL, NULL, NULL); \
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
		PRAGMA(DB, "page_size=512");                                  \
		PRAGMA(DB, "synchronous=OFF");                                \
		PRAGMA(DB, "journal_mode=WAL");                               \
		PRAGMA(DB, "cache_size=1");                                   \
		_rv = sqlite3_db_config(DB, SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, \
					1, NULL);                             \
		munit_assert_int(_rv, ==, SQLITE_OK);                         \
	} while (0)

/* Return the database file associated with the given connection. */
#define FILE(DB, FILE)                                                        \
	do {                                                                  \
		int _rv;                                                      \
		_rv = sqlite3_file_control(DB, "main",                        \
					   SQLITE_FCNTL_FILE_POINTER, &FILE); \
		munit_assert_int(_rv, ==, SQLITE_OK);                         \
	} while (0)

/* Acquire or release a SHM lock. */
#define LOCK(FILE, I, FLAGS)                                       \
	do {                                                       \
		int _rv;                                           \
		_rv = FILE->pMethods->xShmLock(FILE, I, 1, FLAGS); \
		munit_assert_int(_rv, ==, SQLITE_OK);              \
	} while (0)

/* Close a database connection. */
#define CLOSE(DB)                                     \
	do {                                          \
		int _rv;                              \
		_rv = sqlite3_close(DB);              \
		munit_assert_int(_rv, ==, SQLITE_OK); \
	} while (0)

/* Hold WAL replication information about a single transaction. */
struct tx
{
	unsigned n;
	unsigned long *page_numbers;
	void *frames;
};

struct fixture
{
	struct sqlite3_vfs vfs[N_VFS]; /* A "cluster" of VFS objects. */
	char names[8][N_VFS];          /* Registration names */
	sqlite3 *dbs[N_VFS];           /* For replication */
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
		rv = dqlite_vfs_init(&f->vfs[i], f->names[i]);
		munit_assert_int(rv, ==, 0);
		rv = sqlite3_vfs_register(&f->vfs[i], 0);
		munit_assert_int(rv, ==, 0);
		OPEN(f->names[i], f->dbs[i]);
		CLOSE(f->dbs[i]);
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
		dqlite_vfs_close(&f->vfs[i]);
	}

	TEAR_DOWN_SQLITE;
	TEAR_DOWN_HEAP;

	free(f);
}

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
#define POLL(VFS, TX)                                                      \
	do {                                                               \
		sqlite3_vfs *vfs = sqlite3_vfs_find(VFS);                  \
		dqlite_vfs_frame *_frames;                                 \
		unsigned _i;                                               \
		int _rv;                                                   \
		memset(&TX, 0, sizeof TX);                                 \
		_rv = dqlite_vfs_poll(vfs, "test.db", &_frames, &TX.n);    \
		munit_assert_int(_rv, ==, 0);                              \
		if (_frames != NULL) {                                     \
			TX.page_numbers =                                  \
			    munit_malloc(sizeof *TX.page_numbers * TX.n);  \
			TX.frames = munit_malloc(PAGE_SIZE * TX.n);        \
			for (_i = 0; _i < TX.n; _i++) {                    \
				dqlite_vfs_frame *_frame = &_frames[_i];   \
				TX.page_numbers[_i] = _frame->page_number; \
				memcpy(TX.frames + _i * PAGE_SIZE,         \
				       _frame->data, PAGE_SIZE);           \
				sqlite3_free(_frame->data);                \
			}                                                  \
			sqlite3_free(_frames);                             \
		}                                                          \
	} while (0)

/* Commit WAL frames to the given VFS. */
#define COMMIT(VFS, TX)                                                        \
	do {                                                                   \
		sqlite3_vfs *vfs = sqlite3_vfs_find(VFS);                      \
		int _rv;                                                       \
		_rv = dqlite_vfs_commit(vfs, "test.db", TX.n, TX.page_numbers, \
					TX.frames);                            \
		munit_assert_int(_rv, ==, 0);                                  \
	} while (0)

/* Abort a transaction on the given VFS. */
#define ABORT(VFS)                                        \
	do {                                              \
		sqlite3_vfs *vfs = sqlite3_vfs_find(VFS); \
		int _rv;                                  \
		_rv = dqlite_vfs_abort(vfs, "test.db");   \
		munit_assert_int(_rv, ==, 0);             \
	} while (0)

/* Release all memory used by a struct tx object. */
#define DONE(TX)                       \
	do {                           \
		free(TX.frames);       \
		free(TX.page_numbers); \
	} while (0)

/* Peform a full checkpoint on the given database. */
#define CHECKPOINT(DB)                                                       \
	do {                                                                 \
		int _size;                                                   \
		int _ckpt;                                                   \
		int _rv;                                                     \
		_rv = sqlite3_wal_checkpoint_v2(                             \
		    DB, "main", SQLITE_CHECKPOINT_TRUNCATE, &_size, &_ckpt); \
		if (_rv != SQLITE_OK) {                                      \
			munit_errorf("checkpoint: %s (%d)",                  \
				     sqlite3_errmsg(DB), _rv);               \
		}                                                            \
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
	sqlite3_stmt *stmt;
	int rv;

	OPEN("1", db1);
	EXEC(db1, "CREATE TABLE test(n INT)");

	OPEN("1", db2);
	rv = sqlite3_prepare_v2(db2, "SELECT * FROM test", -1, &stmt, NULL);
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
	struct tx tx;
	unsigned i;

	OPEN("1", db);

	PREPARE(db, stmt, "CREATE TABLE test(n INT)");
	STEP(stmt, SQLITE_DONE);

	POLL("1", tx);

	munit_assert_ptr_not_null(tx.frames);
	munit_assert_int(tx.n, ==, 2);
	for (i = 0; i < tx.n; i++) {
		munit_assert_int(tx.page_numbers[i], ==, i + 1);
	}

	DONE(tx);

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
	struct tx tx;

	OPEN("1", db1);
	OPEN("1", db2);

	PREPARE(db1, stmt1, "CREATE TABLE test(n INT)");
	PREPARE(db2, stmt2, "CREATE TABLE test2(n INT)");

	STEP(stmt1, SQLITE_DONE);
	POLL("1", tx);

	STEP(stmt2, SQLITE_BUSY);
	RESET(stmt2, SQLITE_BUSY);

	FINALIZE(stmt1);
	FINALIZE(stmt2);

	CLOSE(db1);
	CLOSE(db2);

	DONE(tx);

	return MUNIT_OK;
}

/* If a the page cache limit is exceeded during write transaction, some
 * non-commit WAL frames will be written before the final commit. */
TEST(vfs, pollAfterPageStress, setUp, tearDown, 0, NULL)
{
	sqlite3 *db;
	sqlite3_stmt *stmt;
	struct tx tx;
	unsigned i;

	OPEN("1", db);

	EXEC(db, "CREATE TABLE test(n INT)");

	POLL("1", tx);
	COMMIT("1", tx);
	DONE(tx);

	/* Start a transaction and accumulate enough dirty data to fill the page
	 * cache and trigger a WAL write of the uncommitted frames. */
	EXEC(db, "BEGIN");
	for (i = 0; i < 163; i++) {
		char sql[64];
		sprintf(sql, "INSERT INTO test(n) VALUES(%d)", i + 1);
		EXEC(db, sql);
		POLL("1", tx);
		munit_assert_int(tx.n, ==, 0);
	}
	for (i = 0; i < 163; i++) {
		char sql[64];
		sprintf(sql, "UPDATE test SET n=%d WHERE n=%d", i, i + 1);
		POLL("1", tx);
		munit_assert_int(tx.n, ==, 0);
		EXEC(db, sql);
	}
	EXEC(db, "COMMIT");

	POLL("1", tx);

	/* Five frames were replicated and the first frame actually contains a
	 * spill of the third page. */
	munit_assert_int(tx.n, ==, 6);
	munit_assert_int(tx.page_numbers[0], ==, 3);
	munit_assert_int(tx.page_numbers[1], ==, 4);
	munit_assert_int(tx.page_numbers[2], ==, 5);
	munit_assert_int(tx.page_numbers[3], ==, 1);
	munit_assert_int(tx.page_numbers[4], ==, 2);

	COMMIT("1", tx);
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

/* Use dqlite_vfs_commit() to actually modify the WAL after quorum is reached,
 * then perform a read transaction and check that it can see the committed
 * changes. */
TEST(vfs, commitThenRead, setUp, tearDown, 0, NULL)
{
	sqlite3 *db;
	sqlite3_stmt *stmt;
	struct tx tx;

	OPEN("1", db);

	EXEC(db, "CREATE TABLE test(n INT)");

	POLL("1", tx);
	COMMIT("1", tx);
	DONE(tx);

	PREPARE(db, stmt, "SELECT * FROM test");
	STEP(stmt, SQLITE_DONE);
	FINALIZE(stmt);

	CLOSE(db);

	return MUNIT_OK;
}

/* Execute an explicit transaction with BEGIN/COMMIT. */
TEST(vfs, explicitTransaction, setUp, tearDown, 0, NULL)
{
	sqlite3 *db;
	sqlite3_stmt *stmt;
	struct tx tx;

	OPEN("1", db);

	PREPARE(db, stmt, "BEGIN");
	STEP(stmt, SQLITE_DONE);
	POLL("1", tx);
	munit_assert_int(tx.n, ==, 0);
	FINALIZE(stmt);

	PREPARE(db, stmt, "CREATE TABLE test(n INT)");
	STEP(stmt, SQLITE_DONE);
	POLL("1", tx);
	munit_assert_int(tx.n, ==, 0);
	FINALIZE(stmt);

	PREPARE(db, stmt, "COMMIT");
	STEP(stmt, SQLITE_DONE);
	POLL("1", tx);
	munit_assert_int(tx.n, ==, 2);
	COMMIT("1", tx);
	DONE(tx);
	FINALIZE(stmt);

	return MUNIT_OK;
}

/* Use dqlite_vfs_commit() to actually modify the WAL after quorum is reached,
 * then perform another commit and finally run a read transaction and check that
 * it can see all committed changes. */
TEST(vfs, commitThenCommitAgainThenRead, setUp, tearDown, 0, NULL)
{
	sqlite3 *db;
	sqlite3_stmt *stmt;
	struct tx tx;

	OPEN("1", db);

	EXEC(db, "CREATE TABLE test(n INT)");

	POLL("1", tx);
	COMMIT("1", tx);
	DONE(tx);

	EXEC(db, "INSERT INTO test(n) VALUES(123)");

	POLL("1", tx);
	COMMIT("1", tx);
	DONE(tx);

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
	struct tx tx;

	OPEN("1", db1);
	OPEN("1", db2);

	EXEC(db1, "CREATE TABLE test(n INT)");

	POLL("1", tx);
	COMMIT("1", tx);
	DONE(tx);

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
	struct tx tx;

	OPEN("1", db);

	EXEC(db, "CREATE TABLE test(n INT)");

	POLL("1", tx);
	COMMIT("1", tx);
	DONE(tx);

	CLOSE(db);

	OPEN("1", db);
	PREPARE(db, stmt, "SELECT * FROM test");
	STEP(stmt, SQLITE_DONE);
	FINALIZE(stmt);
	CLOSE(db);

	return MUNIT_OK;
}

/* Use dqlite_vfs_commit() to replicate changes on a "follower" VFS. */
TEST(vfs, commitFollower, setUp, tearDown, 0, NULL)
{
	sqlite3 *db;
	sqlite3_stmt *stmt;
	struct tx tx;

	OPEN("1", db);

	PREPARE(db, stmt, "CREATE TABLE test(n INT)");
	STEP(stmt, SQLITE_DONE);

	POLL("1", tx);

	COMMIT("1", tx);
	COMMIT("2", tx);

	DONE(tx);

	FINALIZE(stmt);
	CLOSE(db);

	return MUNIT_OK;
}

/* Simulate a failover between a leader and a follower. */
TEST(vfs, commitFailover, setUp, tearDown, 0, NULL)
{
	sqlite3 *db;
	sqlite3_stmt *stmt;
	struct tx tx;

	/* Create a table on VFS 1 and replicate the transaction. */
	OPEN("1", db);

	PREPARE(db, stmt, "CREATE TABLE test(n INT)");
	STEP(stmt, SQLITE_DONE);

	POLL("1", tx);
	COMMIT("1", tx);
	COMMIT("2", tx);
	DONE(tx);

	FINALIZE(stmt);
	CLOSE(db);

	/* Connect to VFS 2 and check that the table is there. */
	OPEN("2", db);
	PREPARE(db, stmt, "SELECT * FROM test");
	STEP(stmt, SQLITE_DONE);
	FINALIZE(stmt);
	CLOSE(db);

	return MUNIT_OK;
}

/* Calling dqlite_vfs_abort() to cancel a transaction releases the write
 * lock on the WAL. */
TEST(vfs, abort, setUp, tearDown, 0, NULL)
{
	sqlite3 *db1;
	sqlite3 *db2;
	sqlite3_stmt *stmt1;
	sqlite3_stmt *stmt2;
	struct tx tx;

	/* Create a table on VFS 1 and replicate the transaction. */
	OPEN("1", db1);
	OPEN("1", db2);

	PREPARE(db1, stmt1, "CREATE TABLE test(n INT)");
	PREPARE(db2, stmt2, "CREATE TABLE test2(n INT)");

	STEP(stmt1, SQLITE_DONE);
	POLL("1", tx);
	ABORT("1");

	STEP(stmt2, SQLITE_DONE);

	FINALIZE(stmt1);
	FINALIZE(stmt2);

	CLOSE(db1);
	CLOSE(db2);

	DONE(tx);

	return MUNIT_OK;
}

/* Perform a checkpoint after a write transaction has completed. */
TEST(vfs, checkpoint, setUp, tearDown, 0, NULL)
{
	sqlite3 *db1;
	sqlite3 *db2;
	sqlite3_stmt *stmt;
	struct tx tx;

	OPEN("1", db1);

	EXEC(db1, "CREATE TABLE test(n INT)");
	POLL("1", tx);
	COMMIT("1", tx);
	DONE(tx);
	EXEC(db1, "INSERT INTO test(n) VALUES(123)");
	POLL("1", tx);
	COMMIT("1", tx);
	DONE(tx);

	OPEN("1", db2);
	CHECKPOINT(db2);
	CLOSE(db2);

	PREPARE(db1, stmt, "SELECT * FROM test");
	STEP(stmt, SQLITE_ROW);
	munit_assert_int(sqlite3_column_int(stmt, 0), ==, 123);
	STEP(stmt, SQLITE_DONE);
	FINALIZE(stmt);

	CLOSE(db1);

	return MUNIT_OK;
}

/* Acquiring all locks prevents both read and write transactions. */
TEST(vfs, lock, setUp, tearDown, 0, NULL)
{
	sqlite3 *db;
	sqlite3_file *file;
	sqlite3_stmt *stmt;
	struct tx tx;
	unsigned i;

	OPEN("1", db);

	EXEC(db, "CREATE TABLE test(n INT)");
	POLL("1", tx);
	COMMIT("1", tx);
	DONE(tx);

	FILE(db, file);

	for (i = 0; i < SQLITE_SHM_NLOCK; i++) {
		LOCK(file, i, SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE);
	}

	PREPARE(db, stmt, "SELECT * FROM test");
	STEP(stmt, SQLITE_PROTOCOL);
	RESET(stmt, SQLITE_PROTOCOL);
	FINALIZE(stmt);

	PREPARE(db, stmt, "INSERT INTO test(n) VALUES(123)");
	STEP(stmt, SQLITE_PROTOCOL);
	RESET(stmt, SQLITE_PROTOCOL);
	FINALIZE(stmt);

	CLOSE(db);

	return MUNIT_OK;
}
