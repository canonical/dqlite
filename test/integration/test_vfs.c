#include "../lib/heap.h"
#include "../lib/runner.h"
#include "../lib/sqlite.h"

#include "../../include/dqlite.h"

SUITE(vfs);

struct fixture
{
	struct sqlite3_vfs vfs;
};

static void *setUp(const MunitParameter params[], void *user_data)
{
	struct fixture *f = munit_malloc(sizeof *f);
	int rv;

	SETUP_HEAP;
	SETUP_SQLITE;

	rv = dqlite_vfs_init(&f->vfs, "dqlite");
	munit_assert_int(rv, ==, 0);

	sqlite3_vfs_register(&f->vfs, 0);

	return f;
}

static void tearDown(void *data)
{
	struct fixture *f = data;

	sqlite3_vfs_unregister(&f->vfs);

	dqlite_vfs_close(&f->vfs);

	TEAR_DOWN_SQLITE;
	TEAR_DOWN_HEAP;

	free(f);
}

/* Helper to execute a SQL statement on the given DB. */
#define EXEC(DB, SQL)                                  \
	_rv = sqlite3_exec(DB, SQL, NULL, NULL, NULL); \
	munit_assert_int(_rv, ==, SQLITE_OK);

/* Open a new database connection. */
#define OPEN(DB)                                                         \
	do {                                                             \
		int _flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE; \
		int _rv;                                                 \
		_rv = sqlite3_open_v2("test.db", &DB, _flags, "dqlite"); \
		munit_assert_int(_rv, ==, SQLITE_OK);                    \
		_rv = sqlite3_extended_result_codes(DB, 1);              \
		munit_assert_int(_rv, ==, SQLITE_OK);                    \
		EXEC(DB, "PRAGMA page_size=512");                        \
		EXEC(DB, "PRAGMA synchronous=OFF");                      \
		EXEC(DB, "PRAGMA journal_mode=WAL");                     \
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

/* Open and close a new connection using the dqlite VFS. */
TEST(vfs, open, setUp, tearDown, 0, NULL)
{
	sqlite3 *db;
	OPEN(db);
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
	OPEN(db1);
	PREPARE(db1, stmt1, "CREATE TABLE test(n INT)");
	STEP(stmt1, SQLITE_DONE);
	FINALIZE(stmt1);

	OPEN(db2);
	rv = sqlite3_prepare_v2(db2, "SELECT * FROM test", -1, &stmt2, NULL);
	munit_assert_int(rv, ==, SQLITE_ERROR);

	CLOSE(db1);
	CLOSE(db2);

	return MUNIT_OK;
}
