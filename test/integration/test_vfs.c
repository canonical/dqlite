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

/* Open a new database connection. */
#define OPEN(DB)                                                         \
	do {                                                             \
		int _flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE; \
		int _rv;                                                 \
		_rv = sqlite3_open_v2("test.db", &DB, _flags, "dqlite"); \
		munit_assert_int(_rv, ==, SQLITE_OK);                    \
	} while (0)

/* Close a database connection. */
#define CLOSE(DB)                                     \
	do {                                          \
		int _rv;                              \
		_rv = sqlite3_close(DB);              \
		munit_assert_int(_rv, ==, SQLITE_OK); \
	} while (0)

/* Open and close a new connection using the dqlite VFS. */
TEST(vfs, open, setUp, tearDown, 0, NULL)
{
	sqlite3 *db;
	OPEN(db);
	CLOSE(db);
	return MUNIT_OK;
}
